"""Shared config, Kafka clients, and Avro serialization for headway-ingest."""
import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
TOPIC = "mta.trip_updates"
PARTITIONS = 6
ENV_KEYS = ["BOOTSTRAP", "SASL_USERNAME", "SASL_PASSWORD", "SR_URL", "SR_API_KEY", "SR_API_SECRET"]
PRODUCER_CONF = {  # message.timeout.ms rides out long outages; idempotence keeps retries safe
    "enable.idempotence": True, "acks": "all", "retries": 2147483647, "retry.backoff.ms": 250,
    "message.timeout.ms": 900000, "linger.ms": 50, "compression.type": "lz4"}


def jlog(**kw):
    print(json.dumps({"ts": round(time.time(), 3), **kw}, default=str), flush=True)


def load_config():
    load_dotenv(ROOT / ".env")
    cfg = {k: os.environ.get(k) for k in ENV_KEYS}
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        sys.exit(f"Missing in {ROOT / '.env'}: {', '.join(missing)} (see .env.example)")
    return cfg


def kafka_conf(cfg):
    return {"bootstrap.servers": cfg["BOOTSTRAP"], "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN", "sasl.username": cfg["SASL_USERNAME"],
            "sasl.password": cfg["SASL_PASSWORD"]}


def make_producer(cfg):
    return Producer({**kafka_conf(cfg), **PRODUCER_CONF})


def avro_value_serializer(cfg):
    sr = SchemaRegistryClient({"url": cfg["SR_URL"],
                               "basic.auth.user.info": f"{cfg['SR_API_KEY']}:{cfg['SR_API_SECRET']}"})
    return AvroSerializer(sr, (ROOT / "schemas" / "trip_update.avsc").read_text())


def enqueue(producer, kwargs, tries=3):
    """produce() with backpressure handling; False means the message was dropped."""
    for _ in range(tries):
        try:
            producer.produce(**kwargs)
            return True
        except BufferError:
            producer.poll(1.0)  # serve delivery reports to free local queue space
    return False


def produce_event(producer, serializer, ev, on_delivery=None):
    value = serializer(ev, SerializationContext(TOPIC, MessageField.VALUE))
    return enqueue(producer, {"topic": TOPIC, "key": ev["route_id"].encode(), "value": value,
                              "on_delivery": on_delivery})


def ensure_topic(cfg, name, partitions=PARTITIONS):
    admin = AdminClient(kafka_conf(cfg))
    if name in admin.list_topics(timeout=15).topics:
        return
    fut = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=3)])[name]
    try:
        fut.result(30)
        jlog(created_topic=name, partitions=partitions)
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
