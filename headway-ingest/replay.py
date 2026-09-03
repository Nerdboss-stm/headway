#!/usr/bin/env python3
"""Replay recorded topics from a start timestamp onto <topic>.replay at N x speed.
Messages are merged across partitions in timestamp order so relative timing holds
globally, not just per partition. Payloads and keys pass through byte-for-byte."""
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
import common
from common import jlog

DEFAULT_TOPICS = ["mta.trip_updates", "citibike.station_status"]
BUF_MAX = 2000  # per-partition merge buffer; pause fetching past this


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="ISO datetime (local), e.g. 2026-09-04T07:30")
    ap.add_argument("--end", help="optional ISO datetime to stop at")
    ap.add_argument("--speed", type=float, default=20.0)
    ap.add_argument("--topics", nargs="+", default=DEFAULT_TOPICS)
    args = ap.parse_args()
    start_ms = int(datetime.fromisoformat(args.start).timestamp() * 1000)
    end_ms = int(datetime.fromisoformat(args.end).timestamp() * 1000) if args.end else None

    cfg = common.load_config()
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    consumer = Consumer({**common.kafka_conf(cfg), "group.id": f"headway-replay-{start_ms}",
                         "enable.auto.commit": False, "enable.partition.eof": True})
    md = consumer.list_topics(timeout=15)
    live = [t for t in args.topics if t in md.topics and not md.topics[t].error]
    for t in set(args.topics) - set(live):
        jlog(level="warn", topic=t, msg="not found, skipping")
    if not live:
        sys.exit("none of the requested topics exist")
    tps = []
    for t in live:
        common.ensure_topic(cfg, t + ".replay", partitions=len(md.topics[t].partitions))
        tps += [TopicPartition(t, p, start_ms) for p in md.topics[t].partitions]

    found = consumer.offsets_for_times(tps, timeout=30)
    for tp in found:
        if tp.error:
            sys.exit(f"offset lookup failed for {tp.topic}[{tp.partition}]: {tp.error}")
        if tp.offset < 0:  # no recorded data at/after start; never assign, or live writes leak in
            jlog(level="warn", topic=tp.topic, partition=tp.partition, msg="no data at/after start")
    assign = [tp for tp in found if tp.offset >= 0]
    if not assign:
        sys.exit("no recorded data at/after --start")
    consumer.assign(assign)
    producer = common.make_producer(cfg)

    active = {(tp.topic, tp.partition) for tp in assign}
    buf = {k: [] for k in active}
    paused = set()
    jlog(msg="replaying", partitions=len(active), speed=args.speed, start=args.start, end=args.end)
    first_ts = wall0 = None
    n = held = 0

    def stop(k):
        active.discard(k)
        consumer.pause([TopicPartition(*k)])  # keep post-EOF live traffic out of the replay

    while active or any(buf.values()):
        # a message is only safe to emit once every still-active partition has a head
        while active and not all(buf[k] for k in active):
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            k = (msg.topic(), msg.partition())
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise SystemExit(f"consume error: {msg.error()}")
                stop(k)
                continue
            ts = msg.timestamp()[1]
            if end_ms and ts > end_ms:
                stop(k)
            elif k in active and ts >= 0:  # a stopped partition's live writes stay out; -1 ts unpaceable
                buf[k].append(msg)
                if len(buf[k]) >= BUF_MAX:
                    consumer.pause([TopicPartition(*k)])
                    paused.add(k)
        ready = [k for k in buf if buf[k]]
        if not ready:
            break
        k = min(ready, key=lambda k: buf[k][0].timestamp()[1])
        msg = buf[k].pop(0)
        if k in paused and k in active and len(buf[k]) < BUF_MAX // 2:
            consumer.resume([TopicPartition(*k)])
            paused.discard(k)
        ts = msg.timestamp()[1]
        if first_ts is None:
            first_ts, wall0 = ts, time.time()
        lag = wall0 + (ts - first_ts) / 1000.0 / args.speed - time.time()
        if lag > 0:
            time.sleep(lag)
        if not common.enqueue(producer, {"topic": msg.topic() + ".replay", "key": msg.key(),
                                         "value": msg.value(), "timestamp": ts}):
            held += 1
        producer.poll(0)
        n += 1
        if n % 5000 == 0:
            jlog(replayed=n, at=datetime.fromtimestamp(ts / 1000).isoformat(), dropped=held)
    queued = producer.flush(60)
    consumer.close()
    jlog(msg="replay done", replayed=n, dropped=held, undelivered=queued)
    sys.exit(1 if held or queued else 0)


if __name__ == "__main__":
    main()
