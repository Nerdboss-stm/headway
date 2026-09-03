#!/usr/bin/env python3
"""Confluent Cloud -> WebSocket bridge for headway-map.

Avro in, JSON out on ws://localhost:8787. Synthetic events are passed through
untouched: the map draws injected trains exactly like real ones.
"""
import argparse
import asyncio
import json
import sys
import threading
import time
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from websockets.asyncio.server import serve

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
import common
from common import jlog

KINDS = {"mta.trip_updates": "train", "citibike.station_status": "station",
         "headway.agent_events": "agent"}
HOST, PORT = "localhost", 8787
STATION_EVERY = 5.0    # at most one message per station per 5s
TRIP_DEBOUNCE = 2.0    # let a whole producer poll land before picking the next stop
SEND_EVERY = 0.1
HOP_SEC = 15.0         # must match HOP_MS in the map's store.js

clients = set()
pending = {}           # trip_id -> [buffered_at, arrival_ts, record]
legs = {}              # trip_id -> leg the train is riding: last stop passed -> next stop
last_station = {}      # station_id -> monotonic clock of last send


def pick(rec, *names):
    for n in names:
        if rec.get(n) is not None:
            return rec[n]
    return None


def shape_station(rec):
    sid = pick(rec, "station_id", "id")
    lat, lon = pick(rec, "lat", "latitude"), pick(rec, "lon", "longitude")
    now = time.monotonic()
    if sid is None or lat is None or lon is None:
        return None
    if now - last_station.get(str(sid), -1e9) < STATION_EVERY:
        return None
    last_station[str(sid)] = now
    return {"type": "station", "id": str(sid), "lat": lat, "lon": lon,
            "bikes_available": pick(rec, "bikes_available", "num_bikes_available") or 0}


def shape_agent(rec):
    keys = ("id", "lat", "lon", "route_id", "severity", "title", "body", "ts", "riders_affected")
    return {"type": "agent", **{k: rec.get(k) for k in keys}}


def buffer_train(rec):
    """The producer emits every upcoming stop; the nearest arrival is the next stop."""
    tid, arr = rec.get("trip_id"), rec.get("arrival_ts")
    if not tid or arr is None or rec.get("lat") is None or rec.get("lon") is None:
        return
    cur = pending.get(tid)
    if cur is None:
        pending[tid] = [time.monotonic(), arr, rec]
    elif arr < cur[1]:
        cur[1], cur[2] = arr, rec


def lerp(a, b, p):
    return [a[0] + (b[0] - a[0]) * p, a[1] + (b[1] - a[1]) * p]


def slice_leg(tid, rec, now):
    """A poll reports the next stop, not a completed leg: the same stop repeats for
    minutes while the train approaches it. Track the leg and cut the next 15s of it."""
    pos, arr, stop = [rec["lon"], rec["lat"]], rec["arrival_ts"], rec.get("stop_id")
    leg = legs.get(tid)
    if leg is None:
        # No history yet, so park it at the next stop; one stop later the leg is real.
        leg = legs[tid] = {"a": pos, "ta": now, "b": pos, "tb": arr}
    elif stop != leg.get("stop"):
        leg["a"], leg["ta"] = leg["b"], min(leg["tb"], now)   # it just passed the old stop
        leg["b"], leg["tb"] = pos, arr
    else:
        leg["b"], leg["tb"] = pos, arr                        # same stop, fresher prediction
    leg["stop"], leg["seen"] = stop, now
    span = leg["tb"] - leg["ta"]
    if span <= 0:
        return leg["b"], leg["b"]
    at = lambda t: min(1.0, max(0.0, (t - leg["ta"]) / span))
    return lerp(leg["a"], leg["b"], at(now)), lerp(leg["a"], leg["b"], at(now + HOP_SEC))


def flush_trains():
    mono, now, out = time.monotonic(), time.time(), []
    for tid in [t for t, v in pending.items() if mono - v[0] >= TRIP_DEBOUNCE]:
        rec = pending.pop(tid)[2]
        frm, to = slice_leg(tid, rec, now)
        out.append({"type": "train", "id": tid, "route_id": rec.get("route_id") or "",
                    "from": frm, "to": to, "delay_sec": rec.get("delay_sec") or 0,
                    "synthetic": bool(rec.get("synthetic"))})
    if len(legs) > 4000:   # trip ids churn daily; drop anything long gone
        for tid in [t for t, v in legs.items() if now - v["seen"] > 3600]:
            del legs[tid]
    return out


def consume(cfg, topics, loop, queue):
    while True:
        con = None
        try:
            sr = SchemaRegistryClient({
                "url": cfg["SR_URL"],
                "basic.auth.user.info": f"{cfg['SR_API_KEY']}:{cfg['SR_API_SECRET']}"})
            deser = AvroDeserializer(sr)
            con = Consumer({**common.kafka_conf(cfg), "auto.offset.reset": "latest",
                            "group.id": f"headway-bridge-{int(time.time())}"})
            con.subscribe(list(topics))
            jlog(msg="consuming", topics=list(topics))
            while True:
                msg = con.poll(1.0)
                if msg is None or msg.error():
                    continue
                rec = deser(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                if rec:
                    loop.call_soon_threadsafe(queue.put_nowait, (topics[msg.topic()], rec))
        except Exception as exc:
            jlog(level="error", stage="consume", error=str(exc))
            if con:
                con.close()
            time.sleep(3)   # and reconnect


async def handler(ws):
    clients.add(ws)
    jlog(msg="client connected", clients=len(clients))
    try:
        await ws.wait_closed()
    finally:
        clients.discard(ws)


async def pump(queue):
    while True:
        await asyncio.sleep(SEND_EVERY)
        batch = []
        while not queue.empty():
            kind, rec = queue.get_nowait()
            if kind == "train":
                buffer_train(rec)
            elif kind == "station":
                shaped = shape_station(rec)
                if shaped:
                    batch.append(shaped)
            else:
                batch.append(shape_agent(rec))
        batch += flush_trains()
        if batch and clients:
            text = json.dumps(batch, default=str)
            await asyncio.gather(*(c.send(text) for c in list(clients)), return_exceptions=True)


async def run(cfg, topics):
    queue = asyncio.Queue()
    threading.Thread(target=consume, args=(cfg, topics, asyncio.get_running_loop(), queue),
                     daemon=True).start()
    async with serve(handler, HOST, PORT):
        jlog(msg="bridge up", url=f"ws://{HOST}:{PORT}", topics=list(topics))
        await pump(queue)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["live", "replay"], default="live")
    ap.add_argument("--port", type=int, default=PORT, help="leave 8787 free for the fake feed")
    args = ap.parse_args()
    PORT = args.port
    suffix = ".replay" if args.source == "replay" else ""
    asyncio.run(run(common.load_config(), {t + suffix: k for t, k in KINDS.items()}))
