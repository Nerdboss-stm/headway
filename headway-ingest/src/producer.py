#!/usr/bin/env python3
"""Poll MTA subway GTFS-realtime feeds and produce one event per upcoming stop to Kafka."""
import argparse
import csv
import io
import json
import time
import zipfile

import requests
from google.transit import gtfs_realtime_pb2

import common
from common import jlog

FEED_BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2F"
FEED_NAMES = ["gtfs", "gtfs-ace", "gtfs-bdfm", "gtfs-nqrw", "gtfs-l", "gtfs-g"]
STATIC_GTFS = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"
STOPS_CACHE = common.ROOT / ".cache" / "stops.json"


def load_stops():
    if STOPS_CACHE.exists():
        try:
            return json.loads(STOPS_CACHE.read_text())
        except (OSError, ValueError):
            jlog(level="warn", msg="stops cache unreadable, re-downloading")
    r = requests.get(STATIC_GTFS, timeout=90)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)).open("stops.txt") as f:
        stops = {row["stop_id"]: [row["stop_name"], float(row["stop_lat"]), float(row["stop_lon"])]
                 for row in csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))}
    STOPS_CACHE.parent.mkdir(exist_ok=True)
    tmp = STOPS_CACHE.with_suffix(".tmp")
    tmp.write_text(json.dumps(stops))
    tmp.replace(STOPS_CACHE)  # atomic, so an interrupted write never leaves a half cache
    jlog(msg="cached stops", count=len(stops))
    return stops


def parse_feed(data, stops, now):
    fm = gtfs_realtime_pb2.FeedMessage()
    fm.ParseFromString(data)
    feed_ts = int(fm.header.timestamp or now)
    status_name = gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name
    vstatus = {e.vehicle.trip.trip_id: status_name(e.vehicle.current_status) for e in fm.entity
               if e.HasField("vehicle") and e.vehicle.trip.trip_id}
    events = []
    for e in fm.entity:
        if not e.HasField("trip_update"):
            continue
        tu = e.trip_update
        for stu in tu.stop_time_update:
            arr, dep = stu.arrival.time, stu.departure.time
            # upcoming means not yet departed, so a train dwelling at a stop still counts
            if max(arr, dep) < now:
                continue
            stop = stops.get(stu.stop_id) or stops.get(stu.stop_id[:-1]) or [None, None, None]
            events.append({
                "trip_id": tu.trip.trip_id, "route_id": tu.trip.route_id, "stop_id": stu.stop_id,
                "stop_name": stop[0], "lat": stop[1], "lon": stop[2],
                "direction": stu.stop_id[-1] if stu.stop_id[-1:] in ("N", "S") else "",
                "arrival_ts": int(arr or dep), "feed_ts": feed_ts, "synthetic": False,
                "delay_sec": int(stu.arrival.delay if arr else stu.departure.delay),
                "vehicle_status": vstatus.get(tu.trip.trip_id)})
    return events


def poll_once(stops, now):
    per_feed = {}
    for name in FEED_NAMES:
        try:
            r = requests.get(FEED_BASE + name, timeout=10)
            r.raise_for_status()
            per_feed[name] = parse_feed(r.content, stops, now)
        except Exception as exc:
            jlog(level="warn", feed=name, error=str(exc))
    return per_feed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="parse feeds once and print, no Kafka")
    ap.add_argument("--interval", type=float, default=15.0)
    args = ap.parse_args()
    stops = load_stops()

    if args.dry_run:
        for name, evs in poll_once(stops, time.time()).items():
            jlog(feed=name, events=len(evs), sample=evs[0] if evs else None)
        return

    cfg = common.load_config()
    common.ensure_topic(cfg, common.TOPIC)
    producer = common.make_producer(cfg)
    serializer = common.avro_value_serializer(cfg)
    errs = {"n": 0, "last": None}

    def on_delivery(err, _msg):
        if err is not None:
            errs["n"] += 1
            errs["last"] = str(err)

    jlog(msg="producing", topic=common.TOPIC, feeds=len(FEED_NAMES), interval_sec=args.interval)
    while True:
        start = time.time()
        per_feed = poll_once(stops, start)
        sent = dropped = queued = 0
        try:
            for evs in per_feed.values():
                for ev in evs:
                    ok = common.produce_event(producer, serializer, ev, on_delivery)
                    sent, dropped = sent + ok, dropped + (not ok)
                producer.poll(0)
            queued = producer.flush(30)
        except Exception as exc:  # Schema Registry outage etc: log, keep the long-running loop alive
            jlog(level="error", stage="produce", error=str(exc))
        elapsed = time.time() - start
        jlog(events=sent, feeds_ok=len(per_feed), elapsed_sec=round(elapsed, 2),
             events_per_sec=round(sent / max(elapsed, 0.001), 1), dropped=dropped,
             queued=queued, delivery_errors=errs["n"], last_error=errs["last"])
        time.sleep(max(0.0, args.interval - elapsed))


if __name__ == "__main__":
    main()
