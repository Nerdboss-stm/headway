#!/usr/bin/env python3
"""Emit a synthetic stalled-train cascade: an L train stopped at Bedford Av (L06S),
then 4 trailing southbound trains bunching up behind it with compounding delays."""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
import common
from common import jlog

ROUTE, STOP_ID, DIRECTION = "L", "L06S", "S"
FALLBACK = ["Bedford Av", 40.717304, -73.956872]
TICK, STALL_SEC, DELAY_FROM, DELAY_TO = 5, 90, 60, 900
SCHED = [240, 420, 540, 630]  # scheduled arrival offsets: headways 240/180/120/90, shrinking
GAPS = [300, 520, 670, 780]   # settled gaps behind the lead: 300/220/150/110, bunched
TRAIL_TICKS = 12


def main():
    cfg = common.load_config()
    common.ensure_topic(cfg, common.TOPIC)
    producer = common.make_producer(cfg)
    serializer = common.avro_value_serializer(cfg)
    try:  # reuse the producer's cached stop metadata when it is present
        stops = json.loads((common.ROOT / ".cache" / "stops.json").read_text())
        name, lat, lon = stops.get(STOP_ID) or stops.get("L06") or FALLBACK
    except (OSError, ValueError):
        name, lat, lon = FALLBACK
    t0 = time.time()
    run = int(t0)
    errs = {"n": 0, "last": None}

    def on_delivery(err, _msg):
        if err is not None:
            errs["n"] += 1
            errs["last"] = str(err)

    def emit(trip_id, sched_ts, delay, status):
        ev = {"trip_id": trip_id, "route_id": ROUTE, "stop_id": STOP_ID, "stop_name": name,
              "lat": lat, "lon": lon, "direction": DIRECTION,
              "arrival_ts": int(sched_ts + delay), "delay_sec": int(delay),
              "feed_ts": int(time.time()), "vehicle_status": status, "synthetic": True}
        if not common.produce_event(producer, serializer, ev, on_delivery):
            errs["n"] += 1
        producer.poll(0)
        return ev

    lead = f"SYN-{run}-L-lead"
    jlog(msg="phase 1: lead train stalls at Bedford Av", trip_id=lead, duration_sec=STALL_SEC)
    for i in range(0, STALL_SEC + 1, TICK):
        ev = emit(lead, t0, DELAY_FROM + (DELAY_TO - DELAY_FROM) * i / STALL_SEC, "STOPPED_AT")
        jlog(trip_id=lead, delay_sec=ev["delay_sec"], vehicle_status="STOPPED_AT")
        if i < STALL_SEC:
            time.sleep(TICK)

    trails = [(f"SYN-{run}-L-trail{k + 1}", SCHED[k], GAPS[k]) for k in range(len(SCHED))]
    jlog(msg="phase 2: trailing trains bunch up", delays=[DELAY_TO + g - s for _, s, g in trails])
    for j in range(1, TRAIL_TICKS + 1):
        f = 0.5 + 0.5 * j / TRAIL_TICKS  # the delay propagates back down the line over the window
        emit(lead, t0, DELAY_TO, "STOPPED_AT")
        for k, (trip, sched, gap) in enumerate(trails):
            # arrival stays behind the stalled lead at t0+DELAY_TO, so no train ever overtakes
            emit(trip, t0 + sched, DELAY_TO + gap * f - sched,
                 "STOPPED_AT" if k == 0 else "IN_TRANSIT_TO")
        producer.flush(10)
        jlog(tick=j, of=TRAIL_TICKS, trains=len(trails) + 1)
        if j < TRAIL_TICKS:
            time.sleep(TICK)
    queued = producer.flush(30)
    jlog(msg="inject done", delivery_errors=errs["n"], last_error=errs["last"], undelivered=queued)
    sys.exit(1 if errs["n"] or queued else 0)


if __name__ == "__main__":
    main()
