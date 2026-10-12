#!/usr/bin/env python3
"""
Sensor node - simulates the occupancy sensors of one lot.

    python sensor_node.py --lot A
    python sensor_node.py --lot B --drop-rate 0.3    # lose 30% of events (omission failure)

Keeps a fake occupancy table in memory, flips spaces at random, and PUBLISHes
the changes to the broker on topic lot/<lot>/sensor. The lot server is
subscribed to that topic. Every HEARTBEAT_INTERVAL it also publishes a
heartbeat with the full table, so the server can tell "nothing changed" apart
from "sensor is gone", and so a dropped event is repaired on the next beat.

The sensor never talks to the lot server directly and doesn't know its address;
it only knows the broker.
"""

import argparse
import random
import time

from comm import pubsub
from common import BROKER_PORT, HEARTBEAT_INTERVAL, MessageLog, now, sensor_topic, ts_str


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lot", required=True)
    ap.add_argument("--broker", default="127.0.0.1:%d" % BROKER_PORT)
    ap.add_argument("--spaces", type=int, default=20)
    ap.add_argument("--flip-rate", type=float, default=0.15,
                    help="chance per second that a car arrives or leaves")
    ap.add_argument("--drop-rate", type=float, default=0.0,
                    help="chance that an event message is silently dropped")
    ap.add_argument("--seed", type=int)
    args = ap.parse_args()

    bhost, bport = args.broker.split(":")
    rng = random.Random(args.seed)
    name = "sensor_node_" + args.lot
    log = MessageLog(name)
    topic = sensor_topic(args.lot)
    pub = pubsub.Publisher(bhost, int(bport), name, log)

    spaces = {}
    for i in range(1, args.spaces + 1):
        sid = "%s-%02d" % (args.lot, i)
        spaces[sid] = rng.choice(["free", "free", "occupied"])
    seq = 0

    def say(text):
        print("[%s] [%s] %s" % (ts_str(now()), name, text), flush=True)

    def publish(data):
        try:
            n = pub.publish(topic, data)
            if n == 0:
                say("published %s but nobody is subscribed to %s yet" % (data["type"], topic))
            return True
        except OSError as e:
            say("broker unreachable (%s), will retry" % e)
            return False

    say("starting with %d spaces, broker %s, topic %s, drop rate %.2f"
        % (args.spaces, args.broker, topic, args.drop_rate))

    next_heartbeat = 0.0
    while True:
        t = now()
        if t >= next_heartbeat:
            seq += 1
            data = {"type": "heartbeat", "lot": args.lot, "seq": seq,
                    "ts": round(t, 3), "spaces": dict(spaces)}
            if publish(data):
                next_heartbeat = t + HEARTBEAT_INTERVAL
            else:
                next_heartbeat = t + 1.0

        if rng.random() < args.flip_rate:
            sid = rng.choice(list(spaces))
            spaces[sid] = "free" if spaces[sid] == "occupied" else "occupied"
            seq += 1
            data = {"type": "event", "lot": args.lot, "seq": seq,
                    "ts": round(now(), 3), "space": sid, "state": spaces[sid]}
            if rng.random() < args.drop_rate:
                say("DROPPED event %d: %s -> %s" % (seq, sid, spaces[sid]))
                log.record("dropped", args.broker, {"topic": topic, "data": data})
            else:
                say("event %d: %s -> %s" % (seq, sid, spaces[sid]))
                publish(data)

        time.sleep(1.0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
