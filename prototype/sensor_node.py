#!/usr/bin/env python3
"""
Sensor node - simulates the occupancy sensors of one lot.

    python sensor_node.py --lot A

Keeps a fake occupancy table in memory, flips spaces at random, and pushes the
changes to the lot server. Also sends a heartbeat every HEARTBEAT_INTERVAL with the
full table so the server can tell "nothing changed" apart from "sensor is gone".
"""

import argparse
import random
import socket
import time

from common import HEARTBEAT_INTERVAL, LOT_PORTS, MessageLog, now, send_json, ts_str


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lot", required=True)
    ap.add_argument("--server", help="host:port of the lot server")
    ap.add_argument("--spaces", type=int, default=20)
    ap.add_argument("--flip-rate", type=float, default=0.15,
                    help="chance per second that a car arrives or leaves")
    ap.add_argument("--seed", type=int)
    args = ap.parse_args()

    if args.server:
        host, port = args.server.split(":")
        port = int(port)
    else:
        host, port = "127.0.0.1", LOT_PORTS[args.lot]

    rng = random.Random(args.seed)
    name = "sensor_node_" + args.lot
    log = MessageLog(name)
    server = "%s:%d" % (host, port)

    spaces = {}
    for i in range(1, args.spaces + 1):
        sid = "%s-%02d" % (args.lot, i)
        spaces[sid] = rng.choice(["free", "free", "occupied"])
    seq = 0

    def say(text):
        print("[%s] [%s] %s" % (ts_str(now()), name, text), flush=True)

    say("starting with %d spaces, server %s" % (args.spaces, server))

    sock = None
    next_heartbeat = 0.0
    while True:
        if sock is None:
            try:
                sock = socket.create_connection((host, port), timeout=2.0)
                say("connected to " + server)
                next_heartbeat = 0.0    # send a full snapshot right away
            except OSError:
                say("server unreachable, retrying in 1s")
                time.sleep(1.0)
                continue
        try:
            t = now()
            if t >= next_heartbeat:
                seq += 1
                msg = {"type": "heartbeat", "lot": args.lot, "seq": seq,
                       "ts": round(t, 3), "spaces": dict(spaces)}
                log.record("send", server, msg)
                send_json(sock, msg)
                next_heartbeat = t + HEARTBEAT_INTERVAL

            if rng.random() < args.flip_rate:
                sid = rng.choice(list(spaces))
                spaces[sid] = "free" if spaces[sid] == "occupied" else "occupied"
                seq += 1
                msg = {"type": "event", "lot": args.lot, "seq": seq,
                       "ts": round(now(), 3), "space": sid, "state": spaces[sid]}
                say("event %d: %s -> %s" % (seq, sid, spaces[sid]))
                log.record("send", server, msg)
                send_json(sock, msg)

            time.sleep(1.0)
        except (BrokenPipeError, ConnectionResetError, OSError):
            say("connection lost, reconnecting")
            try:
                sock.close()
            except OSError:
                pass
            sock = None


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
