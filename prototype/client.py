#!/usr/bin/env python3
"""
Driver client.

    python client.py find
    python client.py --lot B find
"""

import argparse
import json
import socket
import sys
import uuid

from common import (CLIENT_RETRIES, CLIENT_TIMEOUT, LOT_PORTS, MessageLog, now,
                    read_lines, send_json, ts_str)


def request(host, port, msg, log):
    """Send one request and wait for the reply. Retries on timeout."""
    peer = "%s:%d" % (host, port)
    for attempt in range(1, CLIENT_RETRIES + 1):
        try:
            with socket.create_connection((host, port), timeout=CLIENT_TIMEOUT) as s:
                log.record("send", peer, msg)
                send_json(s, msg)
                for reply in read_lines(s):
                    log.record("recv", peer, reply)
                    return reply
            print("[%s] [client] no reply (attempt %d/%d)" % (ts_str(now()), attempt, CLIENT_RETRIES))
        except socket.timeout:
            print("[%s] [client] timeout after %.0fs (attempt %d/%d)"
                  % (ts_str(now()), CLIENT_TIMEOUT, attempt, CLIENT_RETRIES))
        except OSError as e:
            print("[%s] [client] server unreachable: %s (attempt %d/%d)"
                  % (ts_str(now()), e, attempt, CLIENT_RETRIES))
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="driver1")
    ap.add_argument("--lot", default="A")
    ap.add_argument("--server", help="host:port override")
    ap.add_argument("--json", action="store_true", help="print the raw reply")
    ap.add_argument("op", choices=["find"])
    args = ap.parse_args()

    if args.server:
        host, port = args.server.split(":")
        port = int(port)
    else:
        host, port = "127.0.0.1", LOT_PORTS[args.lot]

    log = MessageLog("client_" + args.id)
    msg = {"type": args.op, "req_id": str(uuid.uuid4())[:8],
           "client": args.id, "ts": round(now(), 3)}

    print("[%s] [client %s] -> %s (req_id %s) to %s:%d"
          % (ts_str(now()), args.id, args.op, msg["req_id"], host, port))
    reply = request(host, port, msg, log)
    if reply is None:
        print("[%s] [client %s] FAILED: no reply" % (ts_str(now()), args.id))
        sys.exit(2)
    if args.json:
        print(json.dumps(reply, indent=2))
        return

    s = reply["lot_summary"]
    print("[%s] [client %s] <- OK Lot %s: %d free, %d occupied, %d unknown"
          % (ts_str(now()), args.id, reply["lot"], s["free"], s["occupied"], s["unknown"]))
    print("   free spaces:", ", ".join(sp["id"] for sp in reply["free"]) or "(none)")


if __name__ == "__main__":
    main()
