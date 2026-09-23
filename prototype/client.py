#!/usr/bin/env python3
"""
Driver client.

    python client.py find
    python client.py --lot B find
    python client.py --id alice reserve A-03
    python client.py --id alice release A-03

Reservations are leases: they expire on their own after a while, so a client
that dies still can't hold a space forever.

Every request has a req_id. If we don't get a reply in time we send the same
request again with the same id, so the server can recognise the retry.
--simulate-lost-reply throws away the first reply on purpose to show this.
"""

import argparse
import json
import socket
import sys
import uuid

from common import (CLIENT_RETRIES, CLIENT_TIMEOUT, LOT_PORTS, MessageLog, now,
                    read_lines, send_json, ts_str)


def request(host, port, msg, log, simulate_lost_reply=False):
    """Send one request and wait for the reply. Retries on timeout with the same req_id."""
    peer = "%s:%d" % (host, port)
    for attempt in range(1, CLIENT_RETRIES + 1):
        try:
            with socket.create_connection((host, port), timeout=CLIENT_TIMEOUT) as s:
                log.record("send", peer, msg)
                send_json(s, msg)
                if simulate_lost_reply and attempt == 1:
                    print("[%s] [client] (demo) pretending reply to %s got lost, retrying with same req_id"
                          % (ts_str(now()), msg["req_id"]))
                    continue
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
    ap.add_argument("--simulate-lost-reply", action="store_true")
    ap.add_argument("op", choices=["find", "reserve", "release"])
    ap.add_argument("space", nargs="?")
    args = ap.parse_args()

    if args.op in ("reserve", "release") and not args.space:
        sys.exit("reserve/release need a space id, e.g. A-03")

    if args.server:
        host, port = args.server.split(":")
        port = int(port)
    else:
        # the space id tells us which lot to talk to
        lot = args.space.split("-")[0] if args.space else args.lot
        host, port = "127.0.0.1", LOT_PORTS.get(lot, LOT_PORTS[args.lot])

    log = MessageLog("client_" + args.id)
    msg = {"type": args.op, "req_id": str(uuid.uuid4())[:8],
           "client": args.id, "ts": round(now(), 3)}
    if args.space:
        msg["space"] = args.space

    print("[%s] [client %s] -> %s %s (req_id %s) to %s:%d"
          % (ts_str(now()), args.id, args.op, args.space or "", msg["req_id"], host, port))
    reply = request(host, port, msg, log, args.simulate_lost_reply)
    if reply is None:
        print("[%s] [client %s] FAILED: no reply" % (ts_str(now()), args.id))
        sys.exit(2)
    if args.json:
        print(json.dumps(reply, indent=2))
        return

    dup = " (duplicate - cached reply)" if reply.get("duplicate") else ""
    if args.op == "find":
        s = reply["lot_summary"]
        print("[%s] [client %s] <- OK Lot %s: %d free, %d occupied, %d reserved, %d unknown%s"
              % (ts_str(now()), args.id, reply["lot"], s["free"], s["occupied"],
                 s["reserved"], s["unknown"], dup))
        print("   free spaces:", ", ".join(sp["id"] for sp in reply["free"]) or "(none)")
    else:
        extra = " (lease %.0fs)" % reply["lease"] if reply.get("lease") else ""
        print("[%s] [client %s] <- %s %s%s%s"
              % (ts_str(now()), args.id, reply["status"], reply.get("reason", ""), extra, dup))
    sys.exit(0 if reply["status"] == "OK" else 1)


if __name__ == "__main__":
    main()
