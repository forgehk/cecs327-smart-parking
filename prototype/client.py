#!/usr/bin/env python3
"""
Driver client.

    python client.py find
    python client.py --lot B find
    python client.py --id alice reserve A-03
    python client.py --id alice release A-03
    python client.py watch A                     # subscribe to lot A status changes (Ctrl-C to stop)

find / reserve / release use request-reply: one connection to the lot server,
one REQUEST, one REPLY. Reservations are leases: they expire on their own after
a while, so a client that dies still can't hold a space forever.

Every request has a msg_id. If we don't get a reply in time we send the same
request again with the same id, so the server can recognise the retry.
--simulate-lost-reply throws away the first reply on purpose to show this.

watch uses pub-sub: it subscribes to lot/<lot>/status at the broker and prints
every EVENT the lot server publishes.
"""

import argparse
import json
import sys
import time

from comm import envelope, pubsub, request_reply
from comm.request_reply import ServiceUnavailable
from common import (BROKER_PORT, CLIENT_RETRIES, CLIENT_TIMEOUT, LOT_PORTS, MessageLog, now,
                    status_topic, ts_str)


def say(client, text):
    print("[%s] [client %s] %s" % (ts_str(now()), client, text), flush=True)


def do_request(args, host, port, log):
    sender = "client_" + args.id
    req_args = {"client": args.id}
    if args.space:
        req_args["space"] = args.space

    msg_id = envelope.new_id()
    say(args.id, "-> %s %s (msg_id %s) to %s:%d" % (args.op, args.space or "", msg_id, host, port))
    try:
        reply = request_reply.call(host, port, args.op, req_args, sender, log,
                                   timeout=CLIENT_TIMEOUT, retries=CLIENT_RETRIES,
                                   msg_id=msg_id, drop_first_reply=args.simulate_lost_reply)
    except ServiceUnavailable as e:
        say(args.id, "FAILED: %s" % e)
        sys.exit(2)

    if args.json:
        print(json.dumps(reply, indent=2))
        return

    dup = " (duplicate - cached reply)" if reply.get("duplicate") else ""
    if reply["status"] != "OK":
        say(args.id, "<- ERROR %s%s" % (reply.get("error"), dup))
        sys.exit(1)

    r = reply["result"]
    if args.op == "find":
        s = r["lot_summary"]
        say(args.id, "<- OK Lot %s: %d free, %d occupied, %d reserved, %d unknown%s"
            % (r["lot"], s["free"], s["occupied"], s["reserved"], s["unknown"], dup))
        print("   free spaces:", ", ".join(sp["id"] for sp in r["free"]) or "(none)")
        for lot, v in r.get("other_lots", {}).items():
            if v["free"] is None:
                print("   Lot %s: no summary yet (peer unreachable)" % lot)
            else:
                print("   Lot %s: %d free, %d unknown - %.1fs old%s"
                      % (lot, v["free"], v["unknown"], v["age"], " [STALE]" if v["stale"] else ""))
        return
    verdict = "OK" if r["granted"] else "DENIED"
    extra = " (lease %.0fs)" % r["lease"] if r.get("lease") else ""
    say(args.id, "<- %s %s%s%s" % (verdict, r.get("reason", ""), extra, dup))
    sys.exit(0 if r["granted"] else 1)


def do_watch(args, log):
    lot = args.space or args.lot
    topic = status_topic(lot)
    bhost, bport = args.broker.split(":")

    def on_event(topic, data, msg):
        who = (" by " + data["reserved_by"]) if data.get("reserved_by") else ""
        say(args.id, "EVENT %s -> %s%s   (published by %s at %s)"
            % (data["space"], data["state"], who, data.get("publisher", "?"), msg["timestamp"][11:]))

    sub = pubsub.Subscriber(bhost, int(bport), "client_" + args.id, log, on_event).start()
    sub.subscribe(topic)
    say(args.id, "subscribed to %s via broker %s, waiting for events (Ctrl-C to stop)" % (topic, args.broker))
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        sub.unsubscribe(topic)
        time.sleep(0.2)
        sub.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="driver1")
    ap.add_argument("--lot", default="A")
    ap.add_argument("--server", help="host:port override for the lot server")
    ap.add_argument("--broker", default="127.0.0.1:%d" % BROKER_PORT)
    ap.add_argument("--json", action="store_true", help="print the raw reply")
    ap.add_argument("--simulate-lost-reply", action="store_true")
    ap.add_argument("op", choices=["find", "reserve", "release", "watch"])
    ap.add_argument("space", nargs="?", help="space id (reserve/release) or lot id (watch)")
    args = ap.parse_args()

    log = MessageLog("client_" + args.id)

    if args.op == "watch":
        do_watch(args, log)
        return

    if args.op in ("reserve", "release") and not args.space:
        sys.exit("reserve/release need a space id, e.g. A-03")
    if args.server:
        host, port = args.server.split(":")
        port = int(port)
    else:
        lot = args.space.split("-")[0] if args.space else args.lot
        host, port = "127.0.0.1", LOT_PORTS.get(lot, LOT_PORTS[args.lot])
    do_request(args, host, port, log)


if __name__ == "__main__":
    main()
