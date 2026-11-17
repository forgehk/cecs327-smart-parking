#!/usr/bin/env python3
"""
Merge every process's log into one timeline ordered by Lamport time (Milestone 4).

    python event_order.py                          # every logged message, in Lamport order
    python event_order.py --types REQUEST,REPLY    # only some message types
    python event_order.py --process lot_server_A --last 30
    python event_order.py --grep A-03              # lines mentioning A-03
    python event_order.py --check                  # verify the clock condition, print a summary

Each log line already has the process's Lamport time for that event (see
comm/clock.py). Sorting by (lamport, process) gives one total order that agrees
with happened-before: a receive always sorts after the send it received, and a
reply after the request that caused it. Wall-clock times are printed next to it
for comparison; across processes they are not guaranteed to agree.

--check verifies, for every receive in every log:
    receiver's time after the receive  >  the stamp the message was sent with
and for every REPLY/ACK whose request is also logged:
    reply's stamp  >  request's stamp
"""

import argparse
import glob
import json
import os
import sys
import time

from common import LOG_DIR


def load(log_dir):
    entries = []
    for path in sorted(glob.glob(os.path.join(log_dir, "*.log"))):
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                try:
                    e = json.loads(raw)
                except json.JSONDecodeError:
                    continue                # half-written line after a crash
                if "lamport" in e:          # logs from before Milestone 4 have none
                    entries.append(e)
    return entries


def describe(msg):
    """Short text for one message: what it was about."""
    if not isinstance(msg, dict):
        return str(msg)
    if "type" not in msg:
        return msg.get("note", json.dumps(msg))[:90]
    p = msg.get("payload", {})
    t = msg["type"]
    if t == "REQUEST":
        a = p.get("args", {})
        return "%s %s" % (p.get("op"), " ".join("%s=%s" % kv for kv in sorted(a.items()) if not isinstance(kv[1], (dict, list))))
    if t == "REPLY":
        r = p.get("result", {}) or {}
        bits = [p.get("status")] + ["%s=%s" % (k, r[k]) for k in ("op", "space", "granted", "reason") if k in r]
        return " ".join(str(b) for b in bits if b is not None) or p.get("error", "")
    if t in ("PUBLISH", "EVENT", "SUBSCRIBE", "UNSUBSCRIBE"):
        d = p.get("data", {}) or {}
        return "%s %s" % (p.get("topic"), " ".join("%s=%s" % (k, d[k]) for k in ("space", "state", "type") if k in d))
    text = json.dumps({k: v for k, v in p.items() if k not in ("table", "neighbors", "sample")})
    return text if len(text) <= 90 else text[:87] + "..."


def fmt(e):
    msg = e.get("msg", {})
    mtype = msg.get("type", "-") if isinstance(msg, dict) else "-"
    stamp = msg.get("lamport") if isinstance(msg, dict) else None
    arrow = {"send": "->", "recv": "<-"}.get(e["dir"], "  ")
    carried = " (sent at %s)" % stamp if e["dir"] == "recv" and stamp is not None else ""
    wall = time.strftime("%H:%M:%S", time.localtime(e["ts"])) + ".%03d" % (int(e["ts"] * 1000) % 1000)
    return "L=%-5d %s  %-16s %-4s %-12s %2s %-21s %s%s" % (
        e["lamport"], wall, e["process"], e["dir"], mtype, arrow, e.get("peer", ""), describe(msg), carried)


def check(entries):
    """Verify the clock condition on every receive and request/reply pair."""
    recv_ok = recv_bad = 0
    bad = []
    sent = {}                                   # msg_id -> smallest stamp it was sent with
    for e in entries:
        m = e.get("msg")
        if e["dir"] == "send" and isinstance(m, dict) and "lamport" in m:
            sent[m["msg_id"]] = min(sent.get(m["msg_id"], m["lamport"]), m["lamport"])
    for e in entries:
        m = e.get("msg")
        if e["dir"] != "recv" or not isinstance(m, dict) or "lamport" not in m:
            continue
        if e["lamport"] > m["lamport"]:
            recv_ok += 1
        else:
            recv_bad += 1
            bad.append("receive not after its send: " + fmt(e))
    pair_ok = pair_bad = 0
    for e in entries:
        m = e.get("msg")
        if e["dir"] != "send" or not isinstance(m, dict) or not m.get("reply_to"):
            continue
        req = sent.get(m["reply_to"])
        if req is None:
            continue                            # the request was not logged by anyone we have
        if m["lamport"] > req:
            pair_ok += 1
        else:
            pair_bad += 1
            bad.append("reply not after its request: " + fmt(e))
    return recv_ok, recv_bad, pair_ok, pair_bad, bad


def main():
    ap = argparse.ArgumentParser(description="Merge all logs into one Lamport-ordered timeline.")
    ap.add_argument("--dir", default=LOG_DIR, help="folder with the *.log files (default: logs/)")
    ap.add_argument("--types", help="comma-separated message types to show, e.g. REQUEST,REPLY")
    ap.add_argument("--process", help="only lines from processes whose name contains this")
    ap.add_argument("--grep", help="only lines containing this text")
    ap.add_argument("--last", type=int, help="only the last N lines")
    ap.add_argument("--no-info", action="store_true", help="hide info notes, show only sends and receives")
    ap.add_argument("--check", action="store_true", help="verify the clock condition and print a summary")
    args = ap.parse_args()

    entries = load(args.dir)
    if not entries:
        sys.exit("no log lines with Lamport times in %s (run the system first)" % args.dir)
    entries.sort(key=lambda e: (e["lamport"], e["process"], e["ts"]))

    if args.check:
        recv_ok, recv_bad, pair_ok, pair_bad, bad = check(entries)
        procs = sorted({e["process"] for e in entries})
        print("%d log lines from %d processes: %s" % (len(entries), len(procs), ", ".join(procs)))
        print("receives after their send (C_recv > C_msg): %d ok, %d violated" % (recv_ok, recv_bad))
        print("replies after their request (C_reply > C_request): %d ok, %d violated" % (pair_ok, pair_bad))
        for line in bad[:20]:
            print("  " + line)
        print("clock condition HOLDS" if not (recv_bad or pair_bad) else "clock condition VIOLATED")
        sys.exit(1 if (recv_bad or pair_bad) else 0)

    types = set(args.types.split(",")) if args.types else None
    out = []
    for e in entries:
        m = e.get("msg")
        mtype = m.get("type") if isinstance(m, dict) else None
        if args.no_info and e["dir"] not in ("send", "recv"):
            continue
        if types and mtype not in types:
            continue
        if args.process and args.process not in e["process"]:
            continue
        line = fmt(e)
        if args.grep and args.grep not in line:
            continue
        out.append(line)
    if args.last:
        out = out[-args.last:]
    print("\n".join(out))


if __name__ == "__main__":
    main()
