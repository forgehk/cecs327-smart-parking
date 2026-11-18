#!/usr/bin/env python3
"""
Race demo for the shared overflow garage (Milestone 4).

Several drivers ask for an overflow space at the same instant, through different
lot servers, and we check what they were told against the shared table.

    python mutex_race.py                 # 6 drivers through lots A, B, C, with the distributed lock
    python mutex_race.py --no-lock       # the same race without the lock: double bookings
    python mutex_race.py --drivers 8 --lots AB
    python mutex_race.py --kill-leader 0.6   # crash the lock coordinator 0.6 s into the race

With the lock every assignment runs inside the critical section, one lot server
at a time, in the order the coordinator grants it (Lamport order of the requests),
so every driver gets a different space and the table agrees with every reply.
Without it, the lot servers read the table at the same moment, all pick the same
first free space and overwrite each other's writes.

--kill-leader S asks who the leader (lock coordinator) is, sends the race only
through the other lots, and S seconds after the start kills the leader's process
outright (a crash, not a clean shutdown). The others elect a new leader, which
asks every lot what it holds and waits for, and carries on granting.

Exit code 0 if no conflict, 3 if a conflict was found.
"""

import argparse
import os
import signal
import sys
import threading
import time

from comm import request_reply
from comm.request_reply import ServiceUnavailable
from common import CLIENT_TIMEOUT, LOT_PORTS, MUTEX_WAIT, MessageLog


def leader_of(lot):
    """(leader id, that leader's pid) as seen through `lot`, or (None, None)."""
    log = MessageLog("client_race_admin")
    try:
        r = request_reply.call("127.0.0.1", LOT_PORTS[lot], "leader", {}, "client_race_admin", log,
                               timeout=CLIENT_TIMEOUT, retries=2)
        lid = r["result"]["leader"]
        r = request_reply.call("127.0.0.1", LOT_PORTS[lid], "leader", {}, "client_race_admin", log,
                               timeout=CLIENT_TIMEOUT, retries=2)
        return lid, r["result"].get("pid")
    except (ServiceUnavailable, KeyError, TypeError):
        return None, None


def crash(pid):
    """Kill a process without letting it clean up: SIGKILL on Linux/macOS,
    TerminateProcess on Windows (which is what os.kill does there)."""
    os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))


def call(lot, client, action, no_lock=False):
    log = MessageLog("client_" + client)
    try:
        r = request_reply.call("127.0.0.1", LOT_PORTS[lot], "overflow",
                               {"client": client, "action": action, "no_lock": no_lock},
                               "client_" + client, log, timeout=MUTEX_WAIT + 5, retries=1)
    except ServiceUnavailable as e:
        return {"error": str(e)}
    return r.get("result") if r.get("status") == "OK" else {"error": r.get("error")}


def main():
    ap = argparse.ArgumentParser(description="Many drivers ask for an overflow space at once.")
    ap.add_argument("--drivers", type=int, default=6)
    ap.add_argument("--lots", default="ABC", help="lot servers to send the requests through, round robin")
    ap.add_argument("--no-lock", action="store_true", help="skip the distributed lock")
    ap.add_argument("--no-reset", action="store_true", help="keep the current table instead of emptying it first")
    ap.add_argument("--kill-leader", type=float, metavar="S",
                    help="crash the leader (lock coordinator) S seconds after the race starts")
    args = ap.parse_args()
    lots = [l for l in args.lots if l in LOT_PORTS]

    victim = None
    if args.kill_leader is not None:
        lid, pid = leader_of(lots[0])
        if not pid:
            sys.exit("could not find the leader's process through lot %s" % lots[0])
        victim = (lid, pid)
        lots = [l for l in lots if l != lid]        # requests through a dead lot server would just fail
        if not lots:
            sys.exit("every lot in --lots is the leader; give at least one other lot")
        print("the leader (lock coordinator) is lot %s, pid %d: it will be killed %.1fs into the race; "
              "requests go through lots %s" % (lid, pid, args.kill_leader, ",".join(lots)))

    if not args.no_reset:
        r = call(lots[0], "race_admin", "reset")
        if "error" in r:
            sys.exit("could not reset the table through lot %s: %s" % (lots[0], r["error"]))
        print("table reset through lot %s (inside the lock)" % lots[0])

    drivers = ["driver%d" % k for k in range(1, args.drivers + 1)]
    via = {d: lots[k % len(lots)] for k, d in enumerate(drivers)}
    replies = {}
    start = threading.Barrier(len(drivers))

    def one(d):
        start.wait()                        # everyone fires at the same moment
        replies[d] = call(via[d], d, "assign", args.no_lock)
    threads = [threading.Thread(target=one, args=(d,)) for d in drivers]
    print("%d drivers ask lots %s for an overflow space at the same time, %s"
          % (len(drivers), ",".join(lots), "WITHOUT the lock" if args.no_lock else "with the distributed lock"))
    if victim:
        def killer():
            time.sleep(args.kill_leader)
            crash(victim[1])
            print("  ... %.1fs in: killed the leader, lot %s (pid %d)" % (args.kill_leader, victim[0], victim[1]), flush=True)
        threading.Thread(target=killer, daemon=True).start()
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("  ... all %d replies in after %.1fs" % (len(drivers), time.time() - t0))
    if victim:
        lid, _ = leader_of(lots[0])
        print("  ... the leader is now lot %s" % lid)

    table = call(lots[0], "race_admin", "status")
    print()
    print("  driver     via  told        waited  version")
    for d in drivers:
        r = replies[d]
        if "error" in r:
            print("  %-10s %-4s ERROR %s" % (d, via[d], r["error"]))
            continue
        print("  %-10s %-4s %-11s %5.2fs  %s" % (d, via[d], r["space"] or "(%s)" % r["reason"], r["waited"], r["version"]))
    print()
    print("  shared table now (version %s): %s" % (table.get("version"), table.get("taken")))

    told = {}
    for d in drivers:
        sid = replies[d].get("space")
        if sid:
            told.setdefault(sid, []).append(d)
    double = {sid: ds for sid, ds in told.items() if len(ds) > 1}
    lost = [(d, replies[d]["space"], table["taken"].get(replies[d]["space"])) for d in drivers
            if replies[d].get("space") and table.get("taken", {}).get(replies[d]["space"]) != d]
    versions = [replies[d]["version"] for d in drivers if replies[d].get("space")]
    dup_versions = sorted({v for v in versions if versions.count(v) > 1})

    print()
    if double or lost:
        for sid, ds in sorted(double.items()):
            print("  CONFLICT: %s was given to %d drivers: %s" % (sid, len(ds), ", ".join("%s (via %s)" % (d, via[d]) for d in ds)))
        for d, sid, owner in lost:
            print("  LOST UPDATE: %s was told %s, but the table says %s has it" % (d, sid, owner))
        if dup_versions:
            print("  several writes produced the same table version %s: they overwrote each other" % dup_versions)
        sys.exit(3)
    n = len(told)
    print("  no conflict: %d driver(s) got %d different space(s), the table agrees with every reply%s"
          % (n, n, ", %d told the garage is full" % (len(drivers) - n) if len(drivers) > n else ""))


if __name__ == "__main__":
    main()
