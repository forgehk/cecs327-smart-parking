#!/usr/bin/env python3
"""
Tests for centralized mutual exclusion with the elected leader as coordinator,
and for the shared overflow table it protects (Milestone 4). Three nodes A, B, C
run in threads inside this process, each with a Service, an Election and a
Mutex on a real TCP port, with short timers.

    cd prototype && python tests/test_mutex.py
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

PROTO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROTO)

from comm import request_reply
from common import MessageLog
from coord import wire
from coord.election import Election
from coord.mutex import Mutex
from overflow import OverflowTable

FAILS = []
BASE = 5940          # test ports: A 5940, B 5941, C 5942
IDS = "ABC"
MEMBERS = {i: "127.0.0.1:%d" % (BASE + k) for k, i in enumerate(IDS)}
FAST = dict(answer_timeout=0.5, coordinator_timeout=1.5, check_interval=0.5, ping_timeout=0.5)
HOLD_LIMIT = 2.0
TMP = tempfile.mkdtemp()
TABLE = os.path.join(TMP, "overflow.json")

nodes = {}           # id -> {"svc", "el", "mx"}


def check(cond, text):
    print(("  PASS  " if cond else "  FAIL  ") + text)
    if not cond:
        FAILS.append(text)


def clear_logs():
    d = os.path.dirname(MessageLog("x").path)
    for f in os.listdir(d):
        if f.startswith("tm_"):
            os.remove(os.path.join(d, f))


def log_lines(nid):
    path = MessageLog("tm_" + nid).path
    return [json.loads(l) for l in open(path, encoding="utf-8")] if os.path.exists(path) else []


def start(nid):
    log = MessageLog("tm_" + nid)
    svc = request_reply.Service("tm_" + nid, BASE + IDS.index(nid), log, workers=16)
    box = {}
    el = Election(nid, MEMBERS[nid], MEMBERS, svc, log, info=lambda t: None,
                  on_change=lambda o, n: box["mx"].on_leader_change(o, n),
                  on_elected=lambda: box["mx"].become_coordinator(), **FAST)
    mx = Mutex(nid, MEMBERS[nid], el, svc, log, info=lambda t: None, hold_limit=HOLD_LIMIT, retry=0.2)
    box["mx"] = mx
    threading.Thread(target=svc.serve_forever, daemon=True).start()
    nodes[nid] = {"svc": svc, "el": el, "mx": mx}
    mx.start()
    el.start()


def crash(nid):
    n = nodes.pop(nid)
    n["el"].stop()
    n["mx"].stop()
    n["svc"].stop()


def wait_for(pred, timeout=10.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return time.time() - t0
        time.sleep(0.05)
    return None


def coordinating(nid):
    return nid in nodes and nodes[nid]["mx"].status()["coordinating"]


def all_follow(lid):
    return all(n["el"].current()[0] == lid for n in nodes.values())


# ------------------------------------------------------------------ tests

def test_basic():
    print("[request, grant, queue, release]")
    for nid in IDS:
        start(nid)
    took = wait_for(lambda: all_follow("C") and coordinating("C"))
    check(took is not None, "C is leader and coordinates the lock after %.1fs" % (took or -1))
    A, B = nodes["A"]["mx"], nodes["B"]["mx"]
    check(A.acquire("r", 5) and A.holds("r"), "A asks for r while it is free -> granted at once")
    got = {}

    def b():
        got["ok"] = B.acquire("r", 10)
        got["t"] = time.time()
    t = threading.Thread(target=b)
    t.start()
    time.sleep(0.6)
    check(not B.holds("r") and nodes["C"]["mx"].status()["queues"].get("r") == ["B"],
          "B asks while A holds it -> B waits in the coordinator's queue %s" % nodes["C"]["mx"].status()["queues"])
    released = time.time()
    A.release("r")
    t.join(5)
    check(got.get("ok") and got["t"] >= released, "A releases -> the coordinator grants r to B (%.2fs later)" % (got.get("t", 0) - released))
    grants = [l for l in log_lines("C") if l["dir"] == "send" and l["msg"]["type"] == "MUTEX_GRANT"]
    check(grants and grants[-1]["peer"] == MEMBERS["B"], "C's log has the MUTEX_GRANT sent to B")
    B.release("r")


def test_exclusion():
    print("[mutual exclusion under contention: 3 nodes x 4 threads x 3 rounds]")
    inside = {"now": 0, "max": 0, "done": 0}
    lk = threading.Lock()

    def worker(nid):
        mx = nodes[nid]["mx"]
        for _ in range(3):
            if not mx.acquire("cs", 30):
                return
            with lk:
                inside["now"] += 1
                inside["max"] = max(inside["max"], inside["now"])
            time.sleep(0.02)
            with lk:
                inside["now"] -= 1
                inside["done"] += 1
            mx.release("cs")
    ts = [threading.Thread(target=worker, args=(nid,)) for nid in IDS for _ in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(60)
    check(inside["done"] == 36, "all 36 critical sections ran (%d)" % inside["done"])
    check(inside["max"] == 1, "never more than one inside at a time (max %d)" % inside["max"])


def assign_all(no_lock, n=6):
    """n drivers, two per node, assign at the same instant like the lot servers do."""
    table = OverflowTable(TABLE, capacity=5, work_time=0.15)
    table.reset("test")
    out = {}
    barrier = threading.Barrier(n)

    def one(k):
        nid = IDS[k % 3]
        mx = nodes[nid]["mx"]
        barrier.wait()
        if not no_lock and not mx.acquire("overflow", 30):
            return
        try:
            out["d%d" % k] = table.assign("d%d" % k, "tm_" + nid)
        finally:
            if not no_lock:
                mx.release("overflow")
    ts = [threading.Thread(target=one, args=(k,)) for k in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(60)
    return out, table.summary()


def test_overflow():
    print("[the shared overflow table: the conflict, and the lock preventing it]")
    out, s = assign_all(no_lock=True)
    told = [v[0] for v in out.values() if v[0]]
    lost = [d for d, v in out.items() if v[0] and s["taken"].get(v[0]) != d]
    check(len(set(told)) < len(told) and lost,
          "WITHOUT the lock: %d drivers told %s, table keeps %s -> double booking, %d lost update(s)"
          % (len(told), sorted(set(told)), s["taken"], len(lost)))
    out, s = assign_all(no_lock=False)
    told = [v[0] for v in out.values() if v[0]]
    full = [d for d, v in out.items() if v[0] is None]
    check(len(told) == 5 and len(set(told)) == 5 and len(full) == 1,
          "WITH the lock: 5 different spaces %s, 1 driver told the garage is full" % sorted(told))
    check(all(s["taken"].get(v[0]) == d for d, v in out.items() if v[0]) and s["version"] == 5,
          "the table agrees with every reply and has 5 writes (version %d)" % s["version"])
    check(sorted(v[2] for v in out.values() if v[0]) == [1, 2, 3, 4, 5], "every write saw the previous one: versions 1..5")


def test_lamport_order():
    """Two concurrent requests: X's arrives first but Y's was made at an earlier
    Lamport time (think: sent first, delayed on the way). Y must be served first.
    X and Y are sent straight to the coordinator from addresses where nobody
    listens, so their grants also exercise "not taken -> next in line"."""
    print("[the queue follows Lamport order, not arrival order]")
    A = nodes["A"]["mx"]
    check(A.acquire("q", 5), "A holds q")
    log = MessageLog("tm_fake")
    for node, ts, port in (("X", 900, 5948), ("Y", 100, 5949)):     # X arrives first, Y second
        wire.send(MEMBERS["C"], "MUTEX_REQUEST", {"resource": "q", "node": node, "address": "127.0.0.1:%d" % port,
                                                  "req_id": "req" + node, "ts": ts}, "tm_fake", log, want_ack=True)
    q = nodes["C"]["mx"].queue.get("q", [])
    check([r["node"] for r in q] == ["Y", "X"],
          "X (ts=900) arrived first, Y (ts=100) second; the coordinator queues %s"
          % ["%s(ts=%d)" % (r["node"], r["ts"]) for r in q])
    A.release("q")
    took = wait_for(lambda: not nodes["C"]["mx"].status()["holders"].get("q") and not nodes["C"]["mx"].queue.get("q"), 10)
    grants = [l["peer"] for l in log_lines("C") if l["dir"] in ("send", "info") and l["msg"].get("type", "") == "MUTEX_GRANT"
              or (l["dir"] == "info" and l["msg"].get("note", "").startswith("MUTEX_GRANT to"))]
    tried = [p for p in grants if p in ("127.0.0.1:5948", "127.0.0.1:5949")]
    check(tried[:2] == ["127.0.0.1:5949", "127.0.0.1:5948"],
          "on A's release C grants Y first, then X: %s" % ["Y" if p.endswith("5949") else "X" for p in tried[:2]])
    check(took is not None, "neither took its grant (nobody listening), so C moved on and the lock is free again")


def test_coordinator_crash():
    print("[the coordinator crashes while A holds the lock and B waits]")
    A, B = nodes["A"]["mx"], nodes["B"]["mx"]
    check(A.acquire("k", 5), "A holds k (granted by C)")
    got = {}

    def b():
        got["ok"] = B.acquire("k", 20)
        got["t"] = time.time()
    t = threading.Thread(target=b)
    t.start()
    time.sleep(0.5)
    crash("C")
    took = wait_for(lambda: all_follow("B") and coordinating("B"), 10)
    check(took is not None, "A and B elect B, which rebuilds the lock state and coordinates (%.1fs)" % (took or -1))
    st = nodes["B"]["mx"].status()
    check(st["holders"].get("k") == "A" and st["queues"].get("k") == ["B"],
          "from MUTEX_STATE B knows A still holds k and B waits: holders %s, queue %s" % (st["holders"], st["queues"]))
    time.sleep(0.5)
    check(not B.holds("k"), "B does not get k while A still holds it, even across the coordinator change")
    released = time.time()
    A.release("k")
    t.join(10)
    check(got.get("ok") and got["t"] >= released, "A's release goes to the new coordinator B, which grants k to B")
    B.release("k")


def test_holder_crash():
    print("[a holder crashes without releasing]")
    start("C")
    took = wait_for(lambda: all_follow("C") and coordinating("C"), 10)
    check(took is not None, "C restarts, bullies back, rebuilds and coordinates again (%.1fs)" % (took or -1))
    check(nodes["A"]["mx"].acquire("h", 5), "A holds h")
    crash("A")
    t0 = time.time()
    ok = nodes["B"]["mx"].acquire("h", 15)
    waited = time.time() - t0
    check(ok and waited >= HOLD_LIMIT - 1.0, "B gets h after the %.0fs hold limit takes it back from crashed A (waited %.1fs)" % (HOLD_LIMIT, waited))
    check(nodes["C"]["mx"].stats["revoked"] >= 1, "coordinator C counted a revoked lock")
    nodes["B"]["mx"].release("h")


def test_stale_grant():
    print("[a grant from a node that is not our coordinator is refused]")
    log = MessageLog("tm_fake")
    r = wire.send(MEMBERS["B"], "MUTEX_GRANT", {"resource": "h", "req_id": "nope", "coordinator": "E"}, "tm_fake", log, want_ack=True)
    check(r and r.get("accepted") is False, "B answers accepted=false to a GRANT from E")


def test_order():
    print("[the logs in Lamport order]")
    d = tempfile.mkdtemp()
    src = os.path.dirname(MessageLog("x").path)
    for f in os.listdir(src):
        if f.startswith("tm_"):
            shutil.copy(os.path.join(src, f), d)
    out = subprocess.run([sys.executable, os.path.join(PROTO, "event_order.py"), "--dir", d, "--check"],
                         cwd=PROTO, capture_output=True, text=True)
    check(out.returncode == 0 and "HOLDS" in out.stdout,
          "every MUTEX message received after it was sent\n        " + out.stdout.strip().replace("\n", "\n        "))
    shutil.rmtree(d, ignore_errors=True)


def main():
    clear_logs()
    try:
        test_basic()
        test_exclusion()
        test_overflow()
        test_lamport_order()
        test_coordinator_crash()
        test_holder_crash()
        test_stale_grant()
    finally:
        for nid in list(nodes):
            crash(nid)
    time.sleep(0.5)
    test_order()
    shutil.rmtree(TMP, ignore_errors=True)
    print()
    if FAILS:
        print("%d FAILED:" % len(FAILS))
        for f in FAILS:
            print("  - " + f)
        sys.exit(1)
    print("ALL PASSED")


if __name__ == "__main__":
    main()
