#!/usr/bin/env python3
"""
Tests for the P2P overlay middleware (p2p/overlay.py). Five peers run in
threads inside this process on ports 5950+ and talk over real TCP sockets.

    cd prototype && python3 tests/test_p2p.py        # about 15 s
"""

import os
import random
import sys
import threading
import time
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from comm import request_reply
from common import MessageLog
from p2p.overlay import Peer

FAILS = []
BASE = 5950
peers = {}      # id -> (Peer, Service)


def check(cond, text):
    print(("  PASS  " if cond else "  FAIL  ") + text)
    if not cond:
        FAILS.append(text)


def clear_logs():
    d = os.path.dirname(MessageLog("x").path)
    for f in os.listdir(d):
        if f.startswith("t_p2p_"):
            os.remove(os.path.join(d, f))


def make(pid, port, **kw):
    log = MessageLog("t_p2p_" + pid)
    svc = request_reply.Service("t_p2p_" + pid, port, log)
    peer = Peer(pid, "127.0.0.1:%d" % port, svc, log, info=lambda t: None,
                gossip_interval=1.0, maint_interval=1.0, rng=random.Random(ord(pid)), **kw)
    threading.Thread(target=svc.serve_forever, daemon=True).start()
    peers[pid] = (peer, svc)
    return peer


def addr(pid):
    return peers[pid][0].address


def edges():
    out = set()
    for pid, (p, _) in peers.items():
        for n in p.neighbor_list():
            out.add(tuple(sorted((pid, n["id"]))))
    return sorted(out)


def connected(ids):
    adj = {pid: [n["id"] for n in peers[pid][0].neighbor_list() if n["id"] in ids] for pid in ids}
    seen = {ids[0]}
    q = deque([ids[0]])
    while q:
        u = q.popleft()
        for v in adj[u]:
            if v not in seen:
                seen.add(v)
                q.append(v)
    return seen == set(ids)


def wait_for(pred, timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        time.sleep(0.1)
    return pred()


# ------------------------------------------------------------------ tests

def test_join():
    print("[join: four peers through bootstrap peers]")
    A = make("A", BASE)
    time.sleep(0.2)
    B = make("B", BASE + 1)
    got = B.join(addr("A"))
    check(got == [addr("A")] and A.degree() == 1 and B.degree() == 1, "B joins through A: both list each other")
    C = make("C", BASE + 2)
    got = C.join(addr("B"))
    check(addr("A") in got and addr("B") in got, "C joins through B and gets A as a neighbor pointer from B's list (%s)" % [n["id"] for n in C.neighbor_list()])
    D = make("D", BASE + 3)
    D.join(addr("A"))
    check(connected(["A", "B", "C", "D"]), "four peers form one connected mesh: edges %s" % edges())
    degs = {pid: peers[pid][0].degree() for pid in "ABCD"}
    check(max(degs.values()) >= 3 and min(degs.values()) == 2, "degree varies per node (not a ring, not a full mesh): %s" % degs)
    check(all(d <= 3 for d in degs.values()), "no node has more than max_neighbors+1 links")
    for p, _ in peers.values():
        p.start()


def test_gossip():
    print("[gossip: put spreads to every peer, newer versions win]")
    A = peers["A"][0]
    v1 = A.put("lot/A/summary", {"free": 10, "total": 20})
    ok = wait_for(lambda: all(peers[p][0].get("lot/A/summary") is not None for p in "BCD"), 3.0)
    check(ok, "A.put reaches B, C and D within one gossip interval")
    rep = peers["D"][0].get("lot/A/summary")
    check(rep and rep["origin"] == "A" and rep["version"] == v1 and rep["value"]["free"] == 10, "D holds A's value as a replica with A's version")
    check(peers["D"][0].get("lot/A/summary")["stale"] is False, "a fresh replica is not stale")

    time.sleep(0.01)
    v2 = A.put("lot/A/summary", {"free": 7, "total": 20})
    ok = wait_for(lambda: all(peers[p][0].get("lot/A/summary")["version"] == v2 for p in "BCD"), 3.0)
    check(ok and v2 > v1, "a second put (newer version) replaces the replica everywhere")

    # an old rumor arriving late must be ignored
    D = peers["D"][0]
    before = D.get("lot/A/summary")["version"]
    D._apply("lot/A/summary", {"value": {"free": 99, "total": 20}, "version": v1, "origin": "A"})
    check(D.get("lot/A/summary")["version"] == before and D.get("lot/A/summary")["value"]["free"] == 7,
          "a stale version (older than what we hold) is ignored")

    peers["B"][0].put("lot/B/summary", {"free": 3, "total": 20})
    peers["C"][0].put("lot/C/summary", {"free": 5, "total": 20})
    peers["D"][0].put("lot/D/summary", {"free": 1, "total": 20})
    ok = wait_for(lambda: all(len(peers[p][0].replicas()) == 3 for p in "ABCD"), 4.0)
    check(ok, "after every lot puts, every peer hosts 3 replicas (the other lots)")


def test_lookup():
    print("[lookup: TTL flood with duplicate suppression]")
    C = peers["C"][0]
    D = peers["D"][0]
    C._apply("only/C", {"value": "x", "version": 1, "origin": "C"})       # not gossiped on purpose
    r = D.lookup("only/C", ttl=3, timeout=2.0, fresh=True)
    check(r["found"] and r["origin"] == "C" and r["hops"] >= 1 and r["path"][0] == "D" and r["path"][-1] == "C",
          "D finds a key it does not have: %d hop(s) via %s, %d message(s) sent" % (r["hops"], "->".join(r["path"]), r["messages"]))
    r2 = D.lookup("only/C", ttl=3)
    check(r2["found"] and r2["hops"] == 0 and r2.get("from_cache"), "second lookup is answered from the local cache (0 hops)")

    r = D.lookup("lot/A/summary", ttl=3, use_cache=False, fresh=True)
    check(r["found"] and r["holder"] == "A", "owner-only lookup is answered by the origin, not a replica (holder %s)" % r.get("holder"))

    dup_before = sum(peers[p][0].stats["lookup_dup"] for p in "ABCD")
    peers["A"][0]._apply("only/A", {"value": "y", "version": 1, "origin": "A"})
    r = peers["B"][0].lookup("only/A", ttl=3, use_cache=False)
    time.sleep(0.5)
    dup_after = sum(peers[p][0].stats["lookup_dup"] for p in "ABCD")
    check(r["found"] and dup_after > dup_before, "a flood produces duplicate copies and they are dropped (%d dropped)" % (dup_after - dup_before))

    r = D.lookup("nowhere", ttl=2, timeout=1.0)
    check(r["found"] is False and r["hops"] is None, "a key nobody has -> not found after the timeout")

    nbrs = [n["id"] for n in D.neighbor_list()]
    far = [p for p in "ABC" if p not in nbrs]
    ttl0_before = sum(peers[p][0].stats["lookup_ttl0"] for p in nbrs)
    recv_before = sum(peers[p][0].stats["lookup_recv"] for p in far)
    D.lookup("nowhere2", ttl=1, timeout=0.5)
    time.sleep(0.3)
    ttl0_after = sum(peers[p][0].stats["lookup_ttl0"] for p in nbrs)
    recv_after = sum(peers[p][0].stats["lookup_recv"] for p in far)
    check(ttl0_after - ttl0_before == len(nbrs) and recv_after == recv_before,
          "TTL 1: D's neighbors %s let it expire, non-neighbors %s never see it" % (nbrs, far))


def test_aggregate():
    print("[convergecast: free/total over the overlay]")
    r = peers["A"][0].aggregate(ttl=3, timeout=2.0)
    check(sorted(r["lots"]) == ["A", "B", "C", "D"] and r["total"] == 80 and r["free"] == 7 + 3 + 5 + 1,
          "A's convergecast sums every lot once: lots %s, %d free of %d, %d message(s) up" % (sorted(r["lots"]), r["free"], r["total"], r["messages"]))
    check(r["messages"] == 3, "exactly N-1 = 3 replies travel up the tree")


def test_leave_and_crash():
    print("[leave, crash, repair, random sampling]")
    B = peers["B"][0]
    nb = [n["id"] for n in B.neighbor_list()]
    B.leave()
    time.sleep(0.3)
    gone = all("B" not in [n["id"] for n in peers[p][0].neighbor_list()] for p in nb)
    check(gone, "graceful leave: B's neighbors %s dropped the pointer immediately" % nb)
    B.stop()
    peers["B"][1].stop()

    C, Csvc = peers["C"]
    cn = [n["id"] for n in C.neighbor_list() if n["id"] != "B"]
    Csvc.stop()
    C.stop()
    ok = wait_for(lambda: all("C" not in [n["id"] for n in peers[p][0].neighbor_list()] for p in cn), 4.0)
    check(ok, "crash: C's neighbors %s removed the dead pointer after a failed ping" % cn)
    repairs = sum(peers[p][0].stats["repairs"] for p in cn)
    check(repairs >= len(cn), "repair counter went up once per dropped pointer (%d)" % repairs)

    E = make("E", BASE + 4)
    E.join(addr("A"))
    E.start()
    check(len(E.replicas()) >= 3, "E joins through A and receives A's table: %d replica(s) %s" % (len(E.replicas()), sorted(E.replicas())))
    check(connected(["A", "D", "E"]), "A, D, E are one connected mesh after two departures and one join: %s" % edges())
    ok = wait_for(lambda: all(peers[p][0].degree() >= 2 for p in "ADE"), 4.0)
    check(ok, "random sampling refilled short neighbor lists: degrees %s" % {p: peers[p][0].degree() for p in "ADE"})

    time.sleep(1.5)
    rep = peers["A"][0].get("lot/C/summary")
    check(rep is not None and rep["age"] > 1.0, "A still holds C's last summary as a replica, aging (%.1fs old)" % (rep["age"] if rep else -1))


if __name__ == "__main__":
    clear_logs()
    test_join()
    test_gossip()
    test_lookup()
    test_aggregate()
    test_leave_and_crash()
    print("\n" + ("ALL PASSED" if not FAILS else "%d FAILED:\n  - " % len(FAILS) + "\n  - ".join(FAILS)))
    os._exit(1 if FAILS else 0)
