#!/usr/bin/env python3
"""
Tests for the Bully election (Milestone 4). Four nodes A, B, C, D run in threads
inside this process, each with its own Service on a real TCP port, with short
timers so a test takes seconds.

    cd prototype && python tests/test_election.py
"""

import json
import os
import subprocess
import sys
import threading
import time

PROTO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROTO)

from comm import request_reply
from common import MessageLog
from coord.election import Election

FAILS = []
BASE = 5930          # test ports: A 5930, B 5931, C 5932, D 5933
IDS = "ABCD"
MEMBERS = {i: "127.0.0.1:%d" % (BASE + k) for k, i in enumerate(IDS)}
FAST = dict(answer_timeout=0.5, coordinator_timeout=1.5, check_interval=0.5, ping_timeout=0.5)

nodes = {}           # id -> (service, election)
changes = []         # (node, old, new) in the order they happened


def check(cond, text):
    print(("  PASS  " if cond else "  FAIL  ") + text)
    if not cond:
        FAILS.append(text)


def clear_logs():
    d = os.path.dirname(MessageLog("x").path)
    for f in os.listdir(d):
        if f.startswith("te_"):
            os.remove(os.path.join(d, f))


def log_lines(nid):
    path = MessageLog("te_" + nid).path
    return [json.loads(l) for l in open(path, encoding="utf-8")] if os.path.exists(path) else []


def start(nid):
    log = MessageLog("te_" + nid)
    svc = request_reply.Service("te_" + nid, BASE + IDS.index(nid), log)
    el = Election(nid, MEMBERS[nid], MEMBERS, svc, log, info=lambda t, n=nid: None,
                  on_change=lambda old, new, n=nid: changes.append((n, old, new)), **FAST)
    threading.Thread(target=svc.serve_forever, daemon=True).start()
    nodes[nid] = (svc, el)
    el.start()


def crash(nid):
    svc, el = nodes.pop(nid)
    el.stop()
    svc.stop()


def leaders():
    """id -> who that node believes is the leader, for every running node."""
    return {i: el.current()[0] for i, (svc, el) in nodes.items()}


def wait_agree(expected, timeout=10.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        view = leaders()
        if all(v == expected for v in view.values()):
            return time.time() - t0, view
        time.sleep(0.05)
    return None, leaders()


def test_initial():
    print("[startup: the highest live id wins]")
    for nid in IDS:
        start(nid)
    took, view = wait_agree("D")
    check(took is not None, "all four agree D is the leader (%s) after %.1fs" % (view, took or -1))
    check(sum(1 for i, (s, el) in nodes.items() if el.is_leader()) == 1, "exactly one node thinks it is the leader")
    d_sends = [l for l in log_lines("D") if l["dir"] == "send" and l["msg"]["type"] == "COORDINATOR"]
    check({l["peer"] for l in d_sends} >= {MEMBERS[i] for i in "ABC"}, "D sent COORDINATOR to A, B and C")
    # attempts = sends plus sends that failed because that node wasn't listening yet
    a_el = [l for l in log_lines("A") if (l["dir"] == "send" and l["msg"]["type"] == "ELECTION")
            or (l["dir"] == "info" and l["msg"].get("note", "").startswith("ELECTION to"))]
    check({l["peer"] for l in a_el} >= {MEMBERS[i] for i in "BCD"}, "A's ELECTION went to every higher node (B, C, D)")
    downward = [(nid, l["peer"]) for nid in IDS for l in log_lines(nid)
                if l["dir"] == "send" and l["msg"]["type"] == "ELECTION" and l["peer"] in [MEMBERS[i] for i in IDS if i < nid]]
    check(not downward, "no node ever sent ELECTION to a lower node")
    check(not [l for l in log_lines("D") if l["dir"] == "send" and l["msg"]["type"] == "ELECTION"],
          "D never sent ELECTION: there is nobody higher than D")


def test_leader_crash():
    print("[the leader crashes]")
    crash("D")
    t0 = time.time()
    took, view = wait_agree("C")
    check(took is not None, "A, B, C agree on C, the highest live id (%s) %.1fs after D crashed" % (view, took or -1))
    failed = [l for l in log_lines("A") + log_lines("B") + log_lines("C")
              if l["dir"] == "info" and "LEADER_PING" in l["msg"].get("note", "")]
    check(failed, "a follower's LEADER_PING to D failed and is logged (%d failed ping(s))" % len(failed))
    c_coord = [l for l in log_lines("C") if l["dir"] == "send" and l["msg"]["type"] == "COORDINATOR"]
    check(c_coord, "C announced itself with COORDINATOR")


def test_bully_back():
    print("[the old leader comes back and bullies its way in]")
    start("D")
    took, view = wait_agree("D")
    check(took is not None, "D restarts with the highest id and everyone switches back to D (%s) after %.1fs" % (view, took or -1))
    check([c for c in changes if c[0] == "C" and c[1] == "C" and c[2] == "D"], "C stepped down from leader to follower when D announced")


def test_two_crash():
    print("[two nodes crash at once: leader D and next in line C]")
    crash("D")
    crash("C")
    took, view = wait_agree("B")
    check(took is not None, "A and B agree on B (%s) after %.1fs: C answered nobody, so B won" % (view, took or -1))


def test_alone():
    print("[only the lowest node is left]")
    crash("B")
    took, view = wait_agree("A")
    check(took is not None, "A alone elects itself (%s) after %.1fs" % (view, took or -1))


def test_concurrent():
    print("[everyone starts an election at the same time]")
    for nid in "BCD":
        start(nid)
    wait_agree("D")
    for nid, (svc, el) in nodes.items():
        el.trigger("test: simultaneous election")
    took, view = wait_agree("D")
    time.sleep(1.0)
    view = leaders()
    check(all(v == "D" for v in view.values()) and sum(1 for i, (s, el) in nodes.items() if el.is_leader()) == 1,
          "four simultaneous elections still end with one leader, D (%s)" % view)


def test_order():
    print("[the logs in Lamport order]")
    import shutil
    import tempfile
    d = tempfile.mkdtemp()
    src = os.path.dirname(MessageLog("x").path)
    for f in os.listdir(src):
        if f.startswith("te_"):
            shutil.copy(os.path.join(src, f), d)
    out = subprocess.run([sys.executable, os.path.join(PROTO, "event_order.py"), "--dir", d, "--check"],
                         cwd=PROTO, capture_output=True, text=True)
    check(out.returncode == 0 and "HOLDS" in out.stdout,
          "every ELECTION/COORDINATOR/ACK received after it was sent\n        " + out.stdout.strip().replace("\n", "\n        "))
    # the first election in Lamport order: A's ELECTION to D comes before D's COORDINATOR reaches A
    out = subprocess.run([sys.executable, os.path.join(PROTO, "event_order.py"), "--dir", d, "--no-info",
                          "--types", "ELECTION,COORDINATOR"], cwd=PROTO, capture_output=True, text=True)
    lines = out.stdout.splitlines()
    first_coord_recv = next((k for k, l in enumerate(lines) if " recv COORDINATOR" in l), None)
    first_el = next((k for k, l in enumerate(lines) if " send ELECTION" in l), None)
    check(first_el is not None and first_coord_recv is not None and first_el < first_coord_recv,
          "in the merged order an ELECTION is sent before any COORDINATOR is received")
    shutil.rmtree(d, ignore_errors=True)


def main():
    clear_logs()
    test_initial()
    test_leader_crash()
    test_bully_back()
    test_two_crash()
    test_alone()
    test_concurrent()
    for nid in list(nodes):
        crash(nid)
    time.sleep(0.5)
    test_order()
    print()
    if FAILS:
        print("%d FAILED:" % len(FAILS))
        for f in FAILS:
            print("  - " + f)
        sys.exit(1)
    print("ALL PASSED")


if __name__ == "__main__":
    main()
