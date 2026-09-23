#!/usr/bin/env python3
"""
End-to-end test scenarios.

    cd prototype
    python3 tests/run_tests.py          # all scenarios, about 45s
    python3 tests/run_tests.py 1 4      # just scenarios 1 and 4

Each scenario starts the processes it needs, runs client.py against them and checks
the replies / log files. Console output of every process ends up in tests/out/.
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PROTO = os.path.dirname(HERE)
OUT = os.path.join(HERE, "out")
LOGS = os.path.join(PROTO, "logs")
PY = sys.executable
os.makedirs(OUT, exist_ok=True)

procs = {}


def start(name, *args):
    f = open(os.path.join(OUT, f"{name}.out"), "a")
    p = subprocess.Popen([PY, "-u", os.path.join(PROTO, args[0]), *args[1:]], cwd=PROTO,
                         stdout=f, stderr=subprocess.STDOUT)
    procs[name] = p
    return p


def kill(name):
    p = procs.pop(name, None)
    if p:
        p.kill()
        p.wait()


def kill_all():
    for n in list(procs):
        kill(n)


def client(*args):
    r = subprocess.run([PY, os.path.join(PROTO, "client.py"), "--json", *args], cwd=PROTO,
                       capture_output=True, text=True, timeout=30)
    # last JSON object on stdout is the reply
    txt = r.stdout.strip()
    try:
        return json.loads(txt[txt.index("{"):])
    except (ValueError, json.JSONDecodeError):
        return None


def check(cond, text):
    print(("  PASS  " if cond else "  FAIL  ") + text)
    if not cond:
        FAILS.append(text)


def fresh_logs():
    if os.path.isdir(LOGS):
        for f in os.listdir(LOGS):
            os.remove(os.path.join(LOGS, f))


def first_free(lot="A"):
    for _ in range(10):
        r = client("--lot", lot, "find")
        if r and r["free"]:
            return r["free"][0]["id"]
        time.sleep(1)
    return None


FAILS = []


def scenario_1():
    print("\n[1] Basic request/response, one driver per space")
    fresh_logs()
    start("A", "lot_server.py", "--lot", "A")
    start("B", "lot_server.py", "--lot", "B")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    start("sB", "sensor_node.py", "--lot", "B", "--seed", "2")
    time.sleep(3)
    sid = first_free("A")
    check(sid is not None, f"find returned a free space ({sid})")
    r = client("--id", "alice", "reserve", sid)
    check(r and r["status"] == "OK", f"alice reserve {sid} -> {r and r['status']} {r and r['reason']}")
    r = client("--id", "bob", "reserve", sid)
    check(r and r["status"] == "DENIED" and r["reason"] == "reserved",
          f"bob reserve {sid} -> {r and r['status']} ({r and r['reason']})  [one driver per space]")
    r = client("--id", "bob", "release", sid)
    check(r and r["status"] == "DENIED", f"bob release {sid} -> {r and r['status']} (not his)")
    r = client("--id", "alice", "release", sid)
    check(r and r["status"] == "OK", f"alice release {sid} -> {r and r['status']}")
    log = os.path.join(LOGS, "lot_server_A.log")
    lines = [json.loads(l) for l in open(log)]
    check(any(l["dir"] == "recv" and l["msg"]["type"] == "reserve" for l in lines)
          and any(l["dir"] == "send" and l["msg"]["type"] == "reply" for l in lines),
          "lot_server_A.log contains the received request and the sent reply")
    kill_all()


def scenario_2():
    print("\n[2] Idempotent retry: lost reply must not create a second reservation")
    fresh_logs()
    start("A", "lot_server.py", "--lot", "A")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    time.sleep(3)
    sid = first_free("A")
    r = client("--id", "carol", "reserve", sid, "--simulate-lost-reply")
    check(r and r["status"] == "OK" and r.get("duplicate") is True,
          f"retry with same req_id -> {r and r['status']} duplicate={r and r.get('duplicate')} (cached reply)")
    lines = [json.loads(l) for l in open(os.path.join(LOGS, "lot_server_A.log"))]
    recv = [l for l in lines if l["dir"] == "recv" and l["msg"].get("type") == "reserve"]
    check(len(recv) == 2 and recv[0]["msg"]["req_id"] == recv[1]["msg"]["req_id"],
          "server log shows the request twice with the SAME req_id")
    kill_all()


def scenario_3():
    print("\n[3] Sensor node crash -> spaces become unknown after delta")
    fresh_logs()
    start("A", "lot_server.py", "--lot", "A")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    time.sleep(3)
    r = client("find")
    check(r and r["lot_summary"]["unknown"] == 0, f"before: {r and r['lot_summary']}")
    kill("sA")
    print("  ... sensor killed, waiting 8 s (delta = 6 s)")
    time.sleep(8)
    r = client("find")
    check(r and r["lot_summary"]["unknown"] == r["lot_summary"]["total"] and r["free"] == [],
          f"after : {r and r['lot_summary']} -> find returns no free spaces")
    kill_all()


def scenario_4():
    print("\n[4] Lot server crash + recovery from the append-only log")
    fresh_logs()
    start("A", "lot_server.py", "--lot", "A")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    time.sleep(3)
    sid = first_free("A")
    r = client("--id", "dave", "reserve", sid)
    check(r and r["status"] == "OK", f"dave reserve {sid} -> {r and r['status']}")
    kill("A")
    print("  ... lot server A killed; client should fail, then we restart it")
    r = client("find")
    check(r is None, "client gets no reply while server is down")
    start("A", "lot_server.py", "--lot", "A")
    time.sleep(2.5)
    r = client("--id", "erin", "reserve", sid)
    check(r and r["status"] == "DENIED" and r["reason"] == "reserved",
          f"after restart, erin reserve {sid} -> {r and r['status']} ({r and r['reason']}): reservation survived the crash")
    out = open(os.path.join(OUT, "A.out")).read()
    check("RECOVERY: replayed" in out, "server console shows RECOVERY: replayed N log entries")
    kill_all()


def scenario_5():
    print("\n[5] Peer failure: other lot summary goes stale, own lot keeps working")
    fresh_logs()
    start("A", "lot_server.py", "--lot", "A")
    start("B", "lot_server.py", "--lot", "B")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    start("sB", "sensor_node.py", "--lot", "B", "--seed", "2")
    time.sleep(5)
    r = client("find")
    ob = r and r["other_lots"].get("B")
    check(ob and ob["free"] is not None and not ob["stale"], f"A knows about B: {ob}")
    kill("B")
    print("  ... lot server B killed, waiting 11 s (peer stale after 9 s)")
    time.sleep(11)
    r = client("find")
    ob = r and r["other_lots"].get("B")
    check(r and r["status"] == "OK", "A still answers clients")
    check(ob and ob["stale"] is True, f"B's summary marked STALE: {ob}")
    kill_all()


SCEN = {1: scenario_1, 2: scenario_2, 3: scenario_3, 4: scenario_4, 5: scenario_5}

if __name__ == "__main__":
    which = [int(x) for x in sys.argv[1:]] or sorted(SCEN)
    for f in os.listdir(OUT):
        os.remove(os.path.join(OUT, f))
    try:
        for i in which:
            SCEN[i]()
    finally:
        kill_all()
    print("\n" + ("ALL PASSED" if not FAILS else f"{len(FAILS)} FAILED:\n  - " + "\n  - ".join(FAILS)))
    sys.exit(1 if FAILS else 0)
