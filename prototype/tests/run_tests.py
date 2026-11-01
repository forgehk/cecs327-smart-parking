#!/usr/bin/env python3
"""
End-to-end test scenarios (whole system, real processes).

    cd prototype
    python3 tests/run_tests.py          # all scenarios, about 2 minutes
    python3 tests/run_tests.py 1 4      # just scenarios 1 and 4

Each scenario starts the processes it needs (registry, broker, lot servers,
sensor nodes), runs client.py against them and checks the replies and the log
files. Console output of every process ends up in tests/out/.

For the communication layer on its own (framing, request-reply, pub-sub, group)
see test_framing.py and test_comm.py.
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
FAILS = []


def start(name, *args):
    f = open(os.path.join(OUT, name + ".out"), "a")
    p = subprocess.Popen([PY, "-u", os.path.join(PROTO, args[0]), *args[1:]], cwd=PROTO,
                         stdout=f, stderr=subprocess.STDOUT)
    procs[name] = p
    return p


def start_infra():
    start("registry", "registry.py")
    start("broker", "broker.py")
    time.sleep(0.5)


BOOT = {"A": None, "B": "127.0.0.1:5001", "C": "127.0.0.1:5002", "D": "127.0.0.1:5001", "E": "127.0.0.1:5003"}


def start_lot(lot, *extra):
    """Start a lot server; everyone but A joins the overlay through a bootstrap peer."""
    args = ["lot_server.py", "--lot", lot] + (["--bootstrap", BOOT[lot]] if BOOT.get(lot) else []) + list(extra)
    start(lot, *args)
    time.sleep(0.6)


def kill9(name):
    """crash: SIGKILL, same as kill() (Popen.kill is SIGKILL on POSIX); named for clarity"""
    kill(name)


def term(name):
    """graceful stop: SIGTERM, the lot server leaves the overlay before exiting"""
    p = procs.pop(name, None)
    if p:
        p.terminate()
        p.wait()


def kill(name):
    p = procs.pop(name, None)
    if p:
        p.kill()
        p.wait()


def kill_all():
    for n in list(procs):
        kill(n)


def client(*args, timeout=30):
    r = subprocess.run([PY, os.path.join(PROTO, "client.py"), "--json", *args], cwd=PROTO,
                       capture_output=True, text=True, timeout=timeout)
    txt = r.stdout.strip()
    try:
        reply = json.loads(txt[txt.index("{"):])
    except (ValueError, json.JSONDecodeError):
        return None
    return reply


def result(reply):
    return reply["result"] if reply and reply.get("status") == "OK" else None


def check(cond, text):
    print(("  PASS  " if cond else "  FAIL  ") + text)
    if not cond:
        FAILS.append(text)


def fresh_logs():
    if os.path.isdir(LOGS):
        for f in os.listdir(LOGS):
            os.remove(os.path.join(LOGS, f))


def log_lines(name):
    path = os.path.join(LOGS, name + ".log")
    return [json.loads(l) for l in open(path)] if os.path.exists(path) else []


def console(name):
    path = os.path.join(OUT, name + ".out")
    return open(path).read() if os.path.exists(path) else ""


def first_free(lot="A"):
    for _ in range(10):
        r = result(client("--lot", lot, "find"))
        if r and r["free"]:
            return r["free"][0]["id"]
        time.sleep(1)
    return None


# -------------------------------------------------------------------- scenarios

def scenario_1():
    print("\n[1] Request-reply: find / reserve / release, one driver per space")
    fresh_logs()
    start_infra()
    start_lot("A")
    start_lot("B")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    start("sB", "sensor_node.py", "--lot", "B", "--seed", "2")
    time.sleep(4)
    sid = first_free("A")
    check(sid is not None, "find returned a free space (%s)" % sid)
    r = result(client("--id", "alice", "reserve", sid))
    check(r and r["granted"], "alice reserve %s -> OK" % sid)
    r = result(client("--id", "bob", "reserve", sid))
    check(r and not r["granted"] and r["reason"] == "reserved", "bob reserve %s -> DENIED reserved" % sid)
    r = result(client("--id", "bob", "release", sid))
    check(r and not r["granted"], "bob release %s -> DENIED (not his)" % sid)
    r = result(client("--id", "alice", "release", sid))
    check(r and r["granted"], "alice release %s -> OK" % sid)

    reply = client("--id", "bob", "reserve", "Z-99")
    check(reply and reply["status"] == "ERROR" and "no such space" in reply["error"],
          "bad argument -> status ERROR: %s" % (reply or {}).get("error"))

    lines = log_lines("lot_server_A")
    reqs = [l for l in lines if l["dir"] == "recv" and l["msg"].get("type") == "REQUEST"]
    reps = [l for l in lines if l["dir"] == "send" and l["msg"].get("type") == "REPLY"]
    check(reqs and reps and all("reply_to" in r["msg"] for r in reps),
          "server log has REQUEST/REPLY pairs with reply_to (%d/%d)" % (len(reqs), len(reps)))
    kill_all()


def scenario_2():
    print("\n[2] Lost reply -> retry with same msg_id -> cached reply, no second reservation")
    fresh_logs()
    start_infra()
    start_lot("A")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    time.sleep(4)
    sid = first_free("A")
    reply = client("--id", "carol", "reserve", sid, "--simulate-lost-reply")
    check(reply and reply["status"] == "OK" and reply["result"]["granted"] and reply.get("duplicate") is True,
          "retry got the cached reply (duplicate=True) and the reservation exists once")
    lines = log_lines("lot_server_A")
    recv = [l for l in lines if l["dir"] == "recv" and l["msg"].get("type") == "REQUEST"
            and l["msg"]["payload"].get("op") == "reserve"]
    check(len(recv) == 2 and recv[0]["msg"]["msg_id"] == recv[1]["msg"]["msg_id"],
          "server log shows the REQUEST twice with the same msg_id")
    kill_all()


def scenario_3():
    print("\n[3] Sensor node crash -> spaces become unknown after delta")
    fresh_logs()
    start_infra()
    start_lot("A")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    time.sleep(4)
    r = result(client("find"))
    check(r and r["lot_summary"]["unknown"] == 0, "before: %s" % (r and r["lot_summary"]))
    kill("sA")
    print("  ... sensor killed, waiting 8 s (delta = 6 s)")
    time.sleep(8)
    r = result(client("find"))
    check(r and r["lot_summary"]["unknown"] == r["lot_summary"]["total"] and r["free"] == [],
          "after : %s -> find returns no free spaces" % (r and r["lot_summary"]))
    kill_all()


def scenario_4():
    print("\n[4] Lot server crash + recovery from the log")
    fresh_logs()
    start_infra()
    start_lot("A")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    time.sleep(4)
    sid = first_free("A")
    r = result(client("--id", "dave", "reserve", sid))
    check(r and r["granted"], "dave reserve %s -> OK" % sid)
    kill("A")
    print("  ... lot server A killed; client should fail, then we restart it")
    reply = client("find")
    check(reply is None, "client gets no reply while server is down")
    start_lot("A")
    time.sleep(3)
    r = result(client("--id", "erin", "reserve", sid))
    check(r and not r["granted"] and r["reason"] == "reserved",
          "after restart, erin reserve %s -> DENIED reserved: reservation survived" % sid)
    check("RECOVERY: replayed" in console("A"), "server console shows RECOVERY: replayed N entries")
    kill_all()


def scenario_5():
    print("\n[5] Overlay: summaries travel by gossip; a peer crashes -> dead pointer repaired, its replica goes stale")
    fresh_logs()
    start_infra()
    start_lot("A")
    start_lot("B")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    start("sB", "sensor_node.py", "--lot", "B", "--seed", "2")
    time.sleep(6)
    r = result(client("find"))
    ob = r and r["other_lots"].get("B")
    check(ob and ob["free"] is not None and not ob["stale"], "A holds B's summary as a replica (gossip): %s" % ob)
    gm = [l for l in log_lines("lot_server_B") if l["dir"] == "send" and l["msg"].get("type") == "P2P_GOSSIP"]
    check(len(gm) >= 1 and gm[0]["peer"] == "127.0.0.1:5001", "B's log has P2P_GOSSIP send lines to its neighbor A (%d)" % len(gm))
    jn = [l for l in log_lines("lot_server_A") if l["dir"] == "recv" and l["msg"].get("type") == "P2P_JOIN"]
    check(len(jn) == 1, "A's log shows B's P2P_JOIN")
    kill9("B")
    print("  ... lot server B crashed (kill -9), waiting 13 s (ping every 5 s, replica stale after 12 s)")
    time.sleep(13)
    r = result(client("find"))
    ob = r and r["other_lots"].get("B")
    check(r is not None, "A still answers clients")
    check(ob and ob["stale"] is True, "B's replica is flagged STALE: %s" % ob)
    check("dead pointer removed" in console("A"), "A's console shows the dead pointer to B removed after a failed ping")
    pr = result(client("peers"))
    check(pr and pr["degree"] == 0 and pr["stats"]["repairs"] >= 1, "A's neighbor list is empty, repairs=%s" % (pr and pr["stats"]["repairs"]))
    kill_all()


def scenario_6():
    print("\n[6] Pub-sub: broker crash and restart -> everyone reconnects, lot recovers")
    fresh_logs()
    start_infra()
    start_lot("A")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    time.sleep(4)
    r = result(client("find"))
    check(r and r["lot_summary"]["unknown"] == 0, "before: %s" % (r and r["lot_summary"]))
    kill("broker")
    print("  ... broker killed, waiting 8 s: sensor can't publish, server hears nothing")
    time.sleep(8)
    r = result(client("find"))
    check(r and r["lot_summary"]["unknown"] == r["lot_summary"]["total"],
          "broker down: lot A all unknown (%s), request-reply still works" % (r and r["lot_summary"]))
    start("broker", "broker.py")
    print("  ... broker restarted, waiting 5 s")
    time.sleep(5)
    r = result(client("find"))
    check(r and r["lot_summary"]["unknown"] == 0, "after restart: subscriber re-subscribed, sensor republishes -> %s" % (r and r["lot_summary"]))
    resub = [l for l in log_lines("broker") if l["dir"] == "recv" and l["msg"].get("type") == "SUBSCRIBE"]
    check(len(resub) >= 1, "new broker log shows the lot server's re-SUBSCRIBE")
    kill_all()


def scenario_7():
    print("\n[7] Pub-sub: a watching client receives status EVENTs published by the lot server")
    fresh_logs()
    start_infra()
    start_lot("A")
    start("sA", "sensor_node.py", "--lot", "A", "--seed", "1")
    time.sleep(4)
    watcher = subprocess.Popen([PY, "-u", os.path.join(PROTO, "client.py"), "--id", "watcher", "watch", "A"],
                               cwd=PROTO, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    time.sleep(1.5)
    sid = first_free("A")
    client("--id", "fay", "reserve", sid)
    time.sleep(2)
    client("--id", "fay", "release", sid)
    time.sleep(2)
    watcher.kill()
    out = watcher.stdout.read()
    got_res = ("%s -> reserved by fay" % sid) in out
    got_rel = ("%s -> " % sid) in out and out.count("EVENT") >= 2
    check(got_res, "watcher printed EVENT %s -> reserved by fay" % sid)
    check(got_rel, "watcher printed the release EVENT too")
    ev = [l for l in log_lines("broker") if l["dir"] == "send" and l["msg"].get("type") == "EVENT"
          and l["msg"]["payload"].get("topic") == "lot/A/status"]
    check(len(ev) >= 2, "broker log has one send line per status EVENT delivered (%d)" % len(ev))
    kill_all()


def scenario_8():
    print("\n[8] Overlay join/leave trace across five lots (Milestone 3)")
    fresh_logs()
    start_infra()
    for lot in "ABCD":
        start_lot(lot)
    for lot, seed in (("A", 1), ("B", 2), ("C", 3), ("D", 4)):
        start("s" + lot, "sensor_node.py", "--lot", lot, "--seed", str(seed))
    time.sleep(5)
    views = {lot: result(client("--lot", lot, "peers")) for lot in "ABCD"}
    degs = {lot: v["degree"] for lot, v in views.items() if v}
    check(len(degs) == 4 and all(d >= 2 for d in degs.values()), "four lots up, every one has at least 2 neighbors: %s" % degs)
    check(all(len(v["replicas"]) == 3 for v in views.values() if v), "every lot hosts replicas of the other three")
    r = result(client("--lot", "D", "lookup", "B", "--no-cache", "--fresh"))
    check(r and r["found"] and r["hops"] >= 1 and r["path"][-1] == "B",
          "D's owner-only lookup for B floods the overlay: %s hop(s) via %s" % (r and r["hops"], r and "->".join(r["path"])))
    r = result(client("total"))
    check(r and sorted(r["lots"]) == ["A", "B", "C", "D"] and r["total"] == 80, "convergecast from A covers all four lots: %d free of %d" % (r["free"], r["total"]))

    start_lot("E")
    time.sleep(2)
    e = console("E")
    check("P2P JOIN via bootstrap 127.0.0.1:5003" in e and "took" in e and "P2P joined: neighbors" in e,
          "E joins through C, receives C's table and at least one more neighbor pointer")
    pe = result(client("--lot", "E", "peers"))
    check(pe and pe["degree"] >= 2 and len(pe["replicas"]) >= 4, "E has %s neighbors and %s replicas right after joining" % (pe and pe["degree"], pe and len(pe["replicas"])))

    term("C")                                  # SIGTERM -> graceful leave
    time.sleep(1.5)
    left = [lot for lot in "ABDE" if "P2P neighbor - C" in console(lot) and "left gracefully" in console(lot)]
    check(len(left) >= 2, "C leaves gracefully: its neighbors %s dropped the pointer at once" % left)

    kill9("D")                                 # crash
    print("  ... D crashed (kill -9), waiting 7 s for the next ping round")
    time.sleep(7)
    repaired = [lot for lot in "ABE" if "P2P neighbor - D" in console(lot) and "dead pointer removed" in console(lot)]
    check(len(repaired) >= 1, "D's crash is detected by ping: %s removed the dead pointer" % repaired)
    views = {lot: result(client("--lot", lot, "peers")) for lot in "ABE"}
    adj = {lot: [n["id"] for n in v["neighbors"]] for lot, v in views.items() if v}
    seen, stack = {"A"}, ["A"]
    while stack:
        u = stack.pop()
        for v in adj.get(u, []):
            if v in adj and v not in seen:
                seen.add(v)
                stack.append(v)
    check(seen == {"A", "B", "E"}, "A, B and E are still one connected mesh after a leave and a crash: %s" % adj)
    r = result(client("total"))
    check(r and sorted(r["lots"]) == ["A", "B", "E"], "convergecast now covers exactly the live lots: %s" % (r and sorted(r["lots"])))
    kill_all()


SCEN = {1: scenario_1, 2: scenario_2, 3: scenario_3, 4: scenario_4, 5: scenario_5, 6: scenario_6, 7: scenario_7, 8: scenario_8}

if __name__ == "__main__":
    which = [int(x) for x in sys.argv[1:]] or sorted(SCEN)
    for f in os.listdir(OUT):
        os.remove(os.path.join(OUT, f))
    try:
        for i in which:
            SCEN[i]()
    finally:
        kill_all()
    print("\n" + ("ALL PASSED" if not FAILS else "%d FAILED:\n  - " % len(FAILS) + "\n  - ".join(FAILS)))
    sys.exit(1 if FAILS else 0)
