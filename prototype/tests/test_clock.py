#!/usr/bin/env python3
"""
Tests for Lamport clocks (Milestone 4): the two update rules, the stamp on every
message, the receive rule applied by the log, and the clock condition across
real TCP exchanges between three processes (simulated as threads here).

    cd prototype && python tests/test_clock.py
"""

import json
import os
import subprocess
import sys
import threading
import time

PROTO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROTO)

from comm import envelope, request_reply
from comm.clock import LamportClock, clock_for
from common import MessageLog

FAILS = []
BASE = 5920          # test ports: 5920 service Y, 5921 service Z


def check(cond, text):
    print(("  PASS  " if cond else "  FAIL  ") + text)
    if not cond:
        FAILS.append(text)


def log_lines(name):
    path = MessageLog(name).path
    return [json.loads(l) for l in open(path, encoding="utf-8")] if os.path.exists(path) else []


def clear_logs():
    d = os.path.dirname(MessageLog("x").path)
    for f in os.listdir(d):
        if f.startswith("tc_"):
            os.remove(os.path.join(d, f))


# ------------------------------------------------------------ the rules

def test_rules():
    print("[clock rules]")
    c = LamportClock()
    check(c.now() == 0, "a new clock starts at 0")
    check(c.tick() == 1 and c.tick() == 2, "local event: C = C + 1 (1, 2)")
    check(c.send() == 3, "send is an event too: the message carries 3")
    check(c.receive(10) == 11, "receive a message stamped 10 while at 3: C = max(3, 10) + 1 = 11")
    check(c.receive(4) == 12, "receive a message stamped 4 while at 11: C = max(11, 4) + 1 = 12")
    check(c.now() == 12 and c.now() == 12, "reading the clock is not an event")
    check(c.receive(None) == 13, "a message without a stamp still counts as an event")
    check(c.restore(50) == 50 and c.restore(20) == 50, "restore after a crash only moves forward (50, not back to 20)")

    # many threads sending at once still get unique, increasing stamps
    c = LamportClock()
    got = []
    lock = threading.Lock()

    def worker():
        for _ in range(200):
            v = c.send()
            with lock:
                got.append(v)
    ts = [threading.Thread(target=worker) for _ in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    check(sorted(got) == list(range(1, 1601)), "8 threads x 200 sends: every stamp 1..1600 used exactly once")


def test_envelope():
    print("[envelope]")
    a = envelope.make("REQUEST", "tc_env_a", {"op": "x"})
    b = envelope.make("REQUEST", "tc_env_a", {"op": "x"})
    other = envelope.make("REQUEST", "tc_env_b", {"op": "x"})
    check(isinstance(a["lamport"], int) and b["lamport"] == a["lamport"] + 1,
          "every message carries the sender's clock; two sends -> %d, %d" % (a["lamport"], b["lamport"]))
    check(other["lamport"] == 1, "a different process has its own clock (starts at 1, not %d)" % (b["lamport"] + 1))
    before, mid = b["lamport"], b["msg_id"]
    envelope.stamp(b)
    check(b["lamport"] > before and b["msg_id"] == mid,
          "a re-sent message keeps its msg_id and gets a later stamp (%d -> %d)" % (before, b["lamport"]))
    check(envelope.validate(a) is None, "stamped envelope validates")
    old = dict(a)
    del old["lamport"]
    check(envelope.validate(old) is None, "an envelope without lamport (before M4) still validates")
    check(envelope.validate(dict(a, lamport=-1)) is not None and envelope.validate(dict(a, lamport="7")) is not None,
          "a negative or non-integer lamport is rejected")


# ------------------------------------------------------------ over the network

def test_request_reply():
    print("[request-reply over TCP: X -> Y]")
    y = request_reply.Service("tc_Y", BASE, MessageLog("tc_Y"))
    y.register("echo", lambda args, req: {"echo": args})
    threading.Thread(target=y.serve_forever, daemon=True).start()
    time.sleep(0.2)

    xlog = MessageLog("tc_X")
    reply = request_reply.call("127.0.0.1", BASE, "echo", {"n": 1}, "tc_X", xlog, full=True)
    xl = log_lines("tc_X")
    yl = log_lines("tc_Y")
    req = [l for l in xl if l["dir"] == "send"][-1]
    yrecv = [l for l in yl if l["dir"] == "recv"][-1]
    ysend = [l for l in yl if l["dir"] == "send"][-1]
    xrecv = [l for l in xl if l["dir"] == "recv"][-1]
    check(req["lamport"] == req["msg"]["lamport"], "X logs the send with the stamp the REQUEST carried (%d)" % req["lamport"])
    check(yrecv["lamport"] > req["msg"]["lamport"], "Y receives it later than it was sent (%d > %d)" % (yrecv["lamport"], req["msg"]["lamport"]))
    check(reply["lamport"] > yrecv["lamport"], "Y's REPLY is stamped after the receive (%d > %d)" % (reply["lamport"], yrecv["lamport"]))
    check(xrecv["lamport"] > reply["lamport"], "X receives the REPLY later than Y sent it (%d > %d)" % (xrecv["lamport"], reply["lamport"]))
    check(ysend["msg"]["reply_to"] == req["msg"]["msg_id"], "the REPLY is matched to the REQUEST by msg_id")

    # a lost reply and a retry: the retry is a new send event with a new stamp
    request_reply.call("127.0.0.1", BASE, "echo", {"n": 2}, "tc_X", xlog, drop_first_reply=True)
    sends = [l for l in log_lines("tc_X") if l["dir"] == "send"][-2:]
    check(sends[0]["msg"]["msg_id"] == sends[1]["msg"]["msg_id"] and sends[1]["lamport"] > sends[0]["lamport"],
          "retry keeps the msg_id but gets a later stamp (%d -> %d)" % (sends[0]["lamport"], sends[1]["lamport"]))
    dup = [l for l in log_lines("tc_Y") if l["dir"] == "send"][-1]
    rec = [l for l in log_lines("tc_Y") if l["dir"] == "recv"][-1]
    check(dup["msg"]["payload"].get("duplicate") and dup["lamport"] > rec["lamport"],
          "the cached reply to the retry is re-stamped after the retry was received (%d > %d)" % (dup["lamport"], rec["lamport"]))
    y.stop()


def test_causal_chain():
    """X -> Y, then Y -> Z. Z's clock starts far ahead and X's far behind; the
    receive rule still puts every effect after its cause."""
    print("[causal chain X -> Y -> Z]")
    z = request_reply.Service("tc_Z", BASE + 1, MessageLog("tc_Z"))
    z.register("note", lambda args, req: {"ok": True})
    threading.Thread(target=z.serve_forever, daemon=True).start()
    ylog = MessageLog("tc_Y2")
    y = request_reply.Service("tc_Y2", BASE + 2, ylog)

    def relay(args, req):
        # Y handles X's request by calling Z: Y's send to Z happens after Y's receive from X
        request_reply.call("127.0.0.1", BASE + 1, "note", args, "tc_Y2", ylog)
        return {"relayed": True}
    y.register("relay", relay)
    threading.Thread(target=y.serve_forever, daemon=True).start()
    time.sleep(0.2)

    for _ in range(100):
        clock_for("tc_Z").tick()            # Z has been busy: its clock is at 100
    xlog = MessageLog("tc_X2")
    request_reply.call("127.0.0.1", BASE + 2, "relay", {"k": 1}, "tc_X2", xlog)

    x_send = [l for l in log_lines("tc_X2") if l["dir"] == "send"][0]
    y_recv = [l for l in log_lines("tc_Y2") if l["dir"] == "recv" and l["msg"]["type"] == "REQUEST"][0]
    y_send = [l for l in log_lines("tc_Y2") if l["dir"] == "send" and l["msg"]["type"] == "REQUEST"][0]
    z_recv = [l for l in log_lines("tc_Z") if l["dir"] == "recv"][0]
    x_back = [l for l in log_lines("tc_X2") if l["dir"] == "recv"][0]
    chain = [x_send["lamport"], y_recv["lamport"], y_send["lamport"], z_recv["lamport"]]
    check(chain == sorted(chain) and len(set(chain)) == 4,
          "X send < Y recv < Y send < Z recv: %s" % " < ".join(map(str, chain)))
    check(z_recv["lamport"] == 101, "Z, already at 100, jumps to max(100, %d) + 1 = 101" % y_send["lamport"])
    check(x_back["lamport"] > z_recv["lamport"],
          "X hears back after Z's event (%d > %d): Z's large clock flowed back through Y's reply" % (x_back["lamport"], z_recv["lamport"]))
    y.stop()
    z.stop()


def test_event_order_tool():
    print("[event_order.py]")
    # only our test logs: copy them into a temp folder so the real logs/ don't interfere
    import shutil
    import tempfile
    d = tempfile.mkdtemp()
    src = os.path.dirname(MessageLog("x").path)
    for f in os.listdir(src):
        if f.startswith("tc_"):
            shutil.copy(os.path.join(src, f), d)
    out = subprocess.run([sys.executable, os.path.join(PROTO, "event_order.py"), "--dir", d, "--check"],
                         cwd=PROTO, capture_output=True, text=True)
    check(out.returncode == 0 and "clock condition HOLDS" in out.stdout,
          "--check: every receive and every reply comes after its cause\n        " + out.stdout.strip().replace("\n", "\n        "))
    out = subprocess.run([sys.executable, os.path.join(PROTO, "event_order.py"), "--dir", d, "--no-info"],
                         cwd=PROTO, capture_output=True, text=True)
    stamps = [int(l.split()[0][2:]) for l in out.stdout.splitlines() if l.startswith("L=")]
    check(stamps and stamps == sorted(stamps), "merged timeline is sorted by Lamport time (%d lines)" % len(stamps))
    shutil.rmtree(d, ignore_errors=True)


def main():
    clear_logs()
    test_rules()
    test_envelope()
    test_request_reply()
    test_causal_chain()
    test_event_order_tool()
    print()
    if FAILS:
        print("%d FAILED:" % len(FAILS))
        for f in FAILS:
            print("  - " + f)
        sys.exit(1)
    print("ALL PASSED")


if __name__ == "__main__":
    main()
