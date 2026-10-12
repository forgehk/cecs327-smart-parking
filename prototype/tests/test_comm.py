#!/usr/bin/env python3
"""
Tests for the three communication patterns. Starts a Service, a Broker and a
Registry in threads inside this process and talks to them over real TCP sockets.

    cd prototype && python3 tests/test_comm.py
"""

import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from comm import envelope, framing, group, pubsub, request_reply
from comm.request_reply import BadRequest, ServiceUnavailable
from common import MessageLog

FAILS = []
BASE = 5900          # test ports: 5900 service, 5901 broker, 5902 registry, 5903+ members


def check(cond, text):
    print(("  PASS  " if cond else "  FAIL  ") + text)
    if not cond:
        FAILS.append(text)


def log_lines(name):
    path = MessageLog(name).path
    import json
    return [json.loads(l) for l in open(path)] if os.path.exists(path) else []


def clear_logs():
    d = os.path.dirname(MessageLog("x").path)
    for f in os.listdir(d):
        if f.startswith("t_"):
            os.remove(os.path.join(d, f))


# ------------------------------------------------------------ request-reply

def test_request_reply():
    print("[request-reply]")
    svc = request_reply.Service("t_service", BASE, MessageLog("t_service"))
    calls = []

    def add(args, req):
        if "a" not in args or "b" not in args:
            raise BadRequest("need a and b")
        calls.append(req["msg_id"])
        return {"sum": args["a"] + args["b"]}
    svc.register("add", add)
    threading.Thread(target=svc.serve_forever, daemon=True).start()
    time.sleep(0.2)
    clog = MessageLog("t_client")

    r = request_reply.call("127.0.0.1", BASE, "add", {"a": 2, "b": 3}, "t_client", clog)
    check(r["status"] == "OK" and r["result"]["sum"] == 5, "add(2,3) -> OK 5")

    r = request_reply.call("127.0.0.1", BASE, "nope", {}, "t_client", clog)
    check(r["status"] == "ERROR" and "unknown operation" in r["error"], "unknown op -> ERROR: " + r.get("error", ""))

    r = request_reply.call("127.0.0.1", BASE, "add", {"a": 1}, "t_client", clog)
    check(r["status"] == "ERROR" and r["error"] == "need a and b", "bad args -> ERROR: " + r.get("error", ""))

    before = len(calls)
    r = request_reply.call("127.0.0.1", BASE, "add", {"a": 10, "b": 1}, "t_client", clog, drop_first_reply=True)
    check(r["status"] == "OK" and r["result"]["sum"] == 11 and r.get("duplicate") is True and len(calls) == before + 1,
          "lost reply + retry with same msg_id -> cached reply, handler ran once")

    t0 = time.time()
    try:
        request_reply.call("127.0.0.1", BASE + 50, "add", {"a": 1, "b": 1}, "t_client", clog, timeout=0.5, retries=2)
        check(False, "service unavailable raises ServiceUnavailable")
    except ServiceUnavailable as e:
        check(True, "service unavailable raises ServiceUnavailable after retries (%.1fs): %s" % (time.time() - t0, str(e)[:60]))

    lines = log_lines("t_client")
    kinds = [(l["dir"], l["msg"].get("type")) for l in lines]
    check(("send", "REQUEST") in kinds and ("recv", "REPLY") in kinds, "client log has send REQUEST and recv REPLY")
    notes = [l for l in lines if l["dir"] == "info" and "failed" in l["msg"].get("note", "")]
    check(len(notes) >= 2, "client log has one line per failed attempt (%d)" % len(notes))
    slines = log_lines("t_service")
    rep = [l for l in slines if l["dir"] == "send" and l["msg"]["type"] == "REPLY"]
    req = [l for l in slines if l["dir"] == "recv" and l["msg"]["type"] == "REQUEST"]
    check(rep and rep[0]["msg"]["reply_to"] == req[0]["msg"]["msg_id"], "REPLY.reply_to echoes REQUEST.msg_id")
    svc.stop()


# ------------------------------------------------------------------ pub-sub

def test_pubsub():
    print("[pub-sub]")
    broker = pubsub.Broker("t_broker", BASE + 1, MessageLog("t_broker"))
    threading.Thread(target=broker.serve_forever, daemon=True).start()
    time.sleep(0.2)

    got1, got2 = [], []
    s1 = pubsub.Subscriber("127.0.0.1", BASE + 1, "t_sub1", MessageLog("t_sub1"), lambda t, d, m: got1.append((t, d))).start()
    s2 = pubsub.Subscriber("127.0.0.1", BASE + 1, "t_sub2", MessageLog("t_sub2"), lambda t, d, m: got2.append((t, d))).start()
    s1.subscribe("lot/A/sensor")
    s2.subscribe("lot/A/sensor")
    s2.subscribe("lot/B/sensor")
    time.sleep(0.4)

    pub = pubsub.Publisher("127.0.0.1", BASE + 1, "t_pub", MessageLog("t_pub"))
    n = pub.publish("lot/A/sensor", {"space": "A-01", "state": "free"})
    time.sleep(0.3)
    check(n == 2, "PUBLISH to a topic with 2 subscribers -> broker ACK delivered=2 (%d)" % n)
    check(got1 == [("lot/A/sensor", {"space": "A-01", "state": "free"})] and got2 == got1,
          "both subscribers got their own copy of the EVENT")

    n = pub.publish("lot/B/sensor", {"x": 1})
    time.sleep(0.3)
    check(n == 1 and len(got1) == 1 and len(got2) == 2, "topic routing: only the lot/B subscriber got the lot/B event")

    n = pub.publish("nobody/listens", {"x": 1})
    check(n == 0, "PUBLISH with no subscribers -> delivered=0, no error")

    s1.unsubscribe("lot/A/sensor")
    time.sleep(0.3)
    n = pub.publish("lot/A/sensor", {"space": "A-02", "state": "occupied"})
    time.sleep(0.3)
    check(n == 1 and len(got1) == 1 and len(got2) == 3, "after UNSUBSCRIBE sub1 no longer receives")

    # subscriber dies without an UNSUBSCRIBE: the broker must notice and drop it.
    # (Locally the kernel reports the close right away; on a real link the first
    # failed send is what triggers the removal. Both paths log and remove.)
    s2.stopping = True
    s2.sock.close()
    time.sleep(0.3)
    n = pub.publish("lot/A/sensor", {"space": "A-03", "state": "free"})
    n2 = pub.publish("lot/A/sensor", {"space": "A-04", "state": "free"})
    blines = log_lines("t_broker")
    removed = [l for l in blines if l["dir"] == "info" and "removed" in l["msg"].get("note", "")]
    check(n2 == 0 and len(removed) >= 1, "dead subscriber is logged and removed, later publishes skip it (delivered %d then %d)" % (n, n2))

    ev = [l for l in blines if l["dir"] == "send" and l["msg"]["type"] == "EVENT"]
    check(len(ev) >= 5, "broker log has one send line per EVENT delivery (%d)" % len(ev))
    pub.close()
    s1.stop()
    broker.stopping = True


# ------------------------------------------------------------------- group

def test_group():
    print("[group]")
    reg = group.Registry("t_registry", BASE + 2, MessageLog("t_registry"))
    threading.Thread(target=reg.serve_forever, daemon=True).start()
    time.sleep(0.2)

    received = {}
    members_up = []
    for i, port in enumerate((BASE + 3, BASE + 4, BASE + 5)):
        name = "t_member%d" % i
        svc = request_reply.Service(name, port, MessageLog(name))
        received[name] = []
        svc.on_type("GROUP_MSG", (lambda n: lambda msg, conn: received[n].append(msg["payload"]["data"]))(name))
        threading.Thread(target=svc.serve_forever, daemon=True).start()
        members_up.append(svc)
        group.join("127.0.0.1", BASE + 2, "lots", "127.0.0.1:%d" % port, name, MessageLog(name))
    time.sleep(0.2)

    m = group.members("127.0.0.1", BASE + 2, "lots", "t_member0", MessageLog("t_member0"))
    check(len(m) == 3, "MEMBERS after three JOINs -> 3 (%s)" % m)

    slog = MessageLog("t_member0")
    delivered, failed = group.group_send("127.0.0.1", BASE + 2, "lots", {"hello": 1}, "t_member0", slog,
                                         exclude="127.0.0.1:%d" % (BASE + 3))
    time.sleep(0.3)
    check(len(delivered) == 2 and not failed, "group_send fans out to the 2 other members (%s)" % delivered)
    check(received["t_member1"] == [{"hello": 1}] and received["t_member2"] == [{"hello": 1}] and received["t_member0"] == [],
          "each other member received GROUP_MSG once, sender excluded")
    lines = [l for l in log_lines("t_member0") if l["dir"] == "send" and l["msg"]["type"] == "GROUP_MSG"]
    check(len(lines) == 2, "sender log has one line per target (%d)" % len(lines))

    members_up[2].stop()
    time.sleep(0.2)
    delivered, failed = group.group_send("127.0.0.1", BASE + 2, "lots", {"hello": 2}, "t_member0", slog,
                                         exclude="127.0.0.1:%d" % (BASE + 3))
    time.sleep(0.3)
    check(len(delivered) == 1 and len(failed) == 1 and received["t_member1"][-1] == {"hello": 2},
          "dead member is logged and skipped, the others still get it (failed=%s)" % failed)

    group.leave("127.0.0.1", BASE + 2, "lots", "127.0.0.1:%d" % (BASE + 5), "t_member2", MessageLog("t_member2"))
    m = group.members("127.0.0.1", BASE + 2, "lots", "t_member0", slog)
    check(len(m) == 2, "LEAVE removes the member (%s)" % m)
    reg.stopping = True


if __name__ == "__main__":
    clear_logs()
    test_request_reply()
    test_pubsub()
    test_group()
    print("\n" + ("ALL PASSED" if not FAILS else "%d FAILED:\n  - " % len(FAILS) + "\n  - ".join(FAILS)))
    sys.exit(1 if FAILS else 0)
