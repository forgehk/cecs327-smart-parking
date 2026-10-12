#!/usr/bin/env python3
"""
Unit tests for comm.framing and comm.envelope. No network, uses socketpair.

    cd prototype && python3 tests/test_framing.py
"""

import os
import socket
import struct
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from comm import envelope, framing

FAILS = []


def check(cond, text):
    print(("  PASS  " if cond else "  FAIL  ") + text)
    if not cond:
        FAILS.append(text)


def pair():
    a, b = socket.socketpair()
    a.settimeout(2)
    b.settimeout(2)
    return a, b


def test_roundtrip():
    a, b = pair()
    msg = envelope.make("REQUEST", "tester", {"op": "find", "n": 1, "nested": {"x": [1, 2]}})
    framing.send_msg(a, msg)
    got = framing.recv_msg(b)
    check(got == msg, "marshal -> unmarshal gives the same dict back")
    a.close(); b.close()


def test_two_messages_one_recv():
    """Two messages sent back to back must come out as two separate messages."""
    a, b = pair()
    m1 = envelope.make("PUBLISH", "p", {"topic": "t", "data": 1})
    m2 = envelope.make("PUBLISH", "p", {"topic": "t", "data": 2})
    a.sendall(framing.marshal(m1) + framing.marshal(m2))   # one write, glued together
    g1 = framing.recv_msg(b)
    g2 = framing.recv_msg(b)
    check(g1 == m1 and g2 == m2, "two messages in one TCP write are split correctly")
    a.close(); b.close()


def test_partial_send():
    """Message arrives in small pieces; recv_exact must wait for all of it."""
    a, b = pair()
    msg = envelope.make("EVENT", "broker", {"topic": "lot/A", "data": "x" * 5000})
    data = framing.marshal(msg)

    def dribble():
        for i in range(0, len(data), 7):
            a.sendall(data[i:i + 7])
    t = threading.Thread(target=dribble)
    t.start()
    got = framing.recv_msg(b)
    t.join()
    check(got == msg, "message sent in 7-byte pieces is reassembled")
    a.close(); b.close()


def test_malformed_json():
    a, b = pair()
    body = b"{this is not json"
    a.sendall(struct.pack("!I", len(body)) + body)
    try:
        framing.recv_msg(b)
        check(False, "malformed JSON raises ProtocolError")
    except framing.ProtocolError:
        check(True, "malformed JSON raises ProtocolError")
    a.close(); b.close()


def test_bad_length():
    a, b = pair()
    a.sendall(struct.pack("!I", 0xFFFFFFFF))
    try:
        framing.recv_msg(b)
        check(False, "absurd length header raises ProtocolError")
    except framing.ProtocolError:
        check(True, "absurd length header raises ProtocolError")
    a.close(); b.close()


def test_closed_mid_message():
    a, b = pair()
    msg = envelope.make("REQUEST", "c", {"op": "find"})
    data = framing.marshal(msg)
    a.sendall(data[:10])
    a.close()
    try:
        framing.recv_msg(b)
        check(False, "peer closing mid-message raises ConnectionClosed")
    except framing.ConnectionClosed:
        check(True, "peer closing mid-message raises ConnectionClosed")
    b.close()


def test_envelope_validate():
    good = envelope.make("REPLY", "s", {"status": "OK"}, reply_to="abc")
    check(envelope.validate(good) is None, "a made envelope validates")
    check(envelope.validate({"type": "REQUEST"}) is not None, "missing fields are rejected")
    check(envelope.validate({"type": "NOPE", "msg_id": "1", "sender": "s", "timestamp": "t", "payload": {}}) is not None,
          "unknown type is rejected")
    check(len(good["timestamp"]) == 23 and good["timestamp"][10] == "T", "timestamp is ISO 8601 with milliseconds")


if __name__ == "__main__":
    print("[framing]")
    test_roundtrip()
    test_two_messages_one_recv()
    test_partial_send()
    test_malformed_json()
    test_bad_length()
    test_closed_mid_message()
    print("[envelope]")
    test_envelope_validate()
    print("\n" + ("ALL PASSED" if not FAILS else "%d FAILED" % len(FAILS)))
    sys.exit(1 if FAILS else 0)
