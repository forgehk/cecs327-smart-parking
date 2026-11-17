"""
The one-message exchange the coordination protocols use: open a connection,
send one framed message, optionally read the ACK, close. The receiving side is
the lot server's Service (Service.on_type), the same as the P2P overlay.

Every message goes through envelope.make(), so it carries the sender's Lamport
time, and both ends log it, so the receiver's clock is updated (comm/clock.py).
"""

import socket
import threading

from comm import envelope, framing
from comm.framing import ConnectionClosed, ProtocolError


def send(addr, msg_type, payload, sender, log, want_ack=False, timeout=1.0):
    """Send one message to "host:port". Returns the ACK payload (want_ack) or True,
    or None if the node could not be reached or did not answer in time."""
    host, port = addr.split(":")
    msg = envelope.make(msg_type, sender, payload)
    try:
        with framing.connect(host, int(port), timeout) as s:
            log.record("send", addr, msg)
            framing.send_msg(s, msg)
            if not want_ack:
                return True
            ack = framing.recv_msg(s)
            log.record("recv", addr, ack)
            return ack.get("payload", {})
    except (OSError, ConnectionClosed, ProtocolError, socket.timeout) as e:
        log.record("info", addr, {"note": "%s to %s failed: %s" % (msg_type, addr, e), "msg_id": msg["msg_id"]})
        return None


def send_all(addrs, msg_type, payload, sender, log, want_ack=False, timeout=1.0):
    """send() to several nodes at once (one thread each), so a dead node only costs
    one timeout in total, not one per node. Returns {addr: result}."""
    results = {}
    lock = threading.Lock()

    def one(addr):
        r = send(addr, msg_type, payload, sender, log, want_ack, timeout)
        with lock:
            results[addr] = r
    threads = [threading.Thread(target=one, args=(a,), daemon=True) for a in addrs]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout + 1.0)
    return results


def ack(conn, msg, payload, sender, log):
    """Answer a message on the connection it came in on."""
    reply = envelope.make("ACK", sender, payload, reply_to=msg["msg_id"])
    try:
        peer = "%s:%d" % conn.getpeername()[:2]
    except OSError:
        peer = "-"
    log.record("send", peer, reply)
    try:
        framing.send_msg(conn, reply)
    except OSError:
        pass
