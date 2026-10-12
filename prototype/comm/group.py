"""
Group communication through a membership registry.

    Registry(name, port, log).serve_forever()               the registry process
    join(reg_host, reg_port, group, my_addr, sender, log)   JOIN  -> ACK
    leave(...)                                              LEAVE -> ACK
    members(reg_host, reg_port, group, sender, log)         MEMBERS -> list of "host:port"
    group_send(reg_host, reg_port, group, data, sender, log)
        asks the registry for the members, then opens a new connection to each
        one and sends GROUP_MSG {group, data}. Logs one line per target so the
        fan-out is visible. A member that can't be reached is logged and
        skipped; the send still completes for everyone else.

Registry state: a dict mapping group name -> set of member addresses, with a
lock because each connection is handled in its own thread.

What this does NOT give us: ordering across members (two senders' messages can
arrive in different orders at different members), atomic delivery (some members
may get a message and others not), and a stable membership during a send (a
node can join or leave between MEMBERS and the actual sends). Those are the
things later milestones add on top.
"""

import socket
import threading
import time

from comm import envelope, framing
from comm.framing import ConnectionClosed, ProtocolError

TIMEOUT = 2.0


class Registry:
    def __init__(self, name, port, log):
        self.name = name
        self.port = port
        self.log = log
        self.groups = {}            # group -> set of "host:port"
        self.lock = threading.Lock()
        self.stopping = False
        self.sock = None

    def info(self, text):
        print("[%s] [%s] %s" % (time.strftime("%H:%M:%S"), self.name, text), flush=True)

    def _handle(self, msg):
        p = msg["payload"]
        group = p.get("group")
        if msg["type"] == "JOIN":
            with self.lock:
                self.groups.setdefault(group, set()).add(p.get("address"))
                n = len(self.groups[group])
            self.info("JOIN %s -> %s (%d member%s)" % (p.get("address"), group, n, "" if n == 1 else "s"))
            return {"status": "OK", "group": group, "members": sorted(self.groups[group])}
        if msg["type"] == "LEAVE":
            with self.lock:
                self.groups.get(group, set()).discard(p.get("address"))
            self.info("LEAVE %s from %s" % (p.get("address"), group))
            return {"status": "OK", "group": group}
        if msg["type"] == "MEMBERS":
            with self.lock:
                members = sorted(self.groups.get(group, set()))
            return {"status": "OK", "group": group, "members": members}
        return {"status": "ERROR", "error": "registry ignores type %s" % msg["type"]}

    def _serve(self, sock, addr):
        peer = "%s:%d" % addr
        sock.settimeout(TIMEOUT)
        try:
            msg = framing.recv_msg(sock)
            self.log.record("recv", peer, msg)
            problem = envelope.validate(msg)
            if problem:
                self.log.record("info", peer, {"note": "ignored message: %s" % problem})
                return
            ack = envelope.make("ACK", self.name, self._handle(msg), reply_to=msg["msg_id"])
            self.log.record("send", peer, ack)
            framing.send_msg(sock, ack)
        except (ConnectionClosed, socket.timeout, OSError):
            pass
        except ProtocolError as e:
            self.log.record("info", peer, {"note": "dropped connection, bad frame: %s" % e})
        finally:
            sock.close()

    def serve_forever(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(64)
        self.info("listening on :%d" % self.port)
        try:
            while not self.stopping:
                sock, addr = self.sock.accept()
                threading.Thread(target=self._serve, args=(sock, addr), daemon=True).start()
        except OSError:
            pass


# ------------------------------------------------------------ member side

def _ask(host, port, msg_type, payload, sender, log):
    """One connection, one message to the registry, one ACK back."""
    peer = "%s:%d" % (host, port)
    msg = envelope.make(msg_type, sender, payload)
    with framing.connect(host, port, TIMEOUT) as s:
        log.record("send", peer, msg)
        framing.send_msg(s, msg)
        ack = framing.recv_msg(s)
        log.record("recv", peer, ack)
        return ack["payload"]


def join(host, port, group, address, sender, log):
    return _ask(host, port, "JOIN", {"group": group, "address": address}, sender, log)


def leave(host, port, group, address, sender, log):
    return _ask(host, port, "LEAVE", {"group": group, "address": address}, sender, log)


def members(host, port, group, sender, log):
    return _ask(host, port, "MEMBERS", {"group": group}, sender, log).get("members", [])


def group_send(host, port, group, data, sender, log, exclude=None):
    """
    Fan out GROUP_MSG to every member of `group` (except `exclude`, usually
    ourselves). Returns (delivered_to, failed) as two lists of addresses.
    """
    try:
        targets = members(host, port, group, sender, log)
    except (OSError, ConnectionClosed, ProtocolError) as e:
        log.record("info", "%s:%d" % (host, port), {"note": "registry unreachable: %s" % e})
        return [], []

    delivered, failed = [], []
    for addr in targets:
        if addr == exclude:
            continue
        mhost, mport = addr.split(":")
        msg = envelope.make("GROUP_MSG", sender, {"group": group, "data": data})
        try:
            with framing.connect(mhost, int(mport), TIMEOUT) as s:
                log.record("send", addr, msg)
                framing.send_msg(s, msg)
            delivered.append(addr)
        except OSError as e:
            log.record("info", addr, {"note": "member unreachable, skipped: %s" % e, "msg_id": msg["msg_id"]})
            failed.append(addr)
    return delivered, failed
