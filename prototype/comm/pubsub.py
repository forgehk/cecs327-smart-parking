"""
Publish-subscribe through a broker.

    Broker(name, port, log).serve_forever()         the broker process
    Subscriber(host, port, sender, log, on_event)   long-lived connection, receives EVENTs
    Publisher(host, port, sender, log)              sends PUBLISH, gets an ACK back

Broker state: a dict mapping each topic to the set of subscriber connections,
protected by a lock because every connection runs in its own thread.

Subscribe:   subscriber opens a connection and sends SUBSCRIBE {topic}.
             broker adds the connection to that topic's set and sends ACK.
             the connection stays open; EVENTs arrive on it later.
Publish:     publisher sends PUBLISH {topic, data}.
             broker looks up the topic, sends EVENT {topic, data, publisher} to
             every subscriber on its own connection, logs one line per delivery,
             then sends ACK {delivered: n} back to the publisher.
Unsubscribe: subscriber sends UNSUBSCRIBE {topic}; broker removes it, sends ACK.

Unreachable subscriber: the send fails, the broker logs it and removes that
subscriber from the topic. We don't retry: a subscriber that can't be reached
has to re-subscribe when it comes back, and its first heartbeat/snapshot will
catch it up. Retrying would make a dead subscriber slow everyone else down.

Limitations: the broker is a single point of failure, and a slow subscriber
delays the EVENTs to the subscribers after it in the same publish.
"""

import socket
import threading
import time

from comm import envelope, framing
from comm.framing import ConnectionClosed, ProtocolError


class Broker:
    def __init__(self, name, port, log):
        self.name = name
        self.port = port
        self.log = log
        self.topics = {}            # topic -> set of _Conn
        self.lock = threading.Lock()
        self.stopping = False
        self.sock = None

    class _Conn:
        """A subscriber connection plus the lock that serialises writes to it."""
        def __init__(self, sock, peer):
            self.sock = sock
            self.peer = peer
            self.wlock = threading.Lock()

    def info(self, text):
        print("[%s] [%s] %s" % (time.strftime("%H:%M:%S"), self.name, text), flush=True)

    def _ack(self, conn, peer, reply_to, payload):
        ack = envelope.make("ACK", self.name, payload, reply_to=reply_to)
        self.log.record("send", peer, ack)
        with conn.wlock:
            framing.send_msg(conn.sock, ack)

    def _subscribe(self, conn, msg):
        topic = msg["payload"].get("topic")
        with self.lock:
            self.topics.setdefault(topic, set()).add(conn)
            n = len(self.topics[topic])
        self.info("SUBSCRIBE %s -> %s (%d subscriber%s)" % (msg["sender"], topic, n, "" if n == 1 else "s"))
        self._ack(conn, conn.peer, msg["msg_id"], {"status": "OK", "topic": topic})

    def _unsubscribe(self, conn, msg):
        topic = msg["payload"].get("topic")
        with self.lock:
            self.topics.get(topic, set()).discard(conn)
        self.info("UNSUBSCRIBE %s from %s" % (msg["sender"], topic))
        self._ack(conn, conn.peer, msg["msg_id"], {"status": "OK", "topic": topic})

    def _publish(self, conn, msg):
        topic = msg["payload"].get("topic")
        data = msg["payload"].get("data")
        with self.lock:
            targets = list(self.topics.get(topic, set()))
        delivered = 0
        for sub in targets:
            event = envelope.make("EVENT", self.name,
                                  {"topic": topic, "data": data, "publisher": msg["sender"]})
            try:
                with sub.wlock:
                    framing.send_msg(sub.sock, event)
                self.log.record("send", sub.peer, event)
                delivered += 1
            except OSError as e:
                self.log.record("info", sub.peer, {"note": "subscriber unreachable, removed from %s: %s" % (topic, e)})
                self.info("subscriber %s unreachable, removed from %s" % (sub.peer, topic))
                with self.lock:
                    self.topics.get(topic, set()).discard(sub)
        self._ack(conn, conn.peer, msg["msg_id"], {"status": "OK", "topic": topic, "delivered": delivered})

    def _drop(self, conn):
        """Connection closed or broke: forget it everywhere."""
        gone = []
        with self.lock:
            for topic, subs in self.topics.items():
                if conn in subs:
                    subs.discard(conn)
                    gone.append(topic)
        if gone:
            self.log.record("info", conn.peer, {"note": "subscriber disconnected, removed from %s" % ", ".join(gone)})
            self.info("subscriber %s disconnected, removed from %s" % (conn.peer, ", ".join(gone)))
        try:
            conn.sock.close()
        except OSError:
            pass

    def _serve(self, sock, addr):
        peer = "%s:%d" % addr
        conn = self._Conn(sock, peer)
        sock.settimeout(None)               # subscribers sit idle for a long time
        try:
            while not self.stopping:
                try:
                    msg = framing.recv_msg(sock)
                except ConnectionClosed:
                    return
                except ProtocolError as e:
                    self.log.record("info", peer, {"note": "dropped connection, bad frame: %s" % e})
                    return
                self.log.record("recv", peer, msg)
                problem = envelope.validate(msg)
                if problem:
                    self.log.record("info", peer, {"note": "ignored message: %s" % problem})
                    continue
                t = msg["type"]
                if t == "SUBSCRIBE":
                    self._subscribe(conn, msg)
                elif t == "UNSUBSCRIBE":
                    self._unsubscribe(conn, msg)
                elif t == "PUBLISH":
                    self._publish(conn, msg)
                else:
                    self.log.record("info", peer, {"note": "broker ignores type %s" % t})
        except OSError:
            pass
        finally:
            self._drop(conn)

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


class Subscriber:
    """
    Keeps one long-lived connection to the broker and a reader thread.
    on_event(topic, data, event_envelope) is called for every EVENT.
    If the broker goes away the reader reconnects every second and re-subscribes.
    """

    def __init__(self, host, port, sender, log, on_event):
        self.host = host
        self.port = port
        self.sender = sender
        self.log = log
        self.on_event = on_event
        self.peer = "%s:%d" % (host, port)
        self.topics = set()
        self.sock = None
        self.wlock = threading.Lock()
        self.stopping = False
        self.connected = threading.Event()

    def _send(self, msg):
        with self.wlock:
            self.log.record("send", self.peer, msg)
            framing.send_msg(self.sock, msg)

    def subscribe(self, topic):
        self.topics.add(topic)
        if self.sock is not None:
            try:
                self._send(envelope.make("SUBSCRIBE", self.sender, {"topic": topic}))
            except OSError:
                pass

    def unsubscribe(self, topic):
        self.topics.discard(topic)
        if self.sock is not None:
            try:
                self._send(envelope.make("UNSUBSCRIBE", self.sender, {"topic": topic}))
            except OSError:
                pass

    def _connect(self):
        self.sock = framing.connect(self.host, self.port, 2.0)
        self.sock.settimeout(None)
        for topic in list(self.topics):
            self._send(envelope.make("SUBSCRIBE", self.sender, {"topic": topic}))
        self.connected.set()

    def _reader(self):
        while not self.stopping:
            if self.sock is None:
                try:
                    self._connect()
                except OSError:
                    self.log.record("info", self.peer, {"note": "broker unreachable, retrying in 1s"})
                    time.sleep(1.0)
                    continue
            try:
                msg = framing.recv_msg(self.sock)
            except (ConnectionClosed, ProtocolError, OSError) as e:
                self.log.record("info", self.peer, {"note": "lost broker connection: %s" % e})
                self.connected.clear()
                try:
                    self.sock.close()
                except OSError:
                    pass
                self.sock = None
                continue
            self.log.record("recv", self.peer, msg)
            if msg.get("type") == "EVENT":
                p = msg["payload"]
                self.on_event(p.get("topic"), p.get("data"), msg)
            # ACKs are just logged

    def start(self):
        threading.Thread(target=self._reader, daemon=True).start()
        return self

    def stop(self):
        self.stopping = True
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass


class Publisher:
    """One connection to the broker, reused for many PUBLISH messages."""

    def __init__(self, host, port, sender, log, timeout=2.0):
        self.host = host
        self.port = port
        self.sender = sender
        self.log = log
        self.timeout = timeout
        self.peer = "%s:%d" % (host, port)
        self.sock = None

    def publish(self, topic, data):
        """Returns the number of subscribers the broker delivered to. Raises OSError if the broker is unreachable."""
        msg = envelope.make("PUBLISH", self.sender, {"topic": topic, "data": data})
        try:
            if self.sock is None:
                self.sock = framing.connect(self.host, self.port, self.timeout)
            self.log.record("send", self.peer, msg)
            framing.send_msg(self.sock, msg)
            ack = framing.recv_msg(self.sock)
            self.log.record("recv", self.peer, ack)
            return ack["payload"].get("delivered", 0)
        except (OSError, ConnectionClosed, ProtocolError) as e:
            self.log.record("info", self.peer, {"note": "publish failed: %s" % e, "msg_id": msg["msg_id"]})
            self.close()
            raise OSError(str(e))

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None
