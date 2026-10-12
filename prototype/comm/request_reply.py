"""
Request-reply: one connection, one REQUEST, one REPLY, close.

Client side:   call(host, port, op, args, ...)
Service side:  Service(name, port, log); service.register("find", handler); service.serve_forever()

REQUEST payload: {"op": "reserve", "args": {"space": "A-03", "client": "alice"}}
REPLY   payload: {"status": "OK",    "result": {...}}
                 {"status": "ERROR", "error": "unknown operation 'foo'"}
The REPLY's reply_to field carries the REQUEST's msg_id so the two can be matched
up in the logs.

Why one exchange per connection: the client never has to track which reply
belongs to which request on a shared socket, a half-dead connection can't poison
later requests, and the server thread handling it has a clear start and end.
The cost is one TCP handshake per request, which is fine at our message rates.

Retries: if the client gets no reply within the timeout it reconnects and sends
the same REQUEST again with the same msg_id. The service remembers the replies it
already sent, so a retry gets the cached reply instead of being executed twice.

Error cases the service answers with status ERROR:
    - unknown operation
    - handler raised BadRequest (bad arguments)
    - handler raised anything else (reported as "internal error", logged)
Error case the client raises ServiceUnavailable for:
    - no reply after all retries (service down, or link broken)
"""

import socket
import threading
from concurrent.futures import ThreadPoolExecutor

from comm import envelope, framing
from comm.framing import ConnectionClosed, ProtocolError

DEFAULT_TIMEOUT = 2.0
DEFAULT_RETRIES = 3
SERVICE_READ_TIMEOUT = 10.0


class BadRequest(Exception):
    """A handler raises this when the arguments don't make sense."""


class ServiceUnavailable(Exception):
    """The client gave up: no reply after all retries."""


# ---------------------------------------------------------------- client side

def call(host, port, op, args, sender, log, timeout=DEFAULT_TIMEOUT,
         retries=DEFAULT_RETRIES, msg_id=None, drop_first_reply=False):
    """Send one request, return the REPLY payload. Retries with the same msg_id."""
    peer = "%s:%d" % (host, port)
    request = envelope.make("REQUEST", sender, {"op": op, "args": args}, msg_id=msg_id)
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            with framing.connect(host, port, timeout) as s:
                log.record("send", peer, request)
                framing.send_msg(s, request)
                if drop_first_reply and attempt == 1:
                    # demo hook: pretend the reply was lost so we retry
                    log.record("info", peer, {"note": "dropping reply to %s on purpose" % request["msg_id"]})
                    continue
                reply = framing.recv_msg(s)
                log.record("recv", peer, reply)
                if reply.get("type") != "REPLY" or reply.get("reply_to") != request["msg_id"]:
                    last_error = "reply did not match request"
                    continue
                return reply["payload"]
        except socket.timeout:
            last_error = "timeout after %.1fs" % timeout
        except (ConnectionRefusedError, ConnectionClosed, OSError) as e:
            last_error = "connection failed: %s" % e
        except ProtocolError as e:
            last_error = "bad reply: %s" % e
        log.record("info", peer, {"note": "attempt %d/%d failed: %s" % (attempt, retries, last_error),
                                  "msg_id": request["msg_id"]})
    raise ServiceUnavailable("%s %s: %s after %d attempts" % (peer, op, last_error, retries))


# --------------------------------------------------------------- service side

class Service:
    """
    A TCP listener that dispatches framed messages by envelope type.

    REQUEST messages go to the handler registered for payload["op"].
    Other types (EVENT, GROUP_MSG, ...) go to a handler registered with on_type(),
    so a process can be a request-reply service and a group member on one port.
    """

    def __init__(self, name, port, log, workers=8):
        self.name = name
        self.port = port
        self.log = log
        self.ops = {}
        self.type_handlers = {}
        self.reply_cache = {}             # msg_id -> REPLY envelope (idempotent retries)
        self.lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=workers)
        self.stopping = False
        self.sock = None

    def register(self, op, handler):
        """handler(args: dict, request_envelope) -> result dict. Raise BadRequest for bad args."""
        self.ops[op] = handler

    def on_type(self, msg_type, handler):
        """handler(msg_envelope, conn) for non-REQUEST messages."""
        self.type_handlers[msg_type] = handler

    def _reply_for(self, request):
        with self.lock:
            cached = self.reply_cache.get(request["msg_id"])
        if cached is not None:
            reply = dict(cached)
            reply["payload"] = dict(cached["payload"], duplicate=True)
            return reply

        op = request["payload"].get("op")
        args = request["payload"].get("args", {})
        handler = self.ops.get(op)
        if handler is None:
            payload = {"status": "ERROR", "error": "unknown operation %r" % op}
        elif not isinstance(args, dict):
            payload = {"status": "ERROR", "error": "args must be an object"}
        else:
            try:
                payload = {"status": "OK", "result": handler(args, request)}
            except BadRequest as e:
                payload = {"status": "ERROR", "error": str(e)}
            except Exception as e:          # a bug in the handler must not kill the service
                self.log.record("info", "-", {"note": "handler for %r crashed: %r" % (op, e)})
                payload = {"status": "ERROR", "error": "internal error"}

        reply = envelope.make("REPLY", self.name, payload, reply_to=request["msg_id"])
        with self.lock:
            self.reply_cache[request["msg_id"]] = reply
        return reply

    def _serve(self, conn, addr):
        peer = "%s:%d" % addr
        conn.settimeout(SERVICE_READ_TIMEOUT)
        try:
            while not self.stopping:
                try:
                    msg = framing.recv_msg(conn)
                except (ConnectionClosed, socket.timeout):
                    return
                except ProtocolError as e:
                    self.log.record("info", peer, {"note": "dropped connection, bad frame: %s" % e})
                    return

                self.log.record("recv", peer, msg)
                problem = envelope.validate(msg)
                if problem:
                    self.log.record("info", peer, {"note": "ignored message: %s" % problem})
                    continue

                if msg["type"] == "REQUEST":
                    reply = self._reply_for(msg)
                    self.log.record("send", peer, reply)
                    framing.send_msg(conn, reply)
                    return                           # one exchange per connection
                handler = self.type_handlers.get(msg["type"])
                if handler is None:
                    self.log.record("info", peer, {"note": "no handler for type %s" % msg["type"]})
                    continue
                handler(msg, conn)
        except OSError:
            pass
        finally:
            conn.close()

    def serve_forever(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(64)
        try:
            while not self.stopping:
                conn, addr = self.sock.accept()
                self.pool.submit(self._serve, conn, addr)
        except OSError:
            pass

    def stop(self):
        self.stopping = True
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)   # wakes up accept()
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass
