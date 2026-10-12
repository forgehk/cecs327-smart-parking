"""
Framing: how one message becomes bytes on a TCP socket and back.

TCP is a byte stream. It does not know where one message ends and the next one
starts, and a single recv() can return half a message or two messages glued
together. So every message is sent as:

    4 bytes  big-endian unsigned length of the body  (struct "!I")
    N bytes  the body: a JSON object encoded as UTF-8

Marshalling (dict -> bytes):
    json.dumps(msg) -> str.encode("utf-8") -> struct.pack("!I", len(body)) + body

Unmarshalling (bytes -> dict):
    read exactly 4 bytes -> struct.unpack("!I") -> read exactly N bytes
    -> bytes.decode("utf-8") -> json.loads

recv_exact() keeps calling recv() until it has the number of bytes it was asked
for, which is what deals with partial reads. If the peer closes in the middle
of a message we raise ConnectionClosed. If the body is not valid JSON, or the
length header is absurd, we raise ProtocolError so the caller can log it and
drop the connection instead of crashing.
"""

import json
import socket
import struct

HEADER = struct.Struct("!I")
MAX_MESSAGE_BYTES = 1 << 20      # 1 MiB; anything bigger is treated as garbage


class ConnectionClosed(Exception):
    """The other side closed the socket (cleanly or mid-message)."""


class ProtocolError(Exception):
    """Bytes arrived but they are not a valid framed JSON message."""


def marshal(msg):
    """dict -> framed bytes"""
    body = json.dumps(msg, separators=(",", ":")).encode("utf-8")
    if len(body) > MAX_MESSAGE_BYTES:
        raise ProtocolError("message too large: %d bytes" % len(body))
    return HEADER.pack(len(body)) + body


def unmarshal(body):
    """body bytes -> dict (length header already stripped)"""
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProtocolError("body is not UTF-8: %s" % e)
    try:
        msg = json.loads(text)
    except json.JSONDecodeError as e:
        raise ProtocolError("body is not JSON: %s" % e)
    if not isinstance(msg, dict):
        raise ProtocolError("message is not a JSON object")
    return msg


def recv_exact(sock, n):
    """Read exactly n bytes, looping over partial recv() results."""
    chunks = []
    remaining = n
    while remaining > 0:
        try:
            chunk = sock.recv(remaining)
        except ConnectionResetError:
            raise ConnectionClosed("connection reset")
        if not chunk:
            if remaining == n:
                raise ConnectionClosed("peer closed")
            raise ConnectionClosed("peer closed after %d of %d bytes" % (n - remaining, n))
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def send_msg(sock, msg):
    """Marshal and send one message. Raises OSError if the socket is broken."""
    sock.sendall(marshal(msg))


def recv_msg(sock):
    """Receive one message. Raises ConnectionClosed or ProtocolError."""
    header = recv_exact(sock, HEADER.size)
    (length,) = HEADER.unpack(header)
    if length == 0 or length > MAX_MESSAGE_BYTES:
        raise ProtocolError("bad length header: %d" % length)
    body = recv_exact(sock, length)
    return unmarshal(body)


def connect(host, port, timeout):
    """Open a TCP connection with a timeout that also applies to later reads."""
    s = socket.create_connection((host, port), timeout=timeout)
    s.settimeout(timeout)
    return s
