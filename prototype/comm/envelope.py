"""
The envelope: fields every message carries, no matter which pattern sent it.

    type       what kind of message this is, used to dispatch
               (REQUEST, REPLY, SUBSCRIBE, PUBLISH, EVENT, JOIN, GROUP_MSG, ...)
    msg_id     unique id; a REPLY carries the REQUEST's id in `reply_to`
    sender     name of the process that sent it, used in logs
    timestamp  ISO 8601 with milliseconds, local wall clock of the sender.
               Not a global order, but good enough evidence in the logs.
    payload    whatever the message type needs

Example:

    {"type": "REQUEST", "msg_id": "3f9a1c2e", "sender": "client_alice",
     "timestamp": "2026-10-11T14:02:33.412", "reply_to": null,
     "payload": {"op": "reserve", "args": {"space": "A-03"}}}
"""

import datetime
import uuid

TYPES = {
    # request-reply
    "REQUEST", "REPLY",
    # pub-sub
    "SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH", "EVENT", "ACK",
    # group communication
    "JOIN", "LEAVE", "MEMBERS", "GROUP_MSG",
}


def new_id():
    return uuid.uuid4().hex[:8]


def timestamp():
    return datetime.datetime.now().isoformat(timespec="milliseconds")


def make(msg_type, sender, payload=None, reply_to=None, msg_id=None):
    if msg_type not in TYPES:
        raise ValueError("unknown message type %r" % msg_type)
    return {
        "type": msg_type,
        "msg_id": msg_id or new_id(),
        "sender": sender,
        "timestamp": timestamp(),
        "reply_to": reply_to,
        "payload": payload if payload is not None else {},
    }


def validate(msg):
    """Return None if the envelope is well formed, otherwise a short reason."""
    if not isinstance(msg, dict):
        return "not an object"
    for field in ("type", "msg_id", "sender", "timestamp", "payload"):
        if field not in msg:
            return "missing field %r" % field
    if msg["type"] not in TYPES:
        return "unknown type %r" % msg["type"]
    if not isinstance(msg["payload"], dict):
        return "payload is not an object"
    return None
