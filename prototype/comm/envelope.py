"""
The envelope: fields every message carries, no matter which pattern sent it.

    type       what kind of message this is, used to dispatch
               (REQUEST, REPLY, SUBSCRIBE, PUBLISH, EVENT, JOIN, GROUP_MSG, ...)
    msg_id     unique id; a REPLY carries the REQUEST's id in `reply_to`
    sender     name of the process that sent it, used in logs
    timestamp  ISO 8601 with milliseconds, local wall clock of the sender.
               Not a global order, but good enough evidence in the logs.
    lamport    the sender's Lamport clock when it sent the message (Milestone 4).
               This one does order events across processes; see comm/clock.py.
    payload    whatever the message type needs

Example:

    {"type": "REQUEST", "msg_id": "3f9a1c2e", "sender": "client_alice",
     "timestamp": "2026-10-11T14:02:33.412", "lamport": 7, "reply_to": null,
     "payload": {"op": "reserve", "args": {"space": "A-03"}}}
"""

import datetime
import uuid

from comm.clock import clock_for

TYPES = {
    # request-reply
    "REQUEST", "REPLY",
    # pub-sub
    "SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH", "EVENT", "ACK",
    # group communication
    "JOIN", "LEAVE", "MEMBERS", "GROUP_MSG",
    # peer-to-peer overlay (Milestone 3)
    "P2P_JOIN", "P2P_NEIGHBOR", "P2P_LEAVE", "P2P_PING",   # membership and maintenance, each answered by ACK
    "P2P_GOSSIP",                                          # push: rumor with a versioned value, no reply
    "P2P_LOOKUP", "P2P_FOUND",                             # pull: TTL flood and the answer sent back to the asker
    "P2P_AGG", "P2P_AGG_REPLY",                            # convergecast over a spanning tree
}


def new_id():
    return uuid.uuid4().hex[:8]


def timestamp():
    return datetime.datetime.now().isoformat(timespec="milliseconds")


def make(msg_type, sender, payload=None, reply_to=None, msg_id=None):
    """Build a message. Sending is an event, so this ticks the sender's clock."""
    if msg_type not in TYPES:
        raise ValueError("unknown message type %r" % msg_type)
    return {
        "type": msg_type,
        "msg_id": msg_id or new_id(),
        "sender": sender,
        "timestamp": timestamp(),
        "lamport": clock_for(sender).send(),
        "reply_to": reply_to,
        "payload": payload if payload is not None else {},
    }


def stamp(msg):
    """Re-stamp a message that is sent again (a retry, a cached reply). Same
    msg_id, so the receiver still sees it as a duplicate, but a new send event."""
    msg["lamport"] = clock_for(msg["sender"]).send()
    return msg


def validate(msg):
    """Return None if the envelope is well formed, otherwise a short reason.
    lamport is optional so logs and peers from before Milestone 4 still parse."""
    if not isinstance(msg, dict):
        return "not an object"
    for field in ("type", "msg_id", "sender", "timestamp", "payload"):
        if field not in msg:
            return "missing field %r" % field
    if msg["type"] not in TYPES:
        return "unknown type %r" % msg["type"]
    if not isinstance(msg["payload"], dict):
        return "payload is not an object"
    if "lamport" in msg and (not isinstance(msg["lamport"], int) or msg["lamport"] < 0):
        return "lamport is not a non-negative integer"
    return None
