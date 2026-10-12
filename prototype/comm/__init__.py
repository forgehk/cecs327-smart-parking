"""
Communication layer for the parking system (Milestone 2).

    framing        - length-prefixed JSON over a TCP socket
    envelope       - the fields every message carries
    request_reply  - one request, one reply, one connection
    pubsub         - broker, publisher and subscriber
    group          - registry and group send

All three patterns use the same framing and envelope, so a log line looks the
same no matter which pattern produced it.
"""
