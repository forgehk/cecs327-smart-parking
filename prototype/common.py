"""
Shared helpers for all processes.

Messages are JSON objects, one per line, sent over TCP.
Every process logs what it sends and receives to logs/<process>.log
(also JSON lines) so we can trace the whole conversation afterwards.
"""

import json
import os
import socket
import threading
import time

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

# timing (seconds) - small values so a demo doesn't take forever
HEARTBEAT_INTERVAL = 2.0
STALENESS_DELTA = 3 * HEARTBEAT_INTERVAL   # no report for this long -> unknown
PEER_SUMMARY_INTERVAL = 3.0
PEER_STALE_AFTER = 3 * PEER_SUMMARY_INTERVAL
RESERVATION_LEASE = 30.0
CLIENT_TIMEOUT = 2.0
CLIENT_RETRIES = 3

# which port each lot server listens on
LOT_PORTS = {
    "A": 5001,
    "B": 5002,
}


def now():
    return time.time()


def ts_str(t):
    return time.strftime("%H:%M:%S", time.localtime(t))


class MessageLog:
    """Append-only log file for one process."""

    def __init__(self, process_name):
        os.makedirs(LOG_DIR, exist_ok=True)
        self.process = process_name
        self.path = os.path.join(LOG_DIR, process_name + ".log")
        self._lock = threading.Lock()

    def record(self, direction, peer, msg):
        entry = {
            "ts": round(now(), 3),
            "process": self.process,
            "dir": direction,      # "send", "recv" or "dropped"
            "peer": peer,
            "msg": msg,
        }
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())

    def replay(self):
        """Yield every entry in the log, oldest first."""
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    yield json.loads(raw)
                except json.JSONDecodeError:
                    # half-written last line after a crash, skip it
                    continue


def send_json(sock, msg):
    sock.sendall((json.dumps(msg) + "\n").encode("utf-8"))


def read_lines(sock):
    """Yield one JSON object per line until the other side closes."""
    buf = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            return
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if not line.strip():
                continue
            try:
                yield json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                continue


def peer_name(sock):
    try:
        host, port = sock.getpeername()
        return "%s:%d" % (host, port)
    except OSError:
        return "?"
