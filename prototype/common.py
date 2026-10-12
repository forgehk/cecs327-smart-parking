"""
Shared settings and the per-process message log.

The wire format lives in comm/ (framing + envelope). Every process logs what it
sends and receives to logs/<process>.log as JSON lines so the whole conversation
can be traced afterwards.
"""

import json
import os
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

# ports
BROKER_PORT = 5000          # pub-sub broker
REGISTRY_PORT = 5010        # group membership registry
LOT_PORTS = {               # lot server request-reply + GROUP_MSG listener
    "A": 5001,
    "B": 5002,
}

# topics and groups
def sensor_topic(lot):
    return "lot/%s/sensor" % lot

def status_topic(lot):
    return "lot/%s/status" % lot

LOT_SERVER_GROUP = "lot_servers"


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
            "dir": direction,      # "send", "recv", "dropped" or "info"
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
