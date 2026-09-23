#!/usr/bin/env python3
"""
Lot server - one process per parking lot.

    python lot_server.py --lot A
    python lot_server.py --lot B

Holds the state of every space in its lot, takes updates from the lot's sensor
node and answers client queries. Everything comes in on one TCP port as JSON lines.
"""

import argparse
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common import (LOT_PORTS, STALENESS_DELTA, MessageLog, now, peer_name,
                    read_lines, send_json, ts_str)


class Space:
    def __init__(self, sid):
        self.sid = sid
        self.sensor_state = "unknown"    # last thing the sensor told us
        self.sensor_ts = 0.0             # when it told us

    def state(self, t=None):
        """What we actually report. If the sensor has been quiet for longer
        than STALENESS_DELTA we don't trust the old value any more."""
        t = t or now()
        if t - self.sensor_ts > STALENESS_DELTA:
            return "unknown"
        return self.sensor_state

    def to_dict(self, t=None):
        t = t or now()
        return {
            "id": self.sid,
            "state": self.state(t),
            "age": round(t - self.sensor_ts, 1) if self.sensor_ts else None,
        }


class LotServer:
    def __init__(self, lot, port, n_spaces, workers):
        self.lot = lot
        self.port = port
        self.name = "lot_server_" + lot
        self.log = MessageLog(self.name)
        self.lock = threading.Lock()
        self.spaces = {}
        for i in range(1, n_spaces + 1):
            sid = "%s-%02d" % (lot, i)
            self.spaces[sid] = Space(sid)
        self.pool = ThreadPoolExecutor(max_workers=workers)

    def info(self, text):
        print("[%s] [%s] %s" % (ts_str(now()), self.name, text), flush=True)

    # ---- sensor messages ----

    def handle_sensor(self, msg):
        with self.lock:
            t = now()
            if msg["type"] == "heartbeat":
                for sid, state in msg.get("spaces", {}).items():
                    if sid in self.spaces:
                        self.spaces[sid].sensor_state = state
                        self.spaces[sid].sensor_ts = t
            elif msg["type"] == "event":
                sp = self.spaces.get(msg.get("space"))
                if sp:
                    sp.sensor_state = msg["state"]
                    sp.sensor_ts = t
                    self.info("EVENT  %s -> %s (seq %s)" % (sp.sid, sp.sensor_state, msg.get("seq")))

    # ---- client messages ----

    def handle_client(self, msg, conn, peer):
        with self.lock:
            reply = self.process_client(msg)
        self.log.record("send", peer, reply)
        send_json(conn, reply)

    def process_client(self, msg):
        op = msg.get("type")
        client = msg.get("client", "?")
        reply = {
            "type": "reply",
            "op": op,
            "req_id": msg.get("req_id"),
            "client": client,
            "lot": self.lot,
            "ts": round(now(), 3),
        }
        t = now()
        if op == "find":
            free = [sp.to_dict(t) for sp in self.spaces.values() if sp.state(t) == "free"]
            reply["status"] = "OK"
            reply["free"] = free
            reply["lot_summary"] = self.summary(t)
            self.info("FIND from %s: %d free" % (client, len(free)))
        else:
            reply["status"] = "DENIED"
            reply["reason"] = "unknown op %s" % op
        return reply

    def summary(self, t=None):
        t = t or now()
        counts = {"free": 0, "occupied": 0, "unknown": 0}
        for sp in self.spaces.values():
            counts[sp.state(t)] += 1
        counts["total"] = len(self.spaces)
        return counts

    def summary_text(self):
        s = self.summary()
        return "Lot %s: %d free, %d occupied, %d unknown" % (
            self.lot, s["free"], s["occupied"], s["unknown"])

    def monitor_loop(self):
        """Background thread: print when a space goes stale so it shows up in the demo."""
        last = {sid: sp.state() for sid, sp in self.spaces.items()}
        while True:
            time.sleep(1.0)
            with self.lock:
                t = now()
                for sid, sp in self.spaces.items():
                    st = sp.state(t)
                    if st != last[sid]:
                        if st == "unknown":
                            self.info("STALE  %s: nothing from sensor for >%.0fs -> unknown"
                                      % (sid, STALENESS_DELTA))
                        last[sid] = st

    # ---- connections ----

    def serve_connection(self, conn, addr):
        peer = peer_name(conn)
        try:
            for msg in read_lines(conn):
                self.log.record("recv", peer, msg)
                t = msg.get("type")
                if t in ("heartbeat", "event"):
                    self.handle_sensor(msg)
                elif t == "find":
                    self.handle_client(msg, conn, peer)
                else:
                    self.info("ignored message type %r from %s" % (t, peer))
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            conn.close()

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.port))
        srv.listen(64)
        threading.Thread(target=self.monitor_loop, daemon=True).start()
        self.info("listening on :%d, delta=%.0fs, log=%s" % (self.port, STALENESS_DELTA, self.log.path))
        self.info(self.summary_text())
        try:
            while True:
                conn, addr = srv.accept()
                self.pool.submit(self.serve_connection, conn, addr)
        except KeyboardInterrupt:
            self.info("shutting down")
        finally:
            srv.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lot", required=True)
    ap.add_argument("--port", type=int)
    ap.add_argument("--spaces", type=int, default=20)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    port = args.port or LOT_PORTS.get(args.lot)
    if port is None:
        sys.exit("unknown lot %s, use --port" % args.lot)
    LotServer(args.lot, port, args.spaces, args.workers).run()


if __name__ == "__main__":
    main()
