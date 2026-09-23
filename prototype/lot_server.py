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
from concurrent.futures import ThreadPoolExecutor

from common import (LOT_PORTS, MessageLog, now, peer_name, read_lines,
                    send_json, ts_str)


class Space:
    def __init__(self, sid):
        self.sid = sid
        self.state = "unknown"     # free / occupied / unknown
        self.updated = 0.0

    def to_dict(self):
        return {"id": self.sid, "state": self.state}


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
                        self.spaces[sid].state = state
                        self.spaces[sid].updated = t
            elif msg["type"] == "event":
                sp = self.spaces.get(msg.get("space"))
                if sp:
                    sp.state = msg["state"]
                    sp.updated = t
                    self.info("EVENT  %s -> %s" % (sp.sid, sp.state))

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
        if op == "find":
            free = [sp.to_dict() for sp in self.spaces.values() if sp.state == "free"]
            reply["status"] = "OK"
            reply["free"] = free
            reply["lot_summary"] = self.summary()
            self.info("FIND from %s: %d free" % (client, len(free)))
        else:
            reply["status"] = "DENIED"
            reply["reason"] = "unknown op %s" % op
        return reply

    def summary(self):
        counts = {"free": 0, "occupied": 0, "unknown": 0}
        for sp in self.spaces.values():
            counts[sp.state] += 1
        counts["total"] = len(self.spaces)
        return counts

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
        self.info("listening on :%d, log=%s" % (self.port, self.log.path))
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
