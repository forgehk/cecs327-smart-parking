#!/usr/bin/env python3
"""
Lot server - one process per parking lot.

    python lot_server.py --lot A
    python lot_server.py --lot B

Holds the state of every space in its lot, takes updates from the lot's sensor
node and answers client queries. Everything comes in on one TCP port as JSON lines.

The two lot servers are also peers: every few seconds each one sends the other a
summary of its lot ("7 free, 2 unknown"), so a client talking to lot A can be told
that lot B has room. The summary is only a hint - lot B's server is still the
only one that can reserve a space in B.
"""

import argparse
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common import (LOT_PORTS, PEER_STALE_AFTER, PEER_SUMMARY_INTERVAL, RESERVATION_LEASE,
                    STALENESS_DELTA, MessageLog, now, peer_name, read_lines, send_json,
                    ts_str)


class Space:
    def __init__(self, sid):
        self.sid = sid
        self.sensor_state = "unknown"    # last thing the sensor told us
        self.sensor_ts = 0.0             # when it told us
        self.reserved_by = None          # client id holding the reservation
        self.lease_end = 0.0             # reservation expires at this time

    def state(self, t=None):
        """What we actually report. A live reservation wins; otherwise if the
        sensor has been quiet for longer than STALENESS_DELTA we don't trust
        the old value any more."""
        t = t or now()
        if self.reserved_by and t < self.lease_end:
            return "reserved"
        if t - self.sensor_ts > STALENESS_DELTA:
            return "unknown"
        return self.sensor_state

    def to_dict(self, t=None):
        t = t or now()
        st = self.state(t)
        return {
            "id": self.sid,
            "state": st,
            "age": round(t - self.sensor_ts, 1) if self.sensor_ts else None,
            "reserved_by": self.reserved_by if st == "reserved" else None,
            "lease_left": round(self.lease_end - t, 1) if st == "reserved" else None,
        }


class LotServer:
    def __init__(self, lot, port, peers, n_spaces, workers):
        self.lot = lot
        self.port = port
        self.peers = peers                  # {"B": ("127.0.0.1", 5002)}
        self.peer_summaries = {}            # lot -> (summary msg, time received)
        self.stopping = False
        self.name = "lot_server_" + lot
        self.log = MessageLog(self.name)
        self.lock = threading.Lock()
        self.spaces = {}
        for i in range(1, n_spaces + 1):
            sid = "%s-%02d" % (lot, i)
            self.spaces[sid] = Space(sid)
        self.pool = ThreadPoolExecutor(max_workers=workers)
        # replies we already sent, keyed by req_id. If the client's reply got
        # lost and it retries, we answer the same thing instead of doing it twice.
        self.seen_requests = {}

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
        req_id = msg.get("req_id")
        with self.lock:
            if req_id in self.seen_requests:
                reply = dict(self.seen_requests[req_id])
                reply["duplicate"] = True
                self.info("DUPLICATE req %s -> resending cached reply" % req_id)
            else:
                reply = self.process_client(msg)
                if req_id:
                    self.seen_requests[req_id] = reply
        self.log.record("send", peer, reply)
        send_json(conn, reply)

    def process_client(self, msg):
        op = msg.get("type")
        client = msg.get("client", "?")
        sid = msg.get("space")
        t = now()
        reply = {
            "type": "reply",
            "op": op,
            "req_id": msg.get("req_id"),
            "client": client,
            "lot": self.lot,
            "space": sid,
            "ts": round(t, 3),
        }

        if op == "find":
            free = [sp.to_dict(t) for sp in self.spaces.values() if sp.state(t) == "free"]
            reply["status"] = "OK"
            reply["free"] = free
            reply["lot_summary"] = self.summary(t)
            reply["other_lots"] = self.peer_view(t)
            self.info("FIND from %s: %d free" % (client, len(free)))
            return reply

        sp = self.spaces.get(sid)
        if sp is None:
            reply["status"] = "DENIED"
            reply["reason"] = "no such space in this lot"
            return reply
        st = sp.state(t)

        if op == "reserve":
            if st == "reserved" and sp.reserved_by == client:
                reply["status"] = "OK"
                reply["reason"] = "already held by you"
                reply["lease"] = RESERVATION_LEASE
            elif st == "free":
                sp.reserved_by = client
                sp.lease_end = t + RESERVATION_LEASE
                reply["status"] = "OK"
                reply["reason"] = "reserved"
                reply["lease"] = RESERVATION_LEASE
                self.info("RESERVE %s by %s: OK (%.0fs lease)" % (sid, client, RESERVATION_LEASE))
            else:
                reply["status"] = "DENIED"
                reply["reason"] = st
                self.info("RESERVE %s by %s: DENIED (%s)" % (sid, client, st))
        elif op == "release":
            if st == "reserved" and sp.reserved_by == client:
                sp.reserved_by = None
                sp.lease_end = 0.0
                reply["status"] = "OK"
                reply["reason"] = "released"
                self.info("RELEASE %s by %s: OK" % (sid, client))
            else:
                reply["status"] = "DENIED"
                reply["reason"] = "not reserved by you" if st == "reserved" else st
                self.info("RELEASE %s by %s: DENIED" % (sid, client))
        else:
            reply["status"] = "DENIED"
            reply["reason"] = "unknown op %s" % op
        return reply

    # ---- peer messages ----

    def handle_peer(self, msg, conn, peer):
        with self.lock:
            self.peer_summaries[msg["lot"]] = (msg, now())
        ack = {"type": "summary_ack", "lot": self.lot, "ts": round(now(), 3)}
        self.log.record("send", peer, ack)
        try:
            send_json(conn, ack)
        except OSError:
            pass

    def peer_view(self, t=None):
        """What we currently believe about the other lots, with how old it is."""
        t = t or now()
        out = {}
        for lot, (summ, received) in self.peer_summaries.items():
            age = round(t - received, 1)
            out[lot] = {
                "free": summ["free"],
                "unknown": summ["unknown"],
                "age": age,
                "stale": age > PEER_STALE_AFTER,
            }
        for lot in self.peers:
            if lot not in out:
                out[lot] = {"free": None, "unknown": None, "age": None, "stale": True}
        return out

    def peer_gossip_loop(self):
        """Background thread: send our summary to every peer every few seconds."""
        while not self.stopping:
            time.sleep(PEER_SUMMARY_INTERVAL)
            with self.lock:
                summ = self.summary()
            msg = {"type": "summary", "lot": self.lot, "ts": round(now(), 3)}
            msg.update(summ)
            for lot, (host, port) in self.peers.items():
                target = "%s:%d" % (host, port)
                try:
                    with socket.create_connection((host, port), timeout=1.0) as s:
                        self.log.record("send", target, msg)
                        send_json(s, msg)
                        s.settimeout(1.0)
                        for ack in read_lines(s):
                            self.log.record("recv", target, ack)
                            break
                except OSError:
                    self.info("PEER %s unreachable, still serving own lot" % lot)

    def summary(self, t=None):
        t = t or now()
        counts = {"free": 0, "occupied": 0, "reserved": 0, "unknown": 0}
        for sp in self.spaces.values():
            counts[sp.state(t)] += 1
        counts["total"] = len(self.spaces)
        return counts

    def summary_text(self):
        s = self.summary()
        return "Lot %s: %d free, %d occupied, %d reserved, %d unknown" % (
            self.lot, s["free"], s["occupied"], s["reserved"], s["unknown"])

    def monitor_loop(self):
        """Background thread: print state changes caused by time passing
        (staleness, lease expiry) so they show up in the demo."""
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
                        elif last[sid] == "reserved":
                            self.info("LEASE  %s: reservation by %s expired -> %s"
                                      % (sid, sp.reserved_by, st))
                            sp.reserved_by = None
                            sp.lease_end = 0.0
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
                elif t in ("find", "reserve", "release"):
                    self.handle_client(msg, conn, peer)
                elif t == "summary":
                    self.handle_peer(msg, conn, peer)
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
        threading.Thread(target=self.peer_gossip_loop, daemon=True).start()
        self.info("listening on :%d, peers=%s, delta=%.0fs, lease=%.0fs, log=%s"
                  % (self.port, list(self.peers), STALENESS_DELTA, RESERVATION_LEASE, self.log.path))
        self.info(self.summary_text())
        try:
            while True:
                conn, addr = srv.accept()
                self.pool.submit(self.serve_connection, conn, addr)
        except KeyboardInterrupt:
            self.info("shutting down")
        finally:
            self.stopping = True
            srv.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lot", required=True)
    ap.add_argument("--port", type=int)
    ap.add_argument("--peer", action="append", default=[],
                    help="LOT=HOST:PORT, can repeat. Default: every other lot in LOT_PORTS")
    ap.add_argument("--spaces", type=int, default=20)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    port = args.port or LOT_PORTS.get(args.lot)
    if port is None:
        sys.exit("unknown lot %s, use --port" % args.lot)

    peers = {}
    if args.peer:
        for p in args.peer:
            lot, hostport = p.split("=")
            host, pport = hostport.split(":")
            peers[lot] = (host, int(pport))
    else:
        for lot, pt in LOT_PORTS.items():
            if lot != args.lot:
                peers[lot] = ("127.0.0.1", pt)

    LotServer(args.lot, port, peers, args.spaces, args.workers).run()


if __name__ == "__main__":
    main()
