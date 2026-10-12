#!/usr/bin/env python3
"""
Lot server - one process per parking lot.

    python lot_server.py --lot A
    python lot_server.py --lot B

Holds the state of every space in its lot. It is the only process that may
change that state, which is what stops two drivers from getting the same space.

How it talks to everyone else (Milestone 2):

    sensor node  -> broker -> us      pub-sub: we SUBSCRIBE to lot/<lot>/sensor and
                                      get the sensor's events and heartbeats as EVENTs
    client       -> us                request-reply: find / reserve / release on our port
    us           -> broker            pub-sub: we PUBLISH every space state change to
                                      lot/<lot>/status so anyone can watch the lot
    us          <-> other lot servers group: we JOIN the lot_servers group at the registry
                                      and send our summary to the group every few seconds;
                                      members receive it as GROUP_MSG on their port

Every message is written to logs/lot_server_<lot>.log, and on restart the server
replays that log to rebuild its table (crash-recovery).
"""

import argparse
import os
import sys
import threading
import time

from comm import group, pubsub, request_reply
from comm.request_reply import BadRequest
from common import (BROKER_PORT, LOT_PORTS, LOT_SERVER_GROUP, PEER_STALE_AFTER,
                    PEER_SUMMARY_INTERVAL, REGISTRY_PORT, RESERVATION_LEASE, STALENESS_DELTA,
                    MessageLog, now, sensor_topic, status_topic, ts_str)


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
    def __init__(self, lot, port, broker, registry, n_spaces, workers):
        self.lot = lot
        self.port = port
        self.address = "127.0.0.1:%d" % port
        self.broker = broker                # (host, port)
        self.registry = registry            # (host, port)
        self.name = "lot_server_" + lot
        self.log = MessageLog(self.name)
        self.lock = threading.Lock()
        self.spaces = {}
        for i in range(1, n_spaces + 1):
            sid = "%s-%02d" % (lot, i)
            self.spaces[sid] = Space(sid)
        self.peer_summaries = {}            # lot -> (summary dict, time received)
        self.known_peers = set()
        self.last_sensor_seq = -1
        self.stopping = False

        self.service = request_reply.Service(self.name, port, self.log, workers=workers)
        self.service.register("find", self.op_find)
        self.service.register("reserve", self.op_reserve)
        self.service.register("release", self.op_release)
        self.service.on_type("GROUP_MSG", self.on_group_msg)

        self.subscriber = pubsub.Subscriber(broker[0], broker[1], self.name, self.log, self.on_sensor_event)
        self.publisher = pubsub.Publisher(broker[0], broker[1], self.name, self.log)

    def info(self, text):
        print("[%s] [%s] %s" % (ts_str(now()), self.name, text), flush=True)

    # ---- crash recovery ----

    def apply_sensor(self, data, t):
        if data.get("type") == "heartbeat":
            for sid, state in data.get("spaces", {}).items():
                if sid in self.spaces:
                    self.spaces[sid].sensor_state = state
                    self.spaces[sid].sensor_ts = t
        elif data.get("type") == "event":
            sp = self.spaces.get(data.get("space"))
            if sp:
                sp.sensor_state = data["state"]
                sp.sensor_ts = t

    def replay_log(self):
        """Go through our own log and redo everything that changed state:
        sensor EVENTs we received and reservations/releases we granted."""
        n = 0
        for entry in self.log.replay():
            msg = entry.get("msg", {})
            t = entry.get("ts", 0)
            p = msg.get("payload", {}) if isinstance(msg, dict) else {}
            if entry["dir"] == "recv" and msg.get("type") == "EVENT" and p.get("topic") == sensor_topic(self.lot):
                self.apply_sensor(p.get("data", {}), t)
                n += 1
            elif entry["dir"] == "send" and msg.get("type") == "REPLY" and p.get("status") == "OK":
                r = p.get("result", {})
                sp = self.spaces.get(r.get("space"))
                if sp and r.get("op") == "reserve" and r.get("granted"):
                    sp.reserved_by = r.get("client")
                    sp.lease_end = t + RESERVATION_LEASE
                    n += 1
                elif sp and r.get("op") == "release" and r.get("granted"):
                    sp.reserved_by = None
                    sp.lease_end = 0.0
                    n += 1
        if n:
            self.info("RECOVERY: replayed %d entries from %s" % (n, self.log.path))
            self.info("RECOVERY: " + self.summary_text())

    # ---- sensor (pub-sub subscriber) ----

    def on_sensor_event(self, topic, data, msg):
        with self.lock:
            self.apply_sensor(data, now())
            if data.get("type") == "event":
                self.info("EVENT  %s -> %s (seq %s)" % (data["space"], data["state"], data.get("seq")))
            # gaps in the sequence numbers = the sensor dropped something.
            # the next heartbeat carries the full table so it repairs itself.
            seq = data.get("seq", -1)
            if self.last_sensor_seq >= 0 and seq > self.last_sensor_seq + 1:
                self.info("OMISSION: sensor seq jumped %d -> %d" % (self.last_sensor_seq, seq))
            self.last_sensor_seq = seq

    # ---- clients (request-reply handlers) ----

    def op_find(self, args, req):
        t = now()
        with self.lock:
            free = [sp.to_dict(t) for sp in self.spaces.values() if sp.state(t) == "free"]
            result = {
                "op": "find", "lot": self.lot,
                "free": free,
                "lot_summary": self.summary(t),
                "other_lots": self.peer_view(t),
            }
        self.info("FIND from %s: %d free" % (args.get("client", "?"), len(free)))
        return result

    def _space_and_client(self, args):
        client = args.get("client")
        sid = args.get("space")
        if not client:
            raise BadRequest("missing client")
        if not sid:
            raise BadRequest("missing space")
        sp = self.spaces.get(sid)
        if sp is None:
            raise BadRequest("no such space in lot %s: %s" % (self.lot, sid))
        return sp, client

    def op_reserve(self, args, req):
        sp, client = self._space_and_client(args)
        t = now()
        with self.lock:
            st = sp.state(t)
            result = {"op": "reserve", "lot": self.lot, "space": sp.sid, "client": client}
            if st == "reserved" and sp.reserved_by == client:
                result.update(granted=True, reason="already held by you", lease=RESERVATION_LEASE)
            elif st == "free":
                sp.reserved_by = client
                sp.lease_end = t + RESERVATION_LEASE
                result.update(granted=True, reason="reserved", lease=RESERVATION_LEASE)
                self.info("RESERVE %s by %s: OK (%.0fs lease)" % (sp.sid, client, RESERVATION_LEASE))
            else:
                result.update(granted=False, reason=st)
                self.info("RESERVE %s by %s: DENIED (%s)" % (sp.sid, client, st))
        return result

    def op_release(self, args, req):
        sp, client = self._space_and_client(args)
        t = now()
        with self.lock:
            st = sp.state(t)
            result = {"op": "release", "lot": self.lot, "space": sp.sid, "client": client}
            if st == "reserved" and sp.reserved_by == client:
                sp.reserved_by = None
                sp.lease_end = 0.0
                result.update(granted=True, reason="released")
                self.info("RELEASE %s by %s: OK" % (sp.sid, client))
            else:
                result.update(granted=False, reason="not reserved by you" if st == "reserved" else st)
                self.info("RELEASE %s by %s: DENIED" % (sp.sid, client))
        return result

    # ---- other lot servers (group communication) ----

    def on_group_msg(self, msg, conn):
        data = msg["payload"].get("data", {})
        if data.get("kind") != "summary":
            return
        with self.lock:
            self.peer_summaries[data["lot"]] = (data, now())
            self.known_peers.add(data["lot"])

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
        return out

    def join_group(self):
        while not self.stopping:
            try:
                ack = group.join(self.registry[0], self.registry[1], LOT_SERVER_GROUP, self.address, self.name, self.log)
                self.info("JOIN %s at registry: members now %s" % (LOT_SERVER_GROUP, ack.get("members")))
                return
            except OSError:
                self.info("registry unreachable, retrying JOIN in 2s")
                time.sleep(2.0)

    def gossip_loop(self):
        """Background thread: send our summary to the whole group every few seconds."""
        while not self.stopping:
            time.sleep(PEER_SUMMARY_INTERVAL)
            with self.lock:
                summ = self.summary()
            data = {"kind": "summary", "lot": self.lot, "ts": round(now(), 3)}
            data.update(summ)
            delivered, failed = group.group_send(self.registry[0], self.registry[1], LOT_SERVER_GROUP,
                                                 data, self.name, self.log, exclude=self.address)
            for addr in failed:
                self.info("PEER %s unreachable, still serving own lot" % addr)

    # ---- status publishing ----

    def publish_change(self, sp, st):
        data = {"space": sp.sid, "state": st, "reserved_by": sp.reserved_by if st == "reserved" else None,
                "publisher": self.name}
        try:
            self.publisher.publish(status_topic(self.lot), data)
        except OSError:
            pass    # broker down; the subscriber side will re-sync when it's back

    # ---- summaries and the monitor thread ----

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
        """Background thread: notice state changes (from the sensor, from
        reservations, from time passing) and publish them. Also prints the
        ones caused by time so they show up in the demo."""
        last = {sid: sp.state() for sid, sp in self.spaces.items()}
        last_peer_stale = {}
        while not self.stopping:
            time.sleep(1.0)
            changed = []
            with self.lock:
                t = now()
                for sid, sp in self.spaces.items():
                    st = sp.state(t)
                    if st != last[sid]:
                        if st == "unknown":
                            self.info("STALE  %s: nothing from sensor for >%.0fs -> unknown" % (sid, STALENESS_DELTA))
                        elif last[sid] == "reserved" and sp.reserved_by:
                            # still marked reserved but the lease ran out (a release clears reserved_by itself)
                            self.info("LEASE  %s: reservation by %s expired -> %s" % (sid, sp.reserved_by, st))
                            sp.reserved_by = None
                            sp.lease_end = 0.0
                        last[sid] = st
                        changed.append((sp, st))
                pv = self.peer_view(t)
            for lot, v in pv.items():
                if v["stale"] and not last_peer_stale.get(lot):
                    self.info("PEER %s: last summary is %.0fs old -> STALE" % (lot, v["age"]))
                last_peer_stale[lot] = v["stale"]
            for sp, st in changed:
                self.publish_change(sp, st)

    # ---- lifecycle ----

    def run(self):
        self.replay_log()
        self.info("listening on :%d | broker %s:%d | registry %s:%d | delta=%.0fs lease=%.0fs"
                  % (self.port, self.broker[0], self.broker[1], self.registry[0], self.registry[1],
                     STALENESS_DELTA, RESERVATION_LEASE))
        self.info(self.summary_text())
        self.subscriber.start()
        self.subscriber.subscribe(sensor_topic(self.lot))
        threading.Thread(target=self.join_group, daemon=True).start()
        threading.Thread(target=self.gossip_loop, daemon=True).start()
        threading.Thread(target=self.monitor_loop, daemon=True).start()
        try:
            self.service.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.stopping = True
            self.info("shutting down")
            try:
                group.leave(self.registry[0], self.registry[1], LOT_SERVER_GROUP, self.address, self.name, self.log)
            except OSError:
                pass
            self.service.stop()


def hostport(text, default_port):
    if ":" in text:
        h, p = text.split(":")
        return h, int(p)
    return text, default_port


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lot", required=True)
    ap.add_argument("--port", type=int)
    ap.add_argument("--broker", default="127.0.0.1:%d" % BROKER_PORT)
    ap.add_argument("--registry", default="127.0.0.1:%d" % REGISTRY_PORT)
    ap.add_argument("--spaces", type=int, default=20)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--fresh", action="store_true", help="delete the old log, start empty")
    args = ap.parse_args()

    port = args.port or LOT_PORTS.get(args.lot)
    if port is None:
        sys.exit("unknown lot %s, use --port" % args.lot)

    server = LotServer(args.lot, port, hostport(args.broker, BROKER_PORT),
                       hostport(args.registry, REGISTRY_PORT), args.spaces, args.workers)
    if args.fresh:
        try:
            os.remove(server.log.path)
        except FileNotFoundError:
            pass
    server.run()


if __name__ == "__main__":
    main()
