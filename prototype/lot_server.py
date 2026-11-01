#!/usr/bin/env python3
"""
Lot server - one process per parking lot.

    python lot_server.py --lot A
    python lot_server.py --lot B

Holds the state of every space in its lot. It is the only process that may
change that state, which is what stops two drivers from getting the same space.

How it talks to everyone else:

    sensor node  -> broker -> us      pub-sub: we SUBSCRIBE to lot/<lot>/sensor and
                                      get the sensor's events and heartbeats as EVENTs
    client       -> us                request-reply: find / reserve / release / lookup /
                                      peers / total on our port
    us           -> broker            pub-sub: we PUBLISH every space state change to
                                      lot/<lot>/status so anyone can watch the lot
    us          <-> other lot servers P2P overlay (Milestone 3): an unstructured mesh.
                                      We join through one bootstrap peer, keep a short
                                      neighbor list, gossip our summary (put), hold
                                      replicas of the other lots' summaries (get), and
                                      flood a TTL-bounded lookup for anything not cached.
    us           -> registry          only as a directory to find a bootstrap peer when
                                      --bootstrap is not given (the Milestone 2 group).

Software stack on each lot server:

    application   space table, find / reserve / release, summaries
    middleware    p2p.overlay.Peer: put(key, value) / get(key) / lookup(key)
    overlay       neighbor list, join/leave, ping repair, gossip, flood
    transport     comm/: framed JSON over TCP, one Service on one port

Every message is written to logs/lot_server_<lot>.log, and on restart the server
replays that log to rebuild its table (crash-recovery).
"""

import argparse
import os
import signal
import sys
import threading
import time

from comm import group, pubsub, request_reply
from comm.request_reply import BadRequest
from common import (BROKER_PORT, LOT_PORTS, LOT_SERVER_GROUP, P2P_GOSSIP_INTERVAL, P2P_LOOKUP_TTL,
                    REGISTRY_PORT, RESERVATION_LEASE, STALENESS_DELTA,
                    MessageLog, now, sensor_topic, status_topic, ts_str)
from p2p.overlay import Peer


def summary_key(lot):
    return "lot/%s/summary" % lot


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
    def __init__(self, lot, port, broker, registry, n_spaces, workers, bootstrap=None):
        self.lot = lot
        self.port = port
        self.address = "127.0.0.1:%d" % port
        self.broker = broker                # (host, port)
        self.registry = registry            # (host, port) or None
        self.bootstrap = bootstrap          # "host:port" of one existing peer, or None
        self.name = "lot_server_" + lot
        self.log = MessageLog(self.name)
        self.lock = threading.Lock()
        self.spaces = {}
        for i in range(1, n_spaces + 1):
            sid = "%s-%02d" % (lot, i)
            self.spaces[sid] = Space(sid)
        self.last_sensor_seq = -1
        self.stopping = False

        self.service = request_reply.Service(self.name, port, self.log, workers=workers)
        self.service.register("find", self.op_find)
        self.service.register("reserve", self.op_reserve)
        self.service.register("release", self.op_release)
        self.service.register("lookup", self.op_lookup)
        self.service.register("peers", self.op_peers)
        self.service.register("total", self.op_total)

        # the P2P middleware shares our port: its message types are dispatched by the Service
        self.peer = Peer(lot, self.address, self.service, self.log, info=self.info)

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

    # ---- other lot servers (P2P overlay) ----

    def peer_view(self, t=None):
        """What we currently believe about the other lots: the replicas the
        middleware holds for us, with their age and a stale flag. A read, no network."""
        out = {}
        for key, e in self.peer.replicas().items():
            if not key.endswith("/summary"):
                continue
            v = e["value"]
            out[e["origin"]] = {"free": v.get("free"), "unknown": v.get("unknown"),
                                "age": e["age"], "stale": e["stale"], "version": e["version"]}
        return out

    def op_lookup(self, args, req):
        """Pull: find another lot's summary even if we don't have it cached.
        Local store first, otherwise a TTL-bounded flood through the overlay."""
        lot = args.get("lot")
        if not lot:
            raise BadRequest("missing lot")
        ttl = int(args.get("ttl", P2P_LOOKUP_TTL))
        r = self.peer.lookup(summary_key(lot), ttl=ttl, use_cache=not args.get("no_cache", False),
                             fresh=bool(args.get("fresh", False)))
        r["op"] = "lookup"
        r["lot"] = lot
        return r

    def op_peers(self, args, req):
        """The middleware's view: neighbors, replicas, counters. For the metrics tool."""
        m = self.peer.metrics()
        m["op"] = "peers"
        m["replicas"] = {k: {"origin": e["origin"], "age": e["age"], "stale": e["stale"], "free": e["value"].get("free")}
                         for k, e in self.peer.replicas().items()}
        return m

    def op_total(self, args, req):
        """Convergecast: free / total over every lot reachable in the overlay."""
        r = self.peer.aggregate(ttl=int(args.get("ttl", P2P_LOOKUP_TTL)))
        r["op"] = "total"
        self.info("TOTAL across %s: %d free of %d (%d messages up the tree)" % (r["lots"], r["free"], r["total"], r["messages"]))
        return r

    def find_bootstrap(self):
        """--bootstrap wins. Otherwise ask the registry (the Milestone 2 group) who is
        already running and pick the first one that is not us. First node: nobody."""
        if self.bootstrap:
            return self.bootstrap
        if not self.registry:
            return None
        try:
            members = group.members(self.registry[0], self.registry[1], LOT_SERVER_GROUP, self.name, self.log)
        except OSError:
            self.info("registry unreachable, starting as the first peer")
            return None
        others = [m for m in members if m != self.address]
        return others[0] if others else None

    def join_overlay(self):
        """Register in the directory, then join the mesh through one bootstrap peer.
        Retries until a bootstrap answers, so start order does not matter."""
        if self.registry:
            try:
                ack = group.join(self.registry[0], self.registry[1], LOT_SERVER_GROUP, self.address, self.name, self.log)
                self.info("registered at registry: members now %s" % ack.get("members"))
            except OSError:
                self.info("registry unreachable, skipping registration")
        for attempt in range(1, 6):
            boot = self.find_bootstrap()
            if boot is None:
                self.info("P2P first peer in the overlay, waiting for others to join")
                break
            if self.peer.join(boot):
                break
            self.info("P2P bootstrap %s did not answer, retry %d in 2s" % (boot, attempt))
            time.sleep(2.0)
        self.peer.start()

    def gossip_loop(self):
        """Background thread: put our summary into the overlay every few seconds.
        The middleware gossips it; other lots keep it as a replica."""
        while not self.stopping:
            with self.lock:
                summ = self.summary()
            summ["lot"] = self.lot
            summ["ts"] = round(now(), 3)
            self.peer.put(summary_key(self.lot), summ)
            time.sleep(P2P_GOSSIP_INTERVAL)

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
        self.info("listening on :%d | broker %s:%d | registry %s | bootstrap %s | delta=%.0fs lease=%.0fs"
                  % (self.port, self.broker[0], self.broker[1],
                     ("%s:%d" % self.registry) if self.registry else "none", self.bootstrap or "auto",
                     STALENESS_DELTA, RESERVATION_LEASE))
        self.info(self.summary_text())
        self.subscriber.start()
        self.subscriber.subscribe(sensor_topic(self.lot))
        self.join_overlay()
        threading.Thread(target=self.gossip_loop, daemon=True).start()
        threading.Thread(target=self.monitor_loop, daemon=True).start()
        try:
            self.service.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.stopping = True
            self.info("shutting down")
            self.peer.leave()
            self.peer.stop()
            if self.registry:
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
    ap.add_argument("--registry", default="127.0.0.1:%d" % REGISTRY_PORT,
                    help="directory used to find a bootstrap peer; 'none' to run without it")
    ap.add_argument("--bootstrap", help="host:port of one running lot server to join the overlay through")
    ap.add_argument("--spaces", type=int, default=20)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--fresh", action="store_true", help="delete the old log, start empty")
    args = ap.parse_args()

    port = args.port or LOT_PORTS.get(args.lot)
    if port is None:
        sys.exit("unknown lot %s, use --port" % args.lot)

    registry = None if args.registry == "none" else hostport(args.registry, REGISTRY_PORT)
    server = LotServer(args.lot, port, hostport(args.broker, BROKER_PORT),
                       registry, args.spaces, args.workers, bootstrap=args.bootstrap)
    if args.fresh:
        try:
            os.remove(server.log.path)
        except FileNotFoundError:
            pass

    def on_term(signum, frame):      # plain `kill` = graceful leave; `kill -9` = crash
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_term)
    server.run()


if __name__ == "__main__":
    main()
