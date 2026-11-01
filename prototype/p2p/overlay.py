"""
Unstructured mesh overlay: the P2P middleware every lot server runs.

The application (lot_server.py) sees three calls:

    peer.put(key, value)        store a value we own and push it to the overlay (gossip)
    peer.get(key)               read from the local store: our own keys plus the
                                replicas we hold for other lots. Never touches the network.
    peer.lookup(key, ttl)       pull: TTL-bounded flood through the neighbors for a key
                                we don't have cached. Returns the value, hops and path.
    peer.aggregate(ttl)         optional: convergecast over a spanning tree rooted here,
                                sums the free/total counts of every lot it can reach.

Under that, the middleware owns:

    neighbors    ad hoc list of up to MAX_NEIGHBORS other peers ("host:port" -> info).
                 There is no key ownership rule and nobody knows the whole system.
    store        key -> {value, version, origin, ts}. Keys from other peers are
                 replicas. A newer version replaces an older one, anything else is
                 ignored, so late or duplicated rumors can't roll state back.
    seen         message ids we already handled, so floods and rumors stop.

Join:    P2P_JOIN to one bootstrap peer -> ACK with a sample of its neighbors and
         its whole store. Then P2P_NEIGHBOR to a few of those -> each ACKs with its
         store too. Nothing else in the system has to change.
Leave:   P2P_LEAVE to each neighbor; they drop us from their list. That's all.
Repair:  every MAINT_INTERVAL ping each neighbor; a peer that doesn't answer is
         removed (dead pointer). Each ping ACK carries a random sample of the other
         side's neighbors; if our list is short we try one of those. That random
         sampling is what keeps the mesh from splitting into islands over time.
Gossip:  put() sends the rumor to every neighbor. A peer that learns something
         new forwards it to each of its other neighbors with probability GOSSIP_PROB.
         P(a node gets no copy) = (1 - p_f)^n for n informed neighbors.
Lookup:  P2P_LOOKUP floods to all neighbors, duplicates are dropped by msg id,
         TTL decrements each hop. Whoever has the key sends P2P_FOUND straight back
         to the asker's address with the path the query took.

All messages go over the same framed-JSON TCP connections as Milestone 2 and
arrive on the lot server's existing port through Service.on_type().
"""

import random
import socket
import threading
import time

from comm import envelope, framing
from comm.framing import ConnectionClosed, ProtocolError
from common import (P2P_GOSSIP_INTERVAL, P2P_GOSSIP_PROB, P2P_LOOKUP_TIMEOUT, P2P_LOOKUP_TTL,
                    P2P_MAINT_INTERVAL, P2P_MAX_NEIGHBORS, P2P_PING_TIMEOUT, P2P_REPLICA_STALE, now)

SEND_TIMEOUT = 1.0
SEEN_LIMIT = 5000


class Peer:
    def __init__(self, node_id, address, service, log, info=print,
                 max_neighbors=P2P_MAX_NEIGHBORS, gossip_prob=P2P_GOSSIP_PROB,
                 gossip_interval=P2P_GOSSIP_INTERVAL, maint_interval=P2P_MAINT_INTERVAL,
                 rng=None):
        self.id = node_id                  # "A", "B", ... (the lot)
        self.address = address             # "host:port" other peers can reach us on
        self.service = service             # the lot server's Service; we hook our types into it
        self.log = log
        self.info = info
        self.max_neighbors = max_neighbors
        self.gossip_prob = gossip_prob
        self.gossip_interval = gossip_interval
        self.maint_interval = maint_interval
        self.rng = rng or random.Random()

        self.lock = threading.Lock()
        self.neighbors = {}                # address -> {"id": ..., "since": t, "last_seen": t}
        self.store = {}                    # key -> {"value", "version", "origin", "ts"}
        self.seen = {}                     # msg_id -> t (floods and rumors we already handled)
        self.pending_lookups = {}          # qid -> {"event", "results": []}
        self.pending_aggs = {}             # qid -> {"event", "replies": {child: partial}}
        self.stats = {"gossip_sent": 0, "gossip_recv": 0, "gossip_fwd": 0, "gossip_stale": 0,
                      "lookup_sent": 0, "lookup_recv": 0, "lookup_dup": 0, "lookup_ttl0": 0,
                      "pings": 0, "ping_failed": 0, "repairs": 0, "anti_entropy": 0, "agg_msgs": 0}
        self.stopping = False

        for t, h in (("P2P_JOIN", self.on_join), ("P2P_NEIGHBOR", self.on_neighbor),
                     ("P2P_LEAVE", self.on_leave), ("P2P_PING", self.on_ping),
                     ("P2P_GOSSIP", self.on_gossip), ("P2P_LOOKUP", self.on_lookup),
                     ("P2P_FOUND", self.on_found), ("P2P_AGG", self.on_agg),
                     ("P2P_AGG_REPLY", self.on_agg_reply)):
            service.on_type(t, h)

    # ------------------------------------------------------------------ wire helpers

    def _send(self, addr, msg_type, payload, want_ack=False, timeout=SEND_TIMEOUT):
        """One connection, one message, optionally wait for the ACK. Returns the ACK
        payload (or True) on success, None if the peer could not be reached."""
        host, port = addr.split(":")
        msg = envelope.make(msg_type, "lot_server_" + self.id, payload)
        try:
            with framing.connect(host, int(port), timeout) as s:
                self.log.record("send", addr, msg)
                framing.send_msg(s, msg)
                if not want_ack:
                    return True
                ack = framing.recv_msg(s)
                self.log.record("recv", addr, ack)
                return ack.get("payload", {})
        except (OSError, ConnectionClosed, ProtocolError, socket.timeout) as e:
            self.log.record("info", addr, {"note": "%s to %s failed: %s" % (msg_type, addr, e), "msg_id": msg["msg_id"]})
            return None

    def _ack(self, conn, msg, payload):
        ack = envelope.make("ACK", "lot_server_" + self.id, payload, reply_to=msg["msg_id"])
        try:
            framing.send_msg(conn, ack)
        except OSError:
            pass

    def _mark_seen(self, msg_id):
        """True if this id is new. Remembers it so a second copy is dropped."""
        with self.lock:
            if msg_id in self.seen:
                return False
            self.seen[msg_id] = now()
            if len(self.seen) > SEEN_LIMIT:
                for k in sorted(self.seen, key=self.seen.get)[: SEEN_LIMIT // 2]:
                    del self.seen[k]
            return True

    # ------------------------------------------------------------------ neighbors

    def neighbor_list(self):
        with self.lock:
            return [{"address": a, "id": n["id"]} for a, n in sorted(self.neighbors.items())]

    def _add_neighbor(self, addr, nid, incoming=False):
        """Link to a peer. The size limit only applies to links we start ourselves;
        a peer that asks to join or be our neighbor is always accepted (soft cap),
        otherwise a newcomer could find every list full and end up isolated."""
        if addr == self.address or not addr:
            return False
        with self.lock:
            if addr in self.neighbors:
                self.neighbors[addr]["last_seen"] = now()
                return True
            if not incoming and len(self.neighbors) >= self.max_neighbors:
                return False
            self.neighbors[addr] = {"id": nid, "since": now(), "last_seen": now()}
        self.info("P2P neighbor + %s (%s), degree now %d" % (nid, addr, self.degree()))
        return True

    def _drop_neighbor(self, addr, why):
        with self.lock:
            n = self.neighbors.pop(addr, None)
        if n:
            self.info("P2P neighbor - %s (%s): %s, degree now %d" % (n["id"], addr, why, self.degree()))
        return n is not None

    def degree(self):
        with self.lock:
            return len(self.neighbors)

    def _sample(self, k, exclude=()):
        with self.lock:
            pool = [{"address": a, "id": n["id"]} for a, n in self.neighbors.items() if a not in exclude]
        self.rng.shuffle(pool)
        return pool[:k]

    def _table(self):
        with self.lock:
            return {k: dict(v) for k, v in self.store.items()}

    def _merge_table(self, table, from_addr, quiet=False):
        n = 0
        for key, entry in (table or {}).items():
            if self._apply(key, entry):
                n += 1
        if n and not quiet:
            self.info("P2P took %d replica(s) from %s" % (n, from_addr))
        return n

    # ------------------------------------------------------------------ join / leave

    def join(self, bootstrap):
        """Contact one existing peer, get a few neighbor pointers and its table,
        then say hello to those neighbors. Returns the addresses we connected to."""
        self.info("P2P JOIN via bootstrap %s" % bootstrap)
        ack = self._send(bootstrap, "P2P_JOIN", {"id": self.id, "address": self.address}, want_ack=True)
        if ack is None:
            self.info("P2P bootstrap %s unreachable" % bootstrap)
            return []
        self._add_neighbor(bootstrap, ack.get("id", "?"))
        self._merge_table(ack.get("table"), bootstrap)
        connected = [bootstrap]
        for cand in ack.get("neighbors", []):
            if self.degree() >= self.max_neighbors:
                break
            if cand["address"] in (self.address, bootstrap):
                continue
            a2 = self._send(cand["address"], "P2P_NEIGHBOR", {"id": self.id, "address": self.address}, want_ack=True)
            if a2 and a2.get("accepted"):
                self._add_neighbor(cand["address"], a2.get("id", cand.get("id", "?")))
                self._merge_table(a2.get("table"), cand["address"])
                connected.append(cand["address"])
        self.info("P2P joined: neighbors %s" % [n["id"] for n in self.neighbor_list()])
        return connected

    def leave(self):
        """Graceful leave: stop our own loops (so we don't ping anyone back into
        listing us), tell each neighbor, they drop the pointer. No other node in
        the system has to do anything."""
        self.stopping = True
        for n in self.neighbor_list():
            self._send(n["address"], "P2P_LEAVE", {"id": self.id, "address": self.address})
        self.info("P2P LEAVE sent to %d neighbor(s)" % len(self.neighbor_list()))

    def on_join(self, msg, conn):
        p = msg["payload"]
        sample = self._sample(self.max_neighbors, exclude=(p.get("address"),))
        accepted = self._add_neighbor(p.get("address"), p.get("id"), incoming=True)
        self.info("P2P JOIN from %s (%s): gave %d neighbor pointer(s) and %d table entries"
                  % (p.get("id"), p.get("address"), len(sample), len(self.store)))
        self._ack(conn, msg, {"status": "OK", "id": self.id, "accepted": accepted,
                              "neighbors": sample, "table": self._table()})

    def on_neighbor(self, msg, conn):
        p = msg["payload"]
        accepted = self._add_neighbor(p.get("address"), p.get("id"), incoming=True)
        self._ack(conn, msg, {"status": "OK", "id": self.id, "accepted": accepted,
                              "table": self._table() if accepted else {}})

    def on_leave(self, msg, conn):
        p = msg["payload"]
        self._drop_neighbor(p.get("address"), "left gracefully")

    # ------------------------------------------------------------------ maintenance

    def on_ping(self, msg, conn):
        p = msg["payload"]
        with self.lock:
            if p.get("address") in self.neighbors:
                self.neighbors[p["address"]]["last_seen"] = now()
        # a ping from someone who still lists us is also how we re-learn a neighbor we dropped
        self._add_neighbor(p.get("address"), p.get("id"), incoming=True)
        # the ACK carries a random sample of our neighbors (peer sampling) and our
        # table (anti-entropy): a rumor the probabilistic push missed is picked up here
        self._ack(conn, msg, {"status": "OK", "id": self.id, "degree": self.degree(),
                              "sample": self._sample(2, exclude=(p.get("address"),)),
                              "table": self._table()})

    def maintenance_round(self):
        """Ping every neighbor, drop the dead ones, use the samples to refill."""
        candidates = []
        for n in self.neighbor_list():
            self.stats["pings"] += 1
            ack = self._send(n["address"], "P2P_PING", {"id": self.id, "address": self.address},
                             want_ack=True, timeout=P2P_PING_TIMEOUT)
            if ack is None:
                self.stats["ping_failed"] += 1
                if self._drop_neighbor(n["address"], "no answer to ping, dead pointer removed"):
                    self.stats["repairs"] += 1
            else:
                candidates.extend(ack.get("sample", []))
                n_new = self._merge_table(ack.get("table"), n["address"])
                if n_new:
                    self.stats["anti_entropy"] += n_new
        # random sampling: if we are short of neighbors, try one peer we heard about
        self.rng.shuffle(candidates)
        for cand in candidates:
            if self.degree() >= self.max_neighbors:
                break
            if cand["address"] == self.address or cand["address"] in self.neighbors:
                continue
            ack = self._send(cand["address"], "P2P_NEIGHBOR", {"id": self.id, "address": self.address}, want_ack=True)
            if ack and ack.get("accepted"):
                self._add_neighbor(cand["address"], ack.get("id", cand.get("id", "?")))
                self._merge_table(ack.get("table"), cand["address"])
                self.info("P2P random sampling: linked to %s to keep the mesh connected" % cand.get("id"))

    def _maint_loop(self):
        while not self.stopping:
            time.sleep(self.maint_interval)
            if not self.stopping:
                self.maintenance_round()

    # ------------------------------------------------------------------ store, put, gossip

    def _apply(self, key, entry):
        """Keep an entry only if it is newer than what we have. Returns True if stored."""
        with self.lock:
            cur = self.store.get(key)
            if cur is not None and entry.get("version", 0) <= cur["version"]:
                return False
            self.store[key] = {"value": entry["value"], "version": entry["version"],
                               "origin": entry["origin"], "ts": now()}
            return True

    def put(self, key, value):
        """Store our own value and push it to the neighbors (origin sends to all)."""
        version = int(now() * 1000)
        entry = {"key": key, "value": value, "version": version, "origin": self.id}
        self._apply(key, entry)
        rumor = dict(entry, hop=0, path=[self.id])
        msg_id = envelope.new_id()
        self._mark_seen(msg_id)
        for n in self.neighbor_list():
            self._gossip_to(n["address"], rumor, msg_id)
        return version

    def _gossip_to(self, addr, rumor, msg_id):
        host, port = addr.split(":")
        msg = envelope.make("P2P_GOSSIP", "lot_server_" + self.id, rumor, msg_id=msg_id)
        try:
            with framing.connect(host, int(port), SEND_TIMEOUT) as s:
                self.log.record("send", addr, msg)
                framing.send_msg(s, msg)
            self.stats["gossip_sent"] += 1
        except (OSError, ConnectionClosed) as e:
            self.log.record("info", addr, {"note": "gossip to %s failed: %s" % (addr, e), "msg_id": msg_id})

    def on_gossip(self, msg, conn):
        p = msg["payload"]
        self.stats["gossip_recv"] += 1
        if not self._mark_seen(msg["msg_id"]):
            return                                  # already had this rumor
        with self.lock:
            cur = self.store.get(p["key"])
        if cur is not None and p.get("version", 0) == cur["version"]:
            return                                  # same version, a periodic re-gossip; nothing new
        if not self._apply(p["key"], p):
            self.stats["gossip_stale"] += 1
            self.log.record("info", msg["sender"], {"note": "stale rumor ignored", "key": p["key"],
                                                    "version": p.get("version"), "have": cur["version"],
                                                    "msg_id": msg["msg_id"]})
            return
        # new to us: forward to each other neighbor with probability p_f
        fwd = dict(p, hop=p.get("hop", 0) + 1, path=p.get("path", []) + [self.id])
        for n in self.neighbor_list():
            if n["id"] == p.get("origin") or n["id"] in p.get("path", []):
                continue
            if self.rng.random() < self.gossip_prob:
                self._gossip_to(n["address"], fwd, msg["msg_id"])
                self.stats["gossip_fwd"] += 1

    def get(self, key):
        """Local read only. Returns the entry plus its age, or None."""
        with self.lock:
            e = self.store.get(key)
            if e is None:
                return None
            age = round(now() - e["ts"], 1)
            return dict(e, age=age, stale=age > P2P_REPLICA_STALE)

    def replicas(self):
        """Everything we hold for other peers (what 'replica hosting' means here)."""
        out = {}
        with self.lock:
            for k, e in self.store.items():
                if e["origin"] != self.id:
                    age = round(now() - e["ts"], 1)
                    out[k] = dict(e, age=age, stale=age > P2P_REPLICA_STALE)
        return out

    def _gossip_loop(self):
        """Re-gossip our own keys every interval, so a peer that missed a rumor
        (or just joined) still converges."""
        while not self.stopping:
            time.sleep(self.gossip_interval)
            if self.stopping:
                break
            with self.lock:
                own = [(k, e) for k, e in self.store.items() if e["origin"] == self.id]
            for k, e in own:
                rumor = {"key": k, "value": e["value"], "version": e["version"], "origin": self.id,
                         "hop": 0, "path": [self.id]}
                msg_id = envelope.new_id()
                self._mark_seen(msg_id)
                for n in self.neighbor_list():
                    self._gossip_to(n["address"], rumor, msg_id)

    # ------------------------------------------------------------------ lookup (pull, flood)

    def lookup(self, key, ttl=P2P_LOOKUP_TTL, timeout=P2P_LOOKUP_TIMEOUT, use_cache=True, fresh=False):
        """Find a key. Local store first (0 hops), otherwise flood the overlay.
        use_cache=False skips the local store so the flood can be observed.
        fresh=True means only the key's origin may answer (a read from the owner,
        not from a replica), so the hop count is the real overlay distance.
        Returns {"found", "value", "version", "origin", "hops", "path", "messages"} ."""
        local = self.get(key) if use_cache else None
        if local is not None and fresh and local["origin"] != self.id:
            local = None
        if local is not None:
            return {"found": True, "value": local["value"], "version": local["version"],
                    "origin": local["origin"], "hops": 0, "path": [self.id], "messages": 0, "from_cache": True}
        qid = envelope.new_id()
        ev = threading.Event()
        with self.lock:
            self.pending_lookups[qid] = {"event": ev, "results": []}
        self._mark_seen(qid)
        q = {"qid": qid, "key": key, "ttl": ttl, "asker": self.address, "path": [self.id], "fresh": fresh}
        sent = 0
        for n in self.neighbor_list():
            if self._send(n["address"], "P2P_LOOKUP", q):
                sent += 1
                self.stats["lookup_sent"] += 1
        self.info("P2P LOOKUP %s: %s, flooding %d neighbor(s) with TTL %d (qid %s)" % (key, "owner only" if fresh else "not cached", sent, ttl, qid))
        ev.wait(timeout)
        with self.lock:
            results = self.pending_lookups.pop(qid, {}).get("results", [])
        if not results:
            self.info("P2P LOOKUP %s: no answer within %.1fs" % (key, timeout))
            return {"found": False, "key": key, "hops": None, "path": None, "messages": sent}
        best = sorted(results, key=lambda r: (r["hops"], -r["version"]))[0]
        self.info("P2P LOOKUP %s: found at %s after %d hop(s) via %s" % (key, best["holder"], best["hops"], "->".join(best["path"])))
        self._apply(key, best)                      # cache it: next time it's a local hit
        return dict(best, found=True, messages=sent, from_cache=False)

    def on_lookup(self, msg, conn):
        p = msg["payload"]
        self.stats["lookup_recv"] += 1
        if not self._mark_seen(p["qid"]):
            self.stats["lookup_dup"] += 1
            self.log.record("info", msg["sender"], {"note": "duplicate lookup dropped", "qid": p["qid"]})
            return
        path = p.get("path", []) + [self.id]
        local = self.get(p["key"])
        if local is not None and p.get("fresh") and local["origin"] != self.id:
            local = None                            # a replica may not answer a fresh read
        if local is not None:
            self._send(p["asker"], "P2P_FOUND", {"qid": p["qid"], "key": p["key"], "value": local["value"],
                                                  "version": local["version"], "origin": local["origin"],
                                                  "holder": self.id, "hops": len(path) - 1, "path": path})
            return
        ttl = p.get("ttl", 1) - 1
        if ttl <= 0:
            self.stats["lookup_ttl0"] += 1
            self.log.record("info", msg["sender"], {"note": "lookup TTL expired", "qid": p["qid"], "key": p["key"]})
            return
        fwd = dict(p, ttl=ttl, path=path)
        for n in self.neighbor_list():
            if n["id"] in path:
                continue
            if self._send(n["address"], "P2P_LOOKUP", fwd):
                self.stats["lookup_sent"] += 1

    def on_found(self, msg, conn):
        p = msg["payload"]
        with self.lock:
            pend = self.pending_lookups.get(p["qid"])
            if pend:
                pend["results"].append(p)
                pend["event"].set()

    # ------------------------------------------------------------------ convergecast (optional)

    def aggregate(self, ttl=P2P_LOOKUP_TTL, timeout=P2P_LOOKUP_TIMEOUT):
        """Root of a convergecast: flood P2P_AGG, each node replies to the peer it
        first heard from with its own numbers plus its children's. Values are the
        'free' and 'total' counts of each lot's summary in the store (own key only,
        so each lot is counted once at its origin)."""
        qid = envelope.new_id()
        self._mark_seen(qid)
        children = [n["address"] for n in self.neighbor_list()]
        ev = threading.Event()
        with self.lock:
            self.pending_aggs[qid] = {"event": ev, "expect": set(children), "replies": {}}
        for addr in children:
            if self._send(addr, "P2P_AGG", {"qid": qid, "root": self.address, "parent": self.address, "ttl": ttl}):
                self.stats["agg_msgs"] += 1
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                if self.pending_aggs[qid]["expect"] <= set(self.pending_aggs[qid]["replies"]):
                    break
            ev.wait(0.1)
            ev.clear()
        with self.lock:
            replies = self.pending_aggs.pop(qid)["replies"]
        return self._combine(self._own_partial(), replies.values(), qid)

    def _own_partial(self):
        with self.lock:
            own = [e["value"] for k, e in self.store.items() if e["origin"] == self.id and k.endswith("/summary")]
        v = own[0] if own else {}
        return {"lots": [self.id], "free": v.get("free", 0), "total": v.get("total", 0), "messages": 0}

    def _combine(self, mine, parts, qid):
        out = dict(mine)
        for p in parts:
            if not p or p.get("dup"):
                continue
            out["lots"] = out["lots"] + p.get("lots", [])
            out["free"] += p.get("free", 0)
            out["total"] += p.get("total", 0)
            out["messages"] += p.get("messages", 0) + 1
        out["qid"] = qid
        return out

    def on_agg(self, msg, conn):
        p = msg["payload"]
        if not self._mark_seen(p["qid"]):
            self._send(p["parent"], "P2P_AGG_REPLY", {"qid": p["qid"], "child": self.address, "dup": True})
            return
        threading.Thread(target=self._agg_subtree, args=(p,), daemon=True).start()

    def _agg_subtree(self, p):
        ttl = p.get("ttl", 1) - 1
        children = [n["address"] for n in self.neighbor_list() if n["address"] != p["parent"]] if ttl > 0 else []
        ev = threading.Event()
        with self.lock:
            self.pending_aggs[p["qid"]] = {"event": ev, "expect": set(children), "replies": {}}
        for addr in children:
            if self._send(addr, "P2P_AGG", {"qid": p["qid"], "root": p["root"], "parent": self.address, "ttl": ttl}):
                self.stats["agg_msgs"] += 1
        deadline = time.time() + max(0.3, 0.6 * ttl)
        while time.time() < deadline:
            with self.lock:
                if self.pending_aggs[p["qid"]]["expect"] <= set(self.pending_aggs[p["qid"]]["replies"]):
                    break
            ev.wait(0.1)
            ev.clear()
        with self.lock:
            replies = self.pending_aggs.pop(p["qid"])["replies"]
        partial = self._combine(self._own_partial(), replies.values(), p["qid"])
        partial["child"] = self.address
        self._send(p["parent"], "P2P_AGG_REPLY", partial)

    def on_agg_reply(self, msg, conn):
        p = msg["payload"]
        with self.lock:
            pend = self.pending_aggs.get(p["qid"])
            if pend:
                pend["replies"][p.get("child")] = p
                pend["event"].set()

    # ------------------------------------------------------------------ lifecycle / metrics

    def start(self):
        threading.Thread(target=self._gossip_loop, daemon=True).start()
        threading.Thread(target=self._maint_loop, daemon=True).start()

    def stop(self):
        self.stopping = True

    def metrics(self):
        with self.lock:
            own = sum(1 for e in self.store.values() if e["origin"] == self.id)
            return {
                "id": self.id, "address": self.address, "degree": len(self.neighbors),
                "max_neighbors": self.max_neighbors, "gossip_prob": self.gossip_prob,
                "neighbors": [{"address": a, "id": n["id"], "last_seen_age": round(now() - n["last_seen"], 1)}
                              for a, n in sorted(self.neighbors.items())],
                "keys_own": own, "keys_replica": len(self.store) - own,
                "stats": dict(self.stats),
            }
