"""
Centralized mutual exclusion with the elected leader as coordinator (Milestone 4).

Every lot server runs both halves; only the current leader (coord/election.py)
uses the coordinator half.

Requester (any lot server)
    acquire(resource)   take a Lamport timestamp ts for the request, send
                        MUTEX_REQUEST {resource, req_id, ts} to the leader. The ACK
                        says granted now, or queued. If queued, wait for MUTEX_GRANT.
    release(resource)   MUTEX_RELEASE to the leader; it grants the next in line.

Coordinator (the leader)
    holder[resource]    who is inside the critical section right now
    queue[resource]     everyone waiting, ordered by (request ts, node id): the
                        Lamport order of the requests, not the order they arrived
    A request is granted at once only if nobody holds the resource and nobody waits.

So at most one lot server holds a resource at a time: there is one coordinator,
it hands out one grant per resource and the next one only after a release.

When the coordinator changes
    The leader fails -> followers elect a new one (Bully). Before the new leader
    coordinates it asks every node MUTEX_STATE: "what do you hold, what are you
    waiting for (with your original ts)?" and rebuilds holder and queue from the
    answers. Requests that arrive meanwhile are answered RECOVERING and retried.
    It only asks once every live node has switched to it (Election.on_elected),
    and a requester only accepts a MUTEX_GRANT from the node it currently believes
    is the leader, so a grant from a coordinator that was just replaced is refused
    instead of creating a second holder.
    A waiting requester re-sends its request (same req_id, same ts) to the new
    leader, which is harmless if the state query already queued it.

When the holder fails
    A holder that hasn't released after MUTEX_HOLD_LIMIT is presumed crashed: the
    coordinator takes the lock back and grants the next in line. The critical
    sections here last well under a second, so the limit is far above any real one.

Each node sends at most one request per resource at a time; local threads that
want the same resource queue on a local lock first.
"""

import threading
import time

from comm import envelope
from common import MUTEX_HOLD_LIMIT, MUTEX_RETRY, now
from coord import wire


class Mutex:
    def __init__(self, node_id, address, election, service, log, info=print,
                 hold_limit=MUTEX_HOLD_LIMIT, retry=MUTEX_RETRY):
        self.id = node_id
        self.address = address
        self.election = election
        self.log = log
        self.sender = log.process
        self.info = info
        self.hold_limit = hold_limit
        self.retry = retry

        # requester side
        self.req_lock = threading.Lock()     # guards pending and holding (grant, state report)
        self.pending = {}                    # resource -> {"req_id", "ts", "event", "since"}
        self.holding = {}                    # resource -> {"req_id", "ts", "coordinator", "since"}
        self.local = {}                      # resource -> threading.Lock, one request per node at a time
        self.local_guard = threading.Lock()

        # coordinator side (used only while we are the leader)
        self.coord_lock = threading.Lock()
        self.coordinating = False            # leader and state rebuilt: we answer requests
        self.holder = {}                     # resource -> {"node", "address", "req_id", "ts", "since"}
        self.queue = {}                      # resource -> [{"node", "address", "req_id", "ts"}], sorted
        self.stopping = False
        self.stats = {"requests": 0, "granted_now": 0, "queued": 0, "granted_later": 0,
                      "released": 0, "revoked": 0, "stale_grants_refused": 0, "rebuilds": 0}

        for t, h in (("MUTEX_REQUEST", self.on_request), ("MUTEX_GRANT", self.on_grant),
                     ("MUTEX_RELEASE", self.on_release), ("MUTEX_STATE", self.on_state)):
            service.on_type(t, h)

    # ================================================================== requester side

    def _local(self, resource):
        with self.local_guard:
            return self.local.setdefault(resource, threading.Lock())

    def acquire(self, resource, timeout):
        """Block until we hold `resource` (True) or `timeout` seconds pass (False)."""
        deadline = time.time() + timeout
        if not self._local(resource).acquire(timeout=timeout):
            return False
        ok = False
        try:
            ok = self._acquire(resource, deadline)
        finally:
            if not ok:
                self._local(resource).release()
        return ok

    def _acquire(self, resource, deadline):
        ts = self.log.clock.tick()          # the request is an event; its Lamport time orders it
        req_id = envelope.new_id()
        ev = threading.Event()
        t0 = time.time()
        with self.req_lock:
            self.pending[resource] = {"req_id": req_id, "ts": ts, "event": ev, "since": t0}
        sent_to = None
        while time.time() < deadline and not self.stopping:
            leader, addr = self.election.current()
            if leader is None:
                time.sleep(self.retry)              # an election is running
                continue
            if leader != sent_to:
                self.info("MUTEX want %s: REQUEST ts=%d -> coordinator %s" % (resource, ts, leader))
                r = wire.send(addr, "MUTEX_REQUEST",
                              {"resource": resource, "node": self.id, "address": self.address,
                               "req_id": req_id, "ts": ts},
                              self.sender, self.log, want_ack=True)
                if r is None:
                    self.info("MUTEX coordinator %s did not answer -> election" % leader)
                    self.election.trigger("coordinator %s did not answer MUTEX_REQUEST" % leader)
                    time.sleep(self.retry)
                    continue
                if r.get("status") != "OK":
                    time.sleep(self.retry)          # NOT_LEADER / RECOVERING: ask again shortly
                    continue
                sent_to = leader
                if r.get("granted"):
                    if self._take(resource, req_id, leader):
                        self.info("MUTEX GRANTED %s by %s at once (ts=%d)" % (resource, leader, ts))
                        return True
                    sent_to = None                  # the leader changed under us: ask the new one
                else:
                    self.info("MUTEX %s busy: queued at %s, position %d, holder %s, waiting %s"
                              % (resource, leader, r.get("position", 0), r.get("holder"), r.get("queue")))
            if ev.wait(self.retry):
                self.info("MUTEX GRANTED %s by %s after %.2fs in the queue (ts=%d)"
                          % (resource, self.holding[resource]["coordinator"], time.time() - t0, ts))
                return True
        with self.req_lock:
            self.pending.pop(resource, None)
        self.info("MUTEX gave up on %s after %.0fs" % (resource, time.time() - t0))
        leader, addr = self.election.current()
        if addr:                                    # drop our place in the queue, best effort
            wire.send(addr, "MUTEX_RELEASE", {"resource": resource, "node": self.id, "req_id": req_id},
                      self.sender, self.log, want_ack=True)
        return False

    def _take(self, resource, req_id, coordinator):
        """pending -> holding, if this grant is for the request we are still waiting
        on AND comes from the node we currently believe is the leader.

        The leader check and the switch to holding happen in one req_lock section,
        and on_state (the new coordinator's question) takes the same lock. A new
        coordinator only asks after we switched leader. So either we took the grant
        before the switch and report it as holding, or the switch came first and the
        old coordinator's grant is refused here."""
        with self.req_lock:
            pend = self.pending.get(resource)
            if not pend or pend["req_id"] != req_id or coordinator != self.election.current()[0]:
                return False
            self.pending.pop(resource)
            self.holding[resource] = {"req_id": req_id, "ts": pend["ts"], "coordinator": coordinator,
                                      "since": time.time()}
            pend["event"].set()
            return True

    def release(self, resource, confirm_for=5.0):
        """Leave the critical section. We stop counting as a holder first, so a new
        coordinator that asks MUTEX_STATE from now on doesn't list us. Then we tell
        the current coordinator, retrying while an election is running."""
        with self.req_lock:
            h = self.holding.pop(resource, None)
        try:
            if h is None:
                return
            deadline = time.time() + confirm_for
            while time.time() < deadline:
                leader, addr = self.election.current()
                if addr:
                    r = wire.send(addr, "MUTEX_RELEASE", {"resource": resource, "node": self.id, "req_id": h["req_id"]},
                                  self.sender, self.log, want_ack=True)
                    if r and r.get("status") == "OK":
                        self.info("MUTEX released %s (held %.2fs)" % (resource, time.time() - h["since"]))
                        return
                time.sleep(self.retry)              # no leader yet, or it is still RECOVERING
            self.info("MUTEX release of %s not confirmed (coordinator changing). A new coordinator learns from "
                      "MUTEX_STATE that we hold nothing; one that listed us earlier takes it back after %.0fs"
                      % (resource, self.hold_limit))
        finally:
            self._local(resource).release()

    def holds(self, resource):
        with self.req_lock:
            return resource in self.holding

    def on_grant(self, msg, conn):
        p = msg["payload"]
        ok = self._take(p.get("resource"), p.get("req_id"), p.get("coordinator"))
        wire.ack(conn, msg, {"status": "OK", "accepted": ok}, self.sender, self.log)
        if not ok:
            self.stats["stale_grants_refused"] += 1
            self.info("MUTEX refused a GRANT for %s from %s (not my coordinator %s, or not waiting)"
                      % (p.get("resource"), p.get("coordinator"), self.election.current()[0]))

    def on_state(self, msg, conn):
        """A new coordinator asks what we hold and what we wait for."""
        with self.req_lock:
            holding = [{"resource": r, "req_id": h["req_id"], "ts": h["ts"]} for r, h in self.holding.items()]
            waiting = [{"resource": r, "req_id": w["req_id"], "ts": w["ts"]} for r, w in self.pending.items()]
        wire.ack(conn, msg, {"status": "OK", "node": self.id, "address": self.address,
                             "holding": holding, "waiting": waiting}, self.sender, self.log)

    # ================================================================== coordinator side

    def _sort(self, resource):
        self.queue[resource].sort(key=lambda r: (r["ts"], r["node"]))

    def _next(self, resource):
        """Caller holds coord_lock. Move the first in line to holder; return it or None."""
        q = self.queue.get(resource)
        if not q:
            return None
        n = q.pop(0)
        self.holder[resource] = dict(n, since=now())
        return n

    def _grant(self, resource, n):
        """Send MUTEX_GRANT to n. If it is refused or n is gone, move on to the next."""
        while n is not None:
            self.info("MUTEX[coord] %s: GRANT -> %s (ts=%d)" % (resource, n["node"], n["ts"]))
            r = wire.send(n["address"], "MUTEX_GRANT",
                          {"resource": resource, "req_id": n["req_id"], "ts": n["ts"], "coordinator": self.id},
                          self.sender, self.log, want_ack=True)
            if r and r.get("accepted"):
                self.stats["granted_later"] += 1
                return
            self.info("MUTEX[coord] %s: %s did not take the grant, next in line" % (resource, n["node"]))
            with self.coord_lock:
                if not self.coordinating:
                    return
                if self.holder.get(resource, {}).get("req_id") == n["req_id"]:
                    self.holder.pop(resource)
                n = self._next(resource) if resource not in self.holder else None

    def on_request(self, msg, conn):
        p = msg["payload"]
        res, node = p.get("resource"), p.get("node")
        with self.coord_lock:
            if not self.coordinating:
                status = "RECOVERING" if self.election.is_leader() else "NOT_LEADER"
                wire.ack(conn, msg, {"status": status, "leader": self.election.current()[0]}, self.sender, self.log)
                return
            self.stats["requests"] += 1
            h = self.holder.get(res)
            q = self.queue.setdefault(res, [])
            req = {"node": node, "address": p.get("address"), "req_id": p.get("req_id"), "ts": p.get("ts", 0)}
            if h and h["req_id"] == req["req_id"]:
                granted, why = True, "already holds it"
            elif any(r["req_id"] == req["req_id"] for r in q):
                granted, why = False, "already queued"
            elif h is None and not q:
                self.holder[res] = dict(req, since=now())
                granted, why = True, "free"
                self.stats["granted_now"] += 1
            else:
                q.append(req)
                self._sort(res)
                granted, why = False, "busy"
                self.stats["queued"] += 1
            holder = self.holder.get(res, {}).get("node")
            waiting = ["%s(ts=%d)" % (r["node"], r["ts"]) for r in q]
            position = next((k + 1 for k, r in enumerate(q) if r["req_id"] == req["req_id"]), 0)
        wire.ack(conn, msg, {"status": "OK", "granted": granted, "holder": holder, "position": position,
                             "queue": waiting}, self.sender, self.log)
        if granted:
            self.info("MUTEX[coord] %s: REQUEST from %s (ts=%d) -> GRANT (%s)" % (res, node, req["ts"], why))
        else:
            self.info("MUTEX[coord] %s: REQUEST from %s (ts=%d) -> queued behind %s, queue %s"
                      % (res, node, req["ts"], holder, waiting))

    def on_release(self, msg, conn):
        p = msg["payload"]
        res = p.get("resource")
        nxt = None
        with self.coord_lock:
            if not self.coordinating:
                status = "RECOVERING" if self.election.is_leader() else "NOT_LEADER"
                wire.ack(conn, msg, {"status": status}, self.sender, self.log)
                return
            h = self.holder.get(res)
            if h and h["req_id"] == p.get("req_id"):
                self.holder.pop(res)
                self.stats["released"] += 1
                nxt = self._next(res)
                what = "RELEASE by %s" % p.get("node")
            else:                                   # a requester that gave up leaves the queue
                q = self.queue.get(res, [])
                self.queue[res] = [r for r in q if r["req_id"] != p.get("req_id")]
                what = "%s withdrew its request" % p.get("node")
        wire.ack(conn, msg, {"status": "OK"}, self.sender, self.log)
        self.info("MUTEX[coord] %s: %s%s" % (res, what, ", next: %s" % nxt["node"] if nxt else ", nobody waiting"))
        if nxt:
            self._grant(res, nxt)

    # ------------------------------------------------------------------ coordinator changes

    def become_coordinator(self):
        """Called on the new leader once every live node follows it: rebuild the
        lock state from what the nodes report, then start answering requests."""
        with self.coord_lock:
            self.coordinating = False
            self.holder, self.queue = {}, {}
        addrs = sorted(set(self.election.members.values()) | {self.address})
        res = wire.send_all(addrs, "MUTEX_STATE", {"coordinator": self.id}, self.sender, self.log, want_ack=True)
        grants = []
        with self.coord_lock:
            if not self.election.is_leader():
                return                              # replaced again while we were asking
            for r in res.values():
                if not r:
                    continue
                for h in r.get("holding", []):
                    cur = self.holder.get(h["resource"])
                    if cur is None or h["ts"] < cur["ts"]:
                        self.holder[h["resource"]] = {"node": r["node"], "address": r["address"],
                                                      "req_id": h["req_id"], "ts": h["ts"], "since": now()}
                for w in r.get("waiting", []):
                    self.queue.setdefault(w["resource"], []).append(
                        {"node": r["node"], "address": r["address"], "req_id": w["req_id"], "ts": w["ts"]})
            for rsc in list(self.queue):
                self._sort(rsc)
                if rsc not in self.holder:
                    n = self._next(rsc)
                    if n:
                        grants.append((rsc, n))
            self.coordinating = True
            self.stats["rebuilds"] += 1
            holders = {k: v["node"] for k, v in self.holder.items()}
            queues = {k: ["%s(ts=%d)" % (x["node"], x["ts"]) for x in v] for k, v in self.queue.items() if v}
        self.info("MUTEX[coord] I coordinate now; state from %d node(s): holders %s, queued %s"
                  % (sum(1 for r in res.values() if r), holders or "{}", queues or "{}"))
        for rsc, n in grants:
            self._grant(rsc, n)

    def on_leader_change(self, old, new):
        """Election.on_change: stop coordinating as soon as someone else leads."""
        if new != self.id:
            with self.coord_lock:
                was = self.coordinating
                self.coordinating = False
                self.holder, self.queue = {}, {}
            if was:
                self.info("MUTEX[coord] stepping down, %s coordinates now" % (new or "nobody yet"))

    def _watch_holders(self):
        """Coordinator: take the lock back from a holder that never released."""
        while not self.stopping:
            time.sleep(1.0)
            revoked = []
            with self.coord_lock:
                if not self.coordinating:
                    continue
                for rsc, h in list(self.holder.items()):
                    if now() - h["since"] > self.hold_limit:
                        self.holder.pop(rsc)
                        self.stats["revoked"] += 1
                        revoked.append((rsc, h, self._next(rsc)))
            for rsc, h, nxt in revoked:
                self.info("MUTEX[coord] %s: %s held it for more than %.0fs, presumed crashed -> lock taken back"
                          % (rsc, h["node"], self.hold_limit))
                if nxt:
                    self._grant(rsc, nxt)

    def status(self):
        with self.coord_lock:
            coord = {"coordinating": self.coordinating,
                     "holders": {k: v["node"] for k, v in self.holder.items()},
                     "queues": {k: [x["node"] for x in v] for k, v in self.queue.items()}}
        with self.req_lock:
            mine = {"holding": sorted(self.holding), "waiting": sorted(self.pending)}
        return dict(coord, **mine, stats=dict(self.stats))

    def start(self):
        threading.Thread(target=self._watch_holders, daemon=True).start()

    def stop(self):
        self.stopping = True
