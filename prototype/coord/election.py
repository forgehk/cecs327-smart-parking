"""
Bully leader election among the lot servers (Milestone 4).

Every node knows the ids of all nodes that may exist (the lots in LOT_PORTS);
some of them may be down. The node with the highest id that is alive is the
leader (coordinator). Ids are the lot letters, so E > D > C > B > A.

    ELECTION      sent to every higher node. A live higher node answers ACK
                  {"alive": true} ("OK, I'll take it from here") and starts its own
                  election. If no higher node answers within ELECTION_ANSWER_TIMEOUT,
                  the sender has won.
    COORDINATOR   the winner tells every other node "I am the leader". If an
                  OK came back but no COORDINATOR arrives within COORDINATOR_TIMEOUT
                  (the higher node died mid-election), the election starts again.
    LEADER_PING   every follower pings the leader every LEADER_CHECK_INTERVAL. No ACK
                  within LEADER_PING_TIMEOUT = the leader has failed -> election.

Why the node with the highest id always wins: it never gets an OK (nobody is
higher), so it always announces itself; every lower node that hears ELECTION
answers and hands the election upward. A node that comes back after a crash
starts an election when it starts, so if it has the highest id it "bullies" its
way back to being leader. A COORDINATOR from a node lower than us is not
accepted; we answer it with an election of our own.

Messages to the nodes above go out in parallel (coord.wire.send_all), so the
nodes that are down cost one timeout in total. Each message carries the
sender's Lamport time like every other message (comm/clock.py).
"""

import threading
import time

from common import (COORDINATOR_TIMEOUT, ELECTION_ANSWER_TIMEOUT, LEADER_CHECK_INTERVAL,
                    LEADER_PING_TIMEOUT)
from coord import wire


class Election:
    def __init__(self, node_id, address, members, service, log, info=print, on_change=None, on_elected=None,
                 answer_timeout=ELECTION_ANSWER_TIMEOUT, coordinator_timeout=COORDINATOR_TIMEOUT,
                 check_interval=LEADER_CHECK_INTERVAL, ping_timeout=LEADER_PING_TIMEOUT):
        self.id = node_id                    # "A", "B", ...: also the Bully priority
        self.address = address               # "host:port" of our Service
        self.members = {i: a for i, a in members.items() if i != node_id}   # every other node that may exist
        self.log = log
        self.sender = log.process            # name in our envelopes, e.g. lot_server_A
        self.info = info
        self.on_change = on_change           # called as on_change(old_leader, new_leader)
        self.on_elected = on_elected         # called on the winner once every live node has ACKed COORDINATOR
        self.answer_timeout = answer_timeout
        self.coordinator_timeout = coordinator_timeout
        self.check_interval = check_interval
        self.ping_timeout = ping_timeout

        self.lock = threading.Lock()
        self.leader = None                   # id of the node we believe is the leader
        self.leader_addr = None
        self.electing = False
        self.got_coordinator = threading.Event()   # a higher node announced itself
        self.stopping = False
        self.stats = {"elections": 0, "won": 0, "coordinator_recv": 0, "leader_failures": 0}

        service.on_type("ELECTION", self.on_election)
        service.on_type("COORDINATOR", self.on_coordinator)
        service.on_type("LEADER_PING", self.on_leader_ping)

    # ------------------------------------------------------------------ state

    def _set_leader(self, lid, addr, why):
        with self.lock:
            old = self.leader
            self.leader, self.leader_addr = lid, addr
        if old != lid:
            self.info("LEADER %s -> %s (%s)" % (old or "none", lid or "none", why))
            if self.on_change:
                self.on_change(old, lid)

    def is_leader(self):
        with self.lock:
            return self.leader == self.id

    def current(self):
        """(leader id, leader address), either may be None during an election."""
        with self.lock:
            return self.leader, self.leader_addr

    def status(self):
        with self.lock:
            return {"id": self.id, "leader": self.leader, "leader_address": self.leader_addr,
                    "electing": self.electing, "members": sorted([self.id] + list(self.members)),
                    "stats": dict(self.stats)}

    # ------------------------------------------------------------------ running an election

    def trigger(self, reason):
        """Start an election in the background, unless one is already running here."""
        with self.lock:
            if self.electing or self.stopping:
                return False
            self.electing = True
            self.stats["elections"] += 1
            n = self.stats["elections"]
        threading.Thread(target=self._run, args=(reason, n), daemon=True).start()
        return True

    def _run(self, reason, n):
        try:
            self.info("ELECTION #%d started: %s" % (n, reason))
            while not self.stopping:
                self.got_coordinator.clear()
                higher = {i: a for i, a in self.members.items() if i > self.id}
                alive = []
                if higher:
                    self.info("ELECTION -> %s (every node with a higher id)" % ", ".join(sorted(higher)))
                    by_addr = {a: i for i, a in higher.items()}
                    res = wire.send_all(list(higher.values()), "ELECTION",
                                        {"id": self.id, "address": self.address, "reason": reason},
                                        self.sender, self.log, want_ack=True, timeout=self.answer_timeout)
                    alive = sorted(by_addr[a] for a, r in res.items() if r and r.get("alive"))
                if not alive:
                    self._win()
                    return
                self.info("ELECTION: %s answered OK, waiting for a COORDINATOR" % ", ".join(alive))
                if self.got_coordinator.wait(self.coordinator_timeout):
                    return                                   # a higher node took over
                self.info("ELECTION: no COORDINATOR within %.0fs, starting again" % self.coordinator_timeout)
        finally:
            with self.lock:
                self.electing = False

    def _win(self):
        with self.lock:
            self.stats["won"] += 1
        self._set_leader(self.id, self.address, "no higher node answered, I am the coordinator")
        others = sorted(self.members)
        self.info("COORDINATOR -> %s" % (", ".join(others) or "(nobody else)"))
        res = wire.send_all([self.members[i] for i in others], "COORDINATOR",
                            {"id": self.id, "address": self.address},
                            self.sender, self.log, want_ack=True, timeout=self.answer_timeout)
        heard = sorted(i for i in others if res.get(self.members[i]))
        self.info("COORDINATOR acknowledged by %s" % (", ".join(heard) or "nobody (alone)"))
        # every node that ACKed has already switched to us (on_coordinator sets the
        # leader before it answers), so nobody still treats the old leader as coordinator
        if self.on_elected and self.is_leader():
            self.on_elected()
        return res

    # ------------------------------------------------------------------ messages from other nodes

    def on_election(self, msg, conn):
        p = msg["payload"]
        wire.ack(conn, msg, {"status": "OK", "alive": True, "id": self.id}, self.sender, self.log)
        if p.get("id", "") < self.id:
            self.info("ELECTION from %s: answered OK, taking over the election" % p.get("id"))
            self.trigger("ELECTION from lower node %s" % p.get("id"))

    def on_coordinator(self, msg, conn):
        p = msg["payload"]
        with self.lock:
            self.stats["coordinator_recv"] += 1
        lid = p.get("id")
        if lid is not None and lid < self.id and not self.stopping:
            # a lower node thinks it won (it didn't hear from us): bully it
            wire.ack(conn, msg, {"status": "OK", "id": self.id}, self.sender, self.log)
            self.info("COORDINATOR from %s, but my id is higher -> new election" % lid)
            self.trigger("lower node %s claimed leadership" % lid)
            return
        if lid is not None:
            # switch first, then ACK: when the winner has all ACKs, every one of us has switched
            self._set_leader(lid, p.get("address"), "COORDINATOR from %s" % lid)
        wire.ack(conn, msg, {"status": "OK", "id": self.id}, self.sender, self.log)
        if lid is not None and lid > self.id:
            self.got_coordinator.set()

    def on_leader_ping(self, msg, conn):
        with self.lock:
            leader = self.leader
        wire.ack(conn, msg, {"status": "OK", "id": self.id, "leader": leader}, self.sender, self.log)

    # ------------------------------------------------------------------ failure detection

    def check_leader(self):
        """One round of failure detection. Called every LEADER_CHECK_INTERVAL."""
        with self.lock:
            leader, addr, electing = self.leader, self.leader_addr, self.electing
        if electing:
            return
        if leader is None:
            self.trigger("no leader known")
            return
        if leader == self.id:
            return
        r = wire.send(addr, "LEADER_PING", {"id": self.id}, self.sender, self.log,
                      want_ack=True, timeout=self.ping_timeout)
        if r is None:
            with self.lock:
                self.stats["leader_failures"] += 1
            self.info("LEADER %s did not answer the ping within %.0fs -> it failed" % (leader, self.ping_timeout))
            with self.lock:
                still = self.leader == leader
            if still:
                self._set_leader(None, None, "leader %s unreachable" % leader)
            self.trigger("leader %s failed" % leader)
        elif r.get("leader") != leader:
            self.trigger("%s no longer says it is the leader" % leader)

    def _loop(self):
        while not self.stopping:
            time.sleep(self.check_interval)
            if not self.stopping:
                self.check_leader()

    def start(self):
        """Begin: an election right away (so a node that restarts with the highest
        id takes the leadership back), then keep watching the leader."""
        self.trigger("starting up")
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self.stopping = True
