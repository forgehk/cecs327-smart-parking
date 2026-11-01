# Milestone 3: peer-to-peer overlay

How the lot servers find each other and share state without a coordinator.
Everything below is what the code in `prototype/p2p/overlay.py` does, with numbers
taken from real runs (`prototype/p2p_metrics.py`, `tests/run_tests.py 8`).

## 1. Overlay topology

Unstructured mesh. Each lot server keeps a short, ad hoc neighbor list. There is
no identifier ring, no finger table, no owner for a key. The overlay runs on top of
the Milestone 2 transport: every link is a TCP connection carrying framed JSON, and
all P2P messages arrive on the lot server's existing port through the same
`Service` that handles client requests.

Default demo (`run_demo.sh`): A starts first, B joins through A, C through B, D
through A. The mesh that comes out of it:

```
    A --- B
    | \   |
    |  \  |
    D --- C
```

| metric | value | how it is measured |
|---|---|---|
| nodes N | 4 | `peers` op on every lot |
| edges | 5: A-B, A-C, A-D, B-C, C-D | union of neighbor lists |
| degree | A=3, B=2, C=3, D=2 (avg 2.5) | `len(neighbors)` per node |
| diameter | 2 hops (B to D) | BFS over the rebuilt graph |
| average path length | 1.17 hops | mean over all pairs |
| lookup cost | avg 1.17 hops, max 2; asker sends 2 or 3 messages | owner-only lookups between every pair, `p2p_metrics.py` |
| maintenance cost | 10 pings per round (one per directed edge) every 5 s = 2 msg/s for the whole overlay; each ping ACK carries a neighbor sample and the peer's table | `P2P_MAINT_INTERVAL`, `maintenance_round()` |
| gossip cost per put | origin sends `degree` copies; each informed peer forwards to its other neighbors with p_f = 0.7 | `put()`, `on_gossip()` |

Why the degree varies: the cap (`P2P_MAX_NEIGHBORS = 2`) applies to links a node opens
itself. A node that is asked to be someone's neighbor always says yes, otherwise a
newcomer could find every list full and end up isolated. So early nodes collect more
links than late ones. A ring would give every node degree 2 and diameter N/2; a full
mesh degree N-1 and diameter 1; this sits in between and grows without anyone
recomputing anything.

Stretch. All our nodes run on one machine, so every direct hop costs the same and
stretch = overlay hops / 1. The measured average is 1.00 for owner-only lookups
(the flood always finds the shortest overlay path first). On a real deployment the
overlay would not know physical distance: B and D are two overlay hops apart through
A, and if A were in another city that path would be slow even though it is "short".
Low hop count does not guarantee low latency.

Why not a tree or a Chord ring for this workload: a tree routes everything through
the root, so one busy campus structure would make every other lot wait, and the root
dying partitions everything. Chord gives O(log N) lookups but needs successor and
finger maintenance on every join, and a broadcast ("lot C is full") has to be built
on top of point-to-point routing. For a handful of lots that mostly broadcast small
state updates, a mesh with gossip is simpler and has no single hot node.

## 2. Peer responsibilities

Each lot server runs the same software stack:

```
application   space table, find / reserve / release, summary every 3 s
middleware    p2p.overlay.Peer:  put(key, value)   get(key)   lookup(key, ttl)   aggregate()
overlay       neighbor list, join / leave, ping repair, gossip, flood, convergecast
transport     comm/: framed JSON over TCP, one Service on one port
```

- `put("lot/A/summary", {...})` stores the value with a version (ms timestamp) and
  pushes it to the neighbors. The application calls this every 3 s.
- `get(key)` reads the local store: own keys plus the replicas held for other lots.
  `find` answers "other lots" from this, no network.
- `lookup(key, ttl)` is the pull path for something not cached: a TTL-bounded flood.
- Every peer forwards queries and rumors for others, pings its neighbors, and holds
  replicas of every lot it has heard about. Replicas are what keeps `find` answering
  for lot C after lot server C has died (the entry is flagged stale after 12 s).

Queueing view. With arrival rate lambda (sensor events plus client requests per
second at one lot) and service rate mu (operations the server completes per
second), the server is stable only while lambda < mu. Replication helps on the read
side: a client asking lot A about lot C is served from A's replica, so the question
never reaches C. During a spike at one lot, its neighbors absorb the "how full is it"
traffic and only reservations still have to go to the owner. There is no hot key:
no single peer is responsible for a popular lot's data the way `owner(k) = succ(k)`
would make it in a DHT; the state is wherever gossip has carried it.

## 3. Join and leave protocol

Join (`Peer.join(bootstrap)`):

1. `P2P_JOIN {id, address}` to one bootstrap peer (from `--bootstrap`, or the first
   other address the Milestone 2 registry lists).
2. The bootstrap adds the newcomer to its list and ACKs with a random sample of its
   own neighbors (up to 2) and its whole table.
3. The newcomer sends `P2P_NEIGHBOR` to those sampled peers until it has 2 links of
   its own; each one ACKs with `accepted` and its table.
4. The newcomer now has neighbors and replicas and starts gossiping. No other node
   heard about the join.

Leave (`Peer.leave()`, run on SIGTERM / Ctrl-C): stop our loops, send `P2P_LEAVE`
to each neighbor, exit. Each neighbor deletes one pointer. Maintenance traffic for a
leave: `degree` messages, nothing else.

Crash: nothing is sent. Within one maintenance round (5 s) each neighbor's
`P2P_PING` fails and the dead pointer is removed.

Random sampling: every ping ACK carries a sample of the other side's neighbors. A
node whose list dropped below the target picks one and sends `P2P_NEIGHBOR`. That is
how a node that lost links to a leaver reconnects to the rest of the mesh instead of
drifting off as an island. In the trace below, after C leaves, B (down to one
neighbor) and E (down to one) find each other through A's sample.

The design equation: good P2P = (fast lookup + availability) / (traffic + maintenance).
Join costs 1 + up to 2 round trips, leave costs `degree` one-way messages, steady
state costs 2 pings per edge per 5 s. Lookups are 1 to 2 hops on this mesh, and
availability comes from replicas, not from any node staying up.

### Trace: five lots, a join, a graceful leave, a crash

From `run_demo.sh` followed by `lot_server.py --lot E --bootstrap 127.0.0.1:5003`,
`kill <C>` (SIGTERM) and `kill -9 <D>`. Console lines, trimmed:

```
12:49:24 A  P2P first peer in the overlay, waiting for others to join
12:49:25 B  P2P JOIN via bootstrap 127.0.0.1:5001
12:49:25 A  P2P JOIN from B: gave 0 neighbor pointer(s) and 1 table entries
12:49:25 B  P2P took 1 replica(s) from 127.0.0.1:5001          <- lot/A/summary
12:49:25 B  P2P joined: neighbors ['A']
12:49:25 C  P2P JOIN via bootstrap 127.0.0.1:5002
12:49:25 B  P2P JOIN from C: gave 1 neighbor pointer(s) and 2 table entries
12:49:25 C  P2P neighbor + B, degree now 1
12:49:25 C  P2P neighbor + A, degree now 2                      <- the pointer B handed out
12:49:25 C  P2P joined: neighbors ['A', 'B']
12:49:26 D  P2P JOIN via bootstrap 127.0.0.1:5001
12:49:26 A  P2P JOIN from D: gave 2 neighbor pointer(s) and 3 table entries
12:49:26 D  P2P took 3 replica(s) from 127.0.0.1:5001
12:49:26 D  P2P joined: neighbors ['A', 'C']
            mesh: A-B A-C A-D B-C C-D   degree A3 B2 C3 D2

12:49:35 E  P2P JOIN via bootstrap 127.0.0.1:5003
12:49:35 C  P2P JOIN from E: gave 2 neighbor pointer(s) and 4 table entries
12:49:35 E  P2P took 4 replica(s) from 127.0.0.1:5003          <- A, B, C, D summaries
12:49:35 E  P2P joined: neighbors ['A', 'C']
            E is serving find for all five lots 1 s after starting

12:49:38 D  P2P LOOKUP lot/B/summary: owner only, flooding 2 neighbor(s) with TTL 3
12:49:38 D  P2P LOOKUP lot/B/summary: found at B after 2 hop(s) via D->A->B

12:49:38 C  P2P LEAVE sent to 4 neighbor(s)
12:49:38 A  P2P neighbor - C: left gracefully, degree now 3
12:49:38 B  P2P neighbor - C: left gracefully, degree now 1
12:49:38 D  P2P neighbor - C: left gracefully, degree now 1
12:49:38 E  P2P neighbor - C: left gracefully, degree now 1
12:49:40 B  P2P random sampling: linked to E to keep the mesh connected
12:49:40 E  P2P random sampling: linked to B to keep the mesh connected

         D  (kill -9, no message sent)
12:49:44 A  P2P neighbor - D: no answer to ping, dead pointer removed, degree now 2
12:49:45 E  P2P neighbor - D: no answer to ping, dead pointer removed
            mesh: A-B A-E B-E   still connected, find still answers for C and D (STALE)
```

`tests/run_tests.py 8` replays this scenario and checks every step.

## 4. Lookup algorithm

Two modes.

Push (continuous): every 3 s each lot `put`s its summary. The origin sends
`P2P_GOSSIP` to all its neighbors. A peer that has not seen that message id and
whose stored version is older stores the value and forwards it to each of its other
neighbors with probability p_f = 0.7. A peer that already has that version drops it
silently; a peer that has a newer version logs "stale rumor ignored". Message ids
stop loops, versions stop rollbacks.

Probability bound: a node with n informed neighbors misses one round with
probability (1 - p_f)^n. On our mesh: A and C (n = 3): 0.3^3 = 0.027, a 97.3 %
chance per round. B and D (n = 2): 0.3^2 = 0.09, 91 %. A miss is repaired by the
next round 3 s later, and by anti-entropy: every ping ACK carries the peer's table,
so within one maintenance round (5 s) any missed value arrives anyway.

Pull (on demand): `lookup(key, ttl)`. The asker floods `P2P_LOOKUP {qid, key, ttl,
asker, path}` to all neighbors. A peer that has seen the qid drops it (counted as
`lookup_dup`). A peer that has the key sends `P2P_FOUND` straight back to the asker's
address with the path. Otherwise it decrements the TTL and forwards to its other
neighbors; at TTL 0 it stops (counted as `lookup_ttl0`). The asker takes the first
answer, caches it, and the next lookup is 0 hops. Flooding cost grows like
d(d-1)^(k-1) per step k for degree d; on this mesh with TTL 3 a lookup costs 2 to 3
messages from the asker and a few more forwards, and the duplicates are visible in
the counters (`lookups recv 19 dup 10` on A after 12 lookups).

Measured (owner-only lookups, cache bypassed, every ordered pair of A..D):
12 lookups, 100 % found, average 1.17 hops, max 2 (B->A->D and D->A->B), average
stretch 1.00.

Convergecast (optional extension, implemented): `total` floods `P2P_AGG` from a root;
each node remembers who it first heard from as its parent, forwards to its other
neighbors, waits for their `P2P_AGG_REPLY`, adds its own free/total and replies to its
parent. A node that hears the qid a second time replies `dup` at once so the parent
does not wait. The root receives exactly N-1 useful replies: "52 free of 80 across
lots A,B,D,C (3 message(s) up the tree)".

## 5. Caching and consistency

Every lot keeps copies of every other lot's summary, so three questions:

How long is a cached count valid? A replica carries the time it was stored.
`find` reports its age and flags it `stale` after 12 s (four missed gossip rounds).
Clients see "Lot C: 11 free, 16.6s old [STALE]" rather than a confident wrong number.

When do updates become visible? Direct neighbors see a put within one message
delay. Everyone else within the same round with probability 1 - (1 - p_f)^n, and
otherwise within the next gossip round (3 s) or the next ping (5 s). The system is
eventually consistent; there is no moment when all lots agree exactly.

How are competing updates handled? Only the owning lot ever writes its own key, and
each write carries a version (millisecond timestamp at the origin). A receiver keeps
an entry only if the version is higher than what it holds. Late rumors, duplicates
and re-gossips of an old round cannot roll a replica back. Reservations are not
replicated at all; they go to the owner by request-reply, so two drivers can never be
granted the same space because a replica was behind.

## 6. Files

| file | what |
|---|---|
| `prototype/p2p/overlay.py` | the middleware (about 550 lines, every message type handled in one class) |
| `prototype/lot_server.py` | uses it: `gossip_loop` puts the summary, `peer_view` reads replicas, ops `lookup`, `peers`, `total` |
| `prototype/client.py` | `lookup <lot> [--no-cache] [--fresh] [--ttl n]`, `peers`, `total` |
| `prototype/p2p_metrics.py` | rebuilds the graph from the live overlay and prints every number above |
| `prototype/tests/test_p2p.py` | 27 checks on the middleware alone (join, gossip, versions, lookup, TTL, dups, convergecast, leave, crash, sampling) |
| `prototype/tests/run_tests.py` | scenarios 5 and 8 run the whole system |
| `docs/MESSAGES.md` | one real example of every P2P message |
| `docs/sample_logs_m3/` | consoles, logs and `p2p_metrics.py` output from the run above |
