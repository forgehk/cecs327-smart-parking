# Distributed Smart Parking System

CECS 327 - Introduction to Networks and Distributed Computing, Fall 2026
Milestone 3 (P2P integration). What each earlier milestone added is listed in
[`docs/PLAN.md`](docs/PLAN.md).

Group: Lawrence Degoma, Christopher Gonzalez-Morales, Hasan Karakra, Bertrand Lee,
Omar Lopez, Hong Nguyen, Marco Ocegueda

- Website: browser demo, communication diagrams, overlay: https://cecs327-smart-parking.netlify.app
- Architecture diagram: [`docs/architecture.png`](docs/architecture.png)
- Message reference with real examples: [`docs/MESSAGES.md`](docs/MESSAGES.md)
- P2P overlay design, trace and metrics: [`docs/P2P.md`](docs/P2P.md)
- Project plan for all milestones: [`docs/PLAN.md`](docs/PLAN.md)

## What it is

A campus has several parking lots with 20 spaces each (the demo runs four, A to D, and
a fifth can join while it runs). We track whether each space is free, occupied or
reserved, and let drivers find and reserve a space.

Processes:

| Process | Count | Job |
|---|---|---|
| `registry.py` | 1 | directory of lot servers (JOIN / LEAVE / MEMBERS); a new lot server asks it for a bootstrap peer |
| `broker.py` | 1 | pub-sub broker: routes PUBLISH to the subscribers of a topic |
| `sensor_node.py` | one per lot | simulates the occupancy sensors, publishes `event`s and a `heartbeat` snapshot every 2 s to `lot/<lot>/sensor` |
| `lot_server.py` | one per lot | owns the state of its lot's spaces, answers clients, publishes changes to `lot/<lot>/status`, and is a peer in the overlay: gossips its summary, holds replicas of the other lots, forwards lookups |
| `client.py` | one per command | the driver app: `find`, `reserve`, `release`, `watch`, plus `lookup`, `peers`, `total` for the overlay |

## How they talk (Milestone 2)

Everything is JSON over TCP. One message = a 4-byte big-endian length, then the
JSON body in UTF-8 (`prototype/comm/framing.py`). Every message carries the same
envelope: `type`, `msg_id`, `sender`, `timestamp` (ISO 8601, ms), `reply_to`, `payload`
(`prototype/comm/envelope.py`). Three patterns are built on that:

| Pattern | Module | Used for | Connection |
|---|---|---|---|
| request-reply | `comm/request_reply.py` | client -> lot server (`find`, `reserve`, `release`) | one per request: open, REQUEST, REPLY, close |
| publish-subscribe | `comm/pubsub.py` + `broker.py` | sensor -> lot server (`lot/<lot>/sensor`), lot server -> watchers (`lot/<lot>/status`) | subscriber keeps one long-lived connection to the broker; publisher reuses one |
| group communication | `comm/group.py` + `registry.py` | lot server directory; `group_send` fan-out is still available | ask the registry for MEMBERS, then one new connection per member |

## How the lot servers find each other (Milestone 3)

Lot servers form an unstructured peer-to-peer mesh (`prototype/p2p/overlay.py`).
Each keeps a short neighbor list, joins through one bootstrap peer, and leaves by
telling its neighbors. The middleware gives the application three calls:

| call | what happens |
|---|---|
| `put(key, value)` | store our value with a version and gossip it: send to all neighbors, each informed peer forwards with probability 0.7 |
| `get(key)` | local read of our keys and the replicas we hold for other lots; `find` answers "other lots" from this |
| `lookup(key, ttl)` | TTL-bounded flood for something not cached; duplicates dropped by id; whoever has it answers straight back with the path |

Plus `aggregate()`, a convergecast that sums free spaces over every lot (`client.py total`).
Maintenance: every 5 s each node pings its neighbors, drops the ones that don't answer,
and uses the random neighbor sample in each ping answer to refill its list, so the mesh
stays connected while lots come and go. Replicas carry an age and are flagged stale
after 12 s. Numbers for the default four-lot mesh (degree 3,2,3,2, diameter 2, lookups
1 to 2 hops, 2 ping messages per second overall) and a full join/leave trace are in
[`docs/P2P.md`](docs/P2P.md); `python3 p2p_metrics.py` recomputes them from a live run.

Errors a service answers with `status: ERROR`: unknown operation, bad arguments,
handler crash. If there is no reply at all the client retries with the same `msg_id`
(the service caches replies, so a retry can't run twice) and gives up after 3 tries.
The broker drops a subscriber it can't deliver to; `group_send` logs and skips a member
it can't reach and still delivers to the rest. See `docs/MESSAGES.md` for every message
type with a real example.

Rules that matter:

- A lot server is the only process that can change the state of its lot's spaces.
  That is what stops two drivers from reserving the same space.
- If a lot server hears nothing about a space for 6 s (three heartbeats) it reports the
  space as `unknown`, never as `free`.
- A reservation is a lease. It expires after 30 s so a client that crashed can't hold a
  space forever.
- Every request has a `req_id`. If the reply is lost the client retries with the same id
  and the server answers from its cache, so a retry can't reserve twice.
- Every process appends every message it sends or receives to `logs/<process>.log`
  (JSON lines). A lot server replays its own log on restart, so a crash doesn't lose
  reservations.
- Every lot server puts its summary into the overlay every 3 s. Other lots keep it as
  a replica and tell clients its age; after 12 s without a newer version it is stale.
- Only the owning lot writes its own key, and every write has a version, so a late or
  duplicated rumor can never roll a replica back. Reservations are never served from a
  replica; they always go to the owner.

The browser demo in `web/` is the Milestone 1 version of the system (direct
connections, no broker or registry). It is still useful for showing the lot state and
the failure cases visually; the Milestone 2 and 3 pages on the site are diagrams.

## Requirements

- Python 3.9 or newer (standard library only, nothing to `pip install`)
- bash for `run_demo.sh` (macOS, Linux, WSL or Git Bash). On plain Windows just run the
  commands below in separate terminals.
- for the web demo: any modern browser, and `python3 -m http.server` to serve it locally
  (Web Workers don't load from `file://`)

## Install

```
git clone https://github.com/forgehk/cecs327-smart-parking.git
cd cecs327-smart-parking/prototype
python3 --version
```

That's it.

## Starting each service

All commands run from `prototype/`. Each one is its own process, so use one terminal
per process. Lots A to E listen on ports 5001 to 5005 (`LOT_PORTS` in `common.py`).

```
python3 registry.py                                   # port 5010
python3 broker.py                                     # port 5000
python3 lot_server.py --lot A                         # first peer of the overlay
python3 lot_server.py --lot B --bootstrap 127.0.0.1:5001
python3 lot_server.py --lot C --bootstrap 127.0.0.1:5002
python3 lot_server.py --lot D --bootstrap 127.0.0.1:5001
python3 sensor_node.py --lot A                        # one per lot, publishes to lot/<lot>/sensor
python3 client.py find
python3 client.py watch A                             # prints every status EVENT for lot A until Ctrl-C
```

Without `--bootstrap` a lot server asks the registry who is running and joins through
the first one. Start order matters only a little: a lot server retries its bootstrap
for 10 s, and subscribers/publishers reconnect to the broker every second until it is up.
Ctrl-C or a plain `kill` makes a lot server leave the overlay gracefully; `kill -9` is a crash.

Or start everything in the background at once:

```
./run_demo.sh          # fresh start, deletes old logs
./run_demo.sh keep     # keep the logs so the lot servers recover their state
./run_demo.sh stop
```

Console output of the background processes goes to `run/<name>.out`, the message logs
to `logs/<name>.log`. If you get "permission denied", run `chmod +x run_demo.sh` once
(or use `bash run_demo.sh`).

Useful options:

```
python3 lot_server.py --lot A --fresh            # ignore the old log
python3 lot_server.py --lot E --bootstrap 127.0.0.1:5003   # a fifth lot joins through C while everything runs
python3 lot_server.py --lot F --port 5006 --registry none --bootstrap 127.0.0.1:5001   # no registry at all
python3 lot_server.py --lot A --broker 10.0.0.5:5000 --registry 10.0.0.5:5010   # broker/registry elsewhere
python3 sensor_node.py --lot A --drop-rate 0.3   # drop 30% of events
python3 sensor_node.py --lot A --seed 1          # same random pattern every run
python3 client.py --id alice reserve A-03
python3 client.py --id alice release A-03
python3 client.py --lot B find
python3 client.py --id alice reserve A-03 --simulate-lost-reply
python3 client.py find --json
python3 client.py lookup D                       # lot A answers from its replica of D (0 hops)
python3 client.py --lot D lookup B --no-cache --fresh   # D floods the overlay, only B may answer
python3 client.py --lot C peers                  # C's neighbors, replicas, counters
python3 client.py total                          # free spaces over every lot, by convergecast
python3 p2p_metrics.py                           # degree, edges, diameter, lookup hops, traffic
```

## Running the demo

```
cd prototype
./run_demo.sh
python3 client.py find                       # -> OK Lot A: 14 free ... plus what A holds about B, C, D
python3 client.py --id alice reserve A-03    # -> OK reserved (lease 30s)
python3 client.py --id bob reserve A-03      # -> DENIED reserved
python3 client.py --id bob reserve Z-99      # -> ERROR no such space (bad argument)
python3 client.py watch A &                  # second terminal: live EVENTs from the broker
python3 client.py --id alice release A-03    # -> OK released; the watcher prints "A-03 -> free"
python3 client.py peers                      # A's neighbors (B, C, D) and its replicas
python3 client.py --lot D lookup B --no-cache --fresh   # -> found at B after 2 hop(s), path D->A->B
python3 client.py total                      # -> 52 free of 80 across lots A,B,D,C (3 messages up the tree)
python3 lot_server.py --lot E --bootstrap 127.0.0.1:5003 &   # E joins through C, takes 4 replicas, gets 2 neighbors
kill $(cat run/lot_server_C.pid)             # graceful leave: A, B, D, E log "left gracefully"
kill -9 $(cat run/lot_server_D.pid)          # crash: within 5 s neighbors log "dead pointer removed"
python3 client.py find                       # C and D still listed, now [STALE]
python3 p2p_metrics.py                       # the mesh as it is now
kill $(cat run/sensor_node_A.pid)            # wait 6 s, Lot A goes unknown
kill $(cat run/broker.pid)                   # sensors can't publish; Lot A goes unknown
python3 broker.py                            # everyone reconnects and re-subscribes
./run_demo.sh stop
```

Web version: open the Netlify link (or `cd web && python3 -m http.server 8000` and go to
http://localhost:8000) and click the five buttons under "Demo script" in order. The
"Fault injection" card can kill/restart any process, cut any link, add latency and drop
messages. "download all logs" saves every process's log as one `.jsonl` file.

## Reproducing the test cases

```
cd prototype
python3 tests/test_framing.py     # framing + envelope, no network, < 1 s
python3 tests/test_comm.py        # the three patterns in-process over real sockets, ~5 s
python3 tests/test_p2p.py         # the overlay: five peers in-process over real sockets, ~15 s
python3 tests/run_tests.py        # whole system, eight scenarios, about 2 minutes
python3 tests/run_tests.py 5 8    # just the overlay scenarios
```

`test_framing.py`: round trip, two messages in one TCP write, a message delivered in
7-byte pieces, malformed JSON, absurd length header, peer closing mid-message.

`test_comm.py`: request-reply (OK, unknown op, bad args, lost reply + retry, service
down), pub-sub (two subscribers each get a copy, topic routing, no subscribers,
unsubscribe, dead subscriber removed, one log line per delivery), group (JOIN/MEMBERS,
fan-out to the other members, one log line per target, dead member skipped, LEAVE).

`test_p2p.py`: join through bootstrap peers (neighbor pointers handed out, connected
mesh, varied degree), gossip (put reaches everyone, newer version wins, stale version
ignored, every peer hosts the others' replicas), lookup (flood with hops and path,
cache hit on the second try, owner-only reads, duplicates dropped, not found, TTL 1
stops at the neighbors), convergecast (every lot counted once, N-1 replies), graceful
leave, crash detected by ping, repair counter, a new peer receiving the table, random
sampling refilling short lists, old replicas kept and aging.

`run_tests.py` scenarios:

| # | Scenario | Checks |
|---|---|---|
| 1 | request-reply | `find` lists free spaces; alice `reserve` OK; bob same space DENIED; bob `release` DENIED; alice `release` OK; `Z-99` -> `status: ERROR`; server log has REQUEST/REPLY pairs linked by `reply_to` |
| 2 | lost reply | client drops the first reply, retries with the same `msg_id`; server answers `duplicate: true`; log shows one `msg_id` twice, one reservation |
| 3 | sensor crash | kill the sensor, wait > 6 s: all 20 spaces `unknown`, `find` returns none |
| 4 | server crash | dave reserves; kill lot server A; client gets no reply; restart A; `RECOVERY` in console; erin's `reserve` of that space DENIED because the lease survived |
| 5 | overlay / peer failure | A holds B's summary as a replica delivered by P2P_GOSSIP; A's log shows B's P2P_JOIN; `kill -9` B: A still answers, flags B's replica `stale`, console shows "dead pointer removed", A's degree drops to 0 |
| 6 | broker crash | kill the broker: sensors can't publish, Lot A goes all `unknown`, but request-reply still works; restart the broker: lot server re-SUBSCRIBEs, sensor republishes, lot recovers |
| 7 | status watch | a `client.py watch A` process receives the reserved/free EVENTs the lot server publishes; broker log has one send line per delivery |
| 8 | join / leave trace | four lots up with 2 to 3 neighbors each and three replicas each; D's owner-only lookup for B takes 2 hops; `total` from A covers all four; E joins through C, receives the table, has 2 neighbors; C leaves gracefully and every neighbor drops it at once; D crashes and is removed by ping; A, B, E are still one connected mesh; `total` now covers exactly A, B, E |

Process output from the run is in `tests/out/`. Logs and output from our runs are in
`docs/sample_logs/` (M1), `docs/sample_logs_m2/` (M2) and `docs/sample_logs_m3/` (M3).

## Messages

See [`docs/MESSAGES.md`](docs/MESSAGES.md): the envelope, one real example of every
message type (REQUEST, REPLY, SUBSCRIBE, ACK, PUBLISH, EVENT, UNSUBSCRIBE, JOIN, MEMBERS,
GROUP_MSG, LEAVE, and the nine P2P_* types), the payload of each, the topics and groups,
and the log line format.

## Layout

```
README.md
netlify.toml                publishes web/
docs/
  PLAN.md                   plan for all five milestones
  MESSAGES.md               message reference with real examples
  P2P.md                    overlay design, join/leave trace, metrics
  architecture.svg/.png     diagram for the report
  sample_logs/              M1 logs, sample_logs_m2/ M2 logs, sample_logs_m3/ M3 logs
  screenshots/
prototype/
  common.py                 ports, topics, timing constants, MessageLog
  comm/
    framing.py              length-prefixed JSON over TCP
    envelope.py             the common message fields
    request_reply.py        call() and Service
    pubsub.py               Broker, Subscriber, Publisher
    group.py                Registry, join/leave/members, group_send
  p2p/
    overlay.py              Peer: neighbors, join/leave, ping repair, gossip, lookup, convergecast
  p2p_metrics.py            graph metrics from a live overlay
  registry.py               the registry process
  broker.py                 the broker process
  lot_server.py
  sensor_node.py
  client.py
  run_demo.sh
  tests/
    test_framing.py         unit tests, no network
    test_comm.py            the three patterns over real sockets
    test_p2p.py             the overlay over real sockets
    run_tests.py            whole-system scenarios
web/                        Milestone 1 browser demo
  index.html, style.css
  js/                       config, network router, ui, diagram, scenarios, main
  workers/                  sensor_node.js, lot_server.js, client.js
```
