# Distributed Smart Parking System

CECS 327 - Introduction to Networks and Distributed Computing, Fall 2026
Milestone 2 (Communication). Milestone 1 (Architecture) is tagged `m1`.

Group: Lawrence Degoma, Christopher Gonzalez-Morales, Hasan Karakra, Bertrand Lee,
Omar Lopez, Hong Nguyen, Marco Ocegueda

- Browser demo of the Milestone 1 system: https://cecs327-smart-parking.netlify.app
- Architecture diagram: [`docs/architecture.png`](docs/architecture.png)
- Message reference with real examples: [`docs/MESSAGES.md`](docs/MESSAGES.md)
- Project plan for all milestones: [`docs/PLAN.md`](docs/PLAN.md)

## What it is

A campus has two parking lots with 20 spaces each. We track whether each space is
free, occupied or reserved, and let drivers find and reserve a space.

Processes:

| Process | Count | Job |
|---|---|---|
| `registry.py` | 1 | group membership: which lot servers exist (JOIN / LEAVE / MEMBERS) |
| `broker.py` | 1 | pub-sub broker: routes PUBLISH to the subscribers of a topic |
| `sensor_node.py` | one per lot | simulates the occupancy sensors, publishes `event`s and a `heartbeat` snapshot every 2 s to `lot/<lot>/sensor` |
| `lot_server.py` | one per lot | owns the state of its lot's spaces, answers clients, publishes changes to `lot/<lot>/status`, sends its summary to the `lot_servers` group |
| `client.py` | one per command | the driver app: `find`, `reserve`, `release`, `watch` |

## How they talk (Milestone 2)

Everything is JSON over TCP. One message = a 4-byte big-endian length, then the
JSON body in UTF-8 (`prototype/comm/framing.py`). Every message carries the same
envelope: `type`, `msg_id`, `sender`, `timestamp` (ISO 8601, ms), `reply_to`, `payload`
(`prototype/comm/envelope.py`). Three patterns are built on that:

| Pattern | Module | Used for | Connection |
|---|---|---|---|
| request-reply | `comm/request_reply.py` | client -> lot server (`find`, `reserve`, `release`) | one per request: open, REQUEST, REPLY, close |
| publish-subscribe | `comm/pubsub.py` + `broker.py` | sensor -> lot server (`lot/<lot>/sensor`), lot server -> watchers (`lot/<lot>/status`) | subscriber keeps one long-lived connection to the broker; publisher reuses one |
| group communication | `comm/group.py` + `registry.py` | lot server -> all lot servers (summaries) | ask the registry for MEMBERS, then one new connection per member |

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
- The two lot servers send each other a summary every 3 s. If a summary is more than
  9 s old the client is told the other lot's numbers are stale.

The browser demo in `web/` is the Milestone 1 version of the system (direct
connections, no broker or registry). It is still useful for showing the lot state and
the failure cases visually, but it does not model the M2 communication patterns.

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
per process. Lot A listens on port 5001 and lot B on 5002 (`LOT_PORTS` in `common.py`).

```
python3 registry.py                 # port 5010
python3 broker.py                   # port 5000
python3 lot_server.py --lot A       # port 5001, subscribes to lot/A/sensor, joins lot_servers
python3 lot_server.py --lot B       # port 5002
python3 sensor_node.py --lot A      # publishes to lot/A/sensor
python3 sensor_node.py --lot B
python3 client.py find
python3 client.py watch A           # prints every status EVENT for lot A until Ctrl-C
```

Start order matters only a little: a lot server retries JOIN until the registry is up,
and subscribers/publishers reconnect to the broker every second until it is up.

Or start everything in the background at once:

```
./run_demo.sh          # fresh start, deletes old logs
./run_demo.sh keep     # keep the logs so the lot servers recover their state
./run_demo.sh stop
```

Console output of the background processes goes to `run/<name>.out`, the message logs
to `logs/<name>.log`.

Useful options:

```
python3 lot_server.py --lot A --fresh            # ignore the old log
python3 lot_server.py --lot C --port 5003        # a third lot: just joins the group, nobody needs reconfiguring
python3 lot_server.py --lot A --broker 10.0.0.5:5000 --registry 10.0.0.5:5010   # broker/registry elsewhere
python3 sensor_node.py --lot A --drop-rate 0.3   # drop 30% of events
python3 sensor_node.py --lot A --seed 1          # same random pattern every run
python3 client.py --id alice reserve A-03
python3 client.py --id alice release A-03
python3 client.py --lot B find
python3 client.py --id alice reserve A-03 --simulate-lost-reply
python3 client.py find --json
```

## Running the demo

```
cd prototype
./run_demo.sh
python3 client.py find                       # -> OK Lot A: 14 free ... plus Lot B's summary
python3 client.py --id alice reserve A-03    # -> OK reserved (lease 30s)
python3 client.py --id bob reserve A-03      # -> DENIED reserved
python3 client.py --id bob reserve Z-99      # -> ERROR no such space (bad argument)
python3 client.py watch A &                  # second terminal: live EVENTs from the broker
python3 client.py --id alice release A-03    # -> OK released; the watcher prints "A-03 -> free"
tail -n 5 logs/broker.log                    # the broker's message log
kill $(cat run/sensor_node_A.pid)            # wait 6 s, Lot A goes unknown
kill $(cat run/lot_server_B.pid)             # A logs "member unreachable, skipped"; B's summary goes STALE
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
python3 tests/run_tests.py        # whole system, seven scenarios, about 80 s
python3 tests/run_tests.py 6 7    # just 6 and 7
```

`test_framing.py`: round trip, two messages in one TCP write, a message delivered in
7-byte pieces, malformed JSON, absurd length header, peer closing mid-message.

`test_comm.py`: request-reply (OK, unknown op, bad args, lost reply + retry, service
down), pub-sub (two subscribers each get a copy, topic routing, no subscribers,
unsubscribe, dead subscriber removed, one log line per delivery), group (JOIN/MEMBERS,
fan-out to the other members, one log line per target, dead member skipped, LEAVE).

`run_tests.py` scenarios:

| # | Scenario | Checks |
|---|---|---|
| 1 | request-reply | `find` lists free spaces; alice `reserve` OK; bob same space DENIED; bob `release` DENIED; alice `release` OK; `Z-99` -> `status: ERROR`; server log has REQUEST/REPLY pairs linked by `reply_to` |
| 2 | lost reply | client drops the first reply, retries with the same `msg_id`; server answers `duplicate: true`; log shows one `msg_id` twice, one reservation |
| 3 | sensor crash | kill the sensor, wait > 6 s: all 20 spaces `unknown`, `find` returns none |
| 4 | server crash | dave reserves; kill lot server A; client gets no reply; restart A; `RECOVERY` in console; erin's `reserve` of that space DENIED because the lease survived |
| 5 | group / peer failure | A learned about B through GROUP_MSG; A's log has one send line per member; registry logged both JOINs; kill B: A still answers, marks B `stale`, logs "member unreachable, skipped" |
| 6 | broker crash | kill the broker: sensors can't publish, Lot A goes all `unknown`, but request-reply still works; restart the broker: lot server re-SUBSCRIBEs, sensor republishes, lot recovers |
| 7 | status watch | a `client.py watch A` process receives the reserved/free EVENTs the lot server publishes; broker log has one send line per delivery |

Process output from the run is in `tests/out/`. Logs and output from our runs are in
`docs/sample_logs/` (M1) and `docs/sample_logs_m2/` (M2).

## Messages

See [`docs/MESSAGES.md`](docs/MESSAGES.md): the envelope, one real example of every
message type (REQUEST, REPLY, SUBSCRIBE, ACK, PUBLISH, EVENT, UNSUBSCRIBE, JOIN, MEMBERS,
GROUP_MSG, LEAVE), the payload of each, the topics and groups, and the log line format.

## Layout

```
README.md
netlify.toml                publishes web/
docs/
  PLAN.md                   plan for all five milestones
  MESSAGES.md               message reference with real examples
  architecture.svg/.png     diagram for the report
  sample_logs/              M1 logs, sample_logs_m2/ M2 logs
  screenshots/
prototype/
  common.py                 ports, topics, timing constants, MessageLog
  comm/
    framing.py              length-prefixed JSON over TCP
    envelope.py             the common message fields
    request_reply.py        call() and Service
    pubsub.py               Broker, Subscriber, Publisher
    group.py                Registry, join/leave/members, group_send
  registry.py               the registry process
  broker.py                 the broker process
  lot_server.py
  sensor_node.py
  client.py
  run_demo.sh
  tests/
    test_framing.py         unit tests, no network
    test_comm.py            the three patterns over real sockets
    run_tests.py            whole-system scenarios
web/                        Milestone 1 browser demo
  index.html, style.css
  js/                       config, network router, ui, diagram, scenarios, main
  workers/                  sensor_node.js, lot_server.js, client.js
```
