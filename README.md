# Distributed Smart Parking System

CECS 327 - Introduction to Networks and Distributed Computing, Fall 2026, Milestone 1

Group: Lawrence Degoma, Christopher Gonzalez-Morales, Hasan Karakra, Bertrand Lee,
Omar Lopez, Hong Nguyen, Marco Ocegueda

- Live demo (nothing to install): https://cecs327-smart-parking.netlify.app
- Architecture diagram: [`docs/architecture.png`](docs/architecture.png)
- Project plan for all milestones: [`docs/PLAN.md`](docs/PLAN.md)

## What it is

A campus has two parking lots with 20 spaces each. We track whether each space is
free, occupied or reserved, and let drivers find and reserve a space.

There are three kinds of process:

| Process | Count | Job |
|---|---|---|
| `sensor_node.py` | one per lot | simulates the occupancy sensors, pushes `event`s and a `heartbeat` snapshot every 2 s |
| `lot_server.py` | one per lot | owns the state of its lot's spaces, answers clients, swaps summaries with the other lot server |
| `client.py` | one per command | the driver app: `find`, `reserve`, `release` |

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

The same system also runs in the browser (`web/`): each process is a Web Worker and the
page acts as the network between them, so you can kill processes, cut links and add
latency while watching the lots. That is what the video uses.

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
python3 lot_server.py --lot A
python3 lot_server.py --lot B
python3 sensor_node.py --lot A
python3 sensor_node.py --lot B
python3 client.py find
```

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
python3 lot_server.py --lot C --port 5003 --peer A=127.0.0.1:5001    # a third lot
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
python3 client.py --id alice release A-03    # -> OK released
tail -n 5 logs/lot_server_A.log              # the message log
kill $(cat run/sensor_node_A.pid)            # wait 6 s, Lot A goes unknown
kill $(cat run/lot_server_A.pid)
python3 lot_server.py --lot A                # prints RECOVERY: replayed N entries
./run_demo.sh stop
```

Web version: open the Netlify link (or `cd web && python3 -m http.server 8000` and go to
http://localhost:8000) and click the five buttons under "Demo script" in order. The
"Fault injection" card can kill/restart any process, cut any link, add latency and drop
messages. "download all logs" saves every process's log as one `.jsonl` file.

## Reproducing the test cases

```
cd prototype
python3 tests/run_tests.py        # all five scenarios, about 45 s
python3 tests/run_tests.py 3 4    # just 3 and 4
```

| # | Scenario | Checks |
|---|---|---|
| 1 | request / response | `find` lists free spaces; alice `reserve` OK; bob `reserve` same space DENIED; bob `release` DENIED; alice `release` OK; server log has the request and the reply |
| 2 | lost reply | client drops the first reply, retries with the same `req_id`; server answers `duplicate: true`; the log shows one `req_id` twice and there is one reservation |
| 3 | sensor crash | kill the sensor, wait more than 6 s: all 20 spaces `unknown`, `find` returns none, total is still 20 |
| 4 | server crash | dave reserves; kill lot server A; client gets no reply; restart A; console shows `RECOVERY`; erin's `reserve` of the same space is DENIED because the lease survived |
| 5 | peer failure | A reports B's summary as fresh; kill B; after 9 s A still answers and marks B `stale` |

Process output from the run is in `tests/out/`. Logs and output from one of our runs are
in `docs/sample_logs/`, screenshots of the web demo in `docs/screenshots/`.

## Messages

One JSON object per line over TCP.

| From -> to | type | fields | reply |
|---|---|---|---|
| sensor -> lot server | `heartbeat` | `lot, seq, ts, spaces{id: free/occupied}` | none |
| sensor -> lot server | `event` | `lot, seq, ts, space, state` | none |
| client -> lot server | `find` | `req_id, client` | `reply{status, free[], lot_summary, other_lots}` |
| client -> lot server | `reserve` | `req_id, client, space` | `reply{status, reason, lease}` |
| client -> lot server | `release` | `req_id, client, space` | `reply{status, reason}` |
| lot server -> lot server | `summary` | `lot, free, occupied, reserved, unknown, total` | `summary_ack{lot}` |

A log line looks like this (`dir` is `send`, `recv` or `dropped`):

```
{"ts": 1790142788.814, "process": "lot_server_A", "dir": "recv", "peer": "127.0.0.1:43260", "msg": {"type": "heartbeat", "lot": "A", "seq": 1, "spaces": {"A-01": "free", "...": "..."}}}
```

## Layout

```
README.md
netlify.toml                publishes web/
docs/
  PLAN.md                   plan for all five milestones
  architecture.svg/.png     diagram for the report
  sample_logs/              logs + console output from a run
  screenshots/
prototype/
  common.py                 wire format, MessageLog, timing constants
  lot_server.py
  sensor_node.py
  client.py
  run_demo.sh
  tests/run_tests.py
web/
  index.html, style.css
  js/                       config, network router, ui, diagram, scenarios, main
  workers/                  sensor_node.js, lot_server.js, client.js
```
