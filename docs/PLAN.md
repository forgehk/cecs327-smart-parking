# Project plan - Distributed Smart Parking

CECS 327, Fall 2026. Group: Lawrence Degoma, Christopher Gonzalez-Morales, Hasan Karakra,
Bertrand Lee, Omar Lopez, Hong Nguyen, Marco Ocegueda.

This is our working plan for the semester. It lists what the syllabus says the
parking project has to demonstrate, what we have built so far, and what each of the
next milestones needs. Owners and dates for later milestones get filled in at the
group meeting after each milestone is graded.

## What the project has to demonstrate (from the syllabus)

The Sensor Parking project must support distributed parking sensors, parking-space
status, user queries, reservations and system updates, and must include:

| Requirement | Status | Where |
|---|---|---|
| Sensor communication | done (M1) | sensor node -> lot server events + heartbeats |
| Parking-space status and user queries | done (M1) | lot server state table, `find` |
| Reservations | done (M1) | `reserve` / `release`, 30 s lease |
| Publish-subscribe messaging | done (M2) | broker, `lot/<lot>/sensor` and `lot/<lot>/status` topics |
| Peer-to-peer integration | partial (M1, M2) | lot servers are a group at the registry and exchange summaries by group send |
| Coordination among distributed nodes | not started | M3 |
| Replicated status information | not started | M4 |
| Fault-tolerant sensor operation | partial (M1) | staleness -> unknown, log replay |

## Milestone 1 - Architecture and models (due Sep 24) - DONE

Deliverables: report, video, repo, README, logs/screenshots, contribution table.

What we built:

- [x] `common.py` - JSON-lines over TCP, per-process message log
- [x] `lot_server.py` - one per lot, thread pool, space table, find/reserve/release
- [x] `sensor_node.py` - simulated sensors, events + heartbeat snapshot
- [x] `client.py` - CLI driver app with timeout + retry
- [x] staleness rule (no report within 6 s -> `unknown`)
- [x] reservations as leases (expire after 30 s)
- [x] idempotent requests (`req_id`, server caches replies)
- [x] peer summary exchange between lot servers, STALE flag
- [x] crash recovery: lot server replays its own log on restart
- [x] `tests/run_tests.py` - 5 end-to-end scenarios, all passing
- [x] web demo (Web Workers) deployed on Netlify for the video
- [x] architecture diagram
- [x] README

Still to do before submission:

- [ ] fix report: it says "four processes" / "one sensor node"; the prototype runs one
      sensor node per lot (5 processes). Update section 2 and the requirements table.
- [ ] put `docs/architecture.png` into section 5 of the report (currently empty)
- [ ] record the video: diagram -> web demo scenarios 1-5 -> Python terminals + log file
- [ ] fill in the contribution table (everyone)
- [ ] team lead submits: PDF, video link, repo link

## Milestone 2 - Communication

Goal: one transport (TCP sockets), one wire format (length-prefixed JSON with a
common envelope), and the three communication patterns from the course built on it.

Code - done:

- [x] `comm/framing.py` - 4-byte length + JSON body, partial reads, malformed input
- [x] `comm/envelope.py` - type, msg_id, sender, timestamp, reply_to, payload
- [x] `comm/request_reply.py` - one exchange per connection, ERROR replies for unknown
      op / bad args, reply cache so a retry with the same msg_id isn't executed twice
- [x] `comm/pubsub.py` + `broker.py` - topics, long-lived subscriber connections,
      one EVENT copy per subscriber, dead subscribers dropped
- [x] `comm/group.py` + `registry.py` - JOIN/LEAVE/MEMBERS, group_send fan-out with
      one log line per target, unreachable members skipped
- [x] sensor -> broker -> lot server (pub-sub), client -> lot server (request-reply),
      lot server -> group (summaries), lot server -> `lot/<lot>/status` (pub-sub)
- [x] `client.py watch` subscribes to a lot's status topic
- [x] tests: framing unit tests, pattern tests, seven whole-system scenarios
- [x] `docs/MESSAGES.md` with a real example of every message type
- [x] `docs/sample_logs_m2/`

Report - to do (owner: BL, outline already written):

- [ ] fill in the sockets / marshalling / TCP sections from `comm/framing.py`'s docstring
- [ ] message format section: copy the examples from `docs/MESSAGES.md`
- [ ] three sequence diagrams: request-reply, pub-sub (1 publisher, 2 subscribers,
      broker), group send (sender, registry, 2 members)
- [ ] communication model diagram (who connects to whom, which pattern on which arrow)
- [ ] logging and evidence: paste from `docs/sample_logs_m2/demo_console.txt` and the
      `.log` files
- [ ] limitations: broker single point of failure, slow subscriber, no ordering across
      group members, no atomic delivery, membership can change mid-send
- [ ] video

Not done / deferred:

- the browser demo still shows the M1 wiring (no broker/registry)
- UDP for heartbeats was considered and dropped: TCP already gives us framing and
  the loss we care about is simulated at the sensor (`--drop-rate`)

## Milestone 3 - Peer-to-peer and coordination (tentative)

Goal: lot servers behave as real peers and coordinate on shared decisions.

- [ ] peer discovery: servers find each other instead of a hard-coded port table
- [ ] logical clocks (Lamport) on every message so events across lots can be ordered
- [ ] coordinator election among lot servers (bully or ring) - the coordinator handles
      cross-lot operations such as "reserve any free space on campus"
- [ ] mutual exclusion for a shared resource that is NOT owned by a single lot
      (e.g. a campus-wide reservation counter or a shared overflow lot)
- [ ] failure of the coordinator -> re-election, shown in the demo

Report items: P2P organization, election algorithm, ordering of events, what happens
when the coordinator dies.

## Milestone 4 - Replication and consistency (tentative)

Goal: space status survives the loss of a lot server without going `unknown`.

- [ ] replicate each lot's space table to the other lot server (primary-backup)
- [ ] backup takes over answering `find` for a dead lot (read-only) and marks the data
      as replicated/possibly stale
- [ ] decide consistency model for reservations (primary only vs. quorum) and justify
- [ ] sensor node can fail over to the backup server
- [ ] compare: recovery from log (M1) vs. recovery from replica (M4)

Report items: replication scheme, consistency guarantees, what is and isn't lost on
failure, measurements (time to fail over).

## Milestone 5 - Transactions, fault tolerance, final presentation (tentative)

- [ ] multi-space / multi-lot reservation as a transaction (two-phase commit across lot
      servers, with abort if any lot denies)
- [ ] full fault-injection matrix: sensor crash, sensor omission, server crash,
      link cut, partition between lots, coordinator crash - each with the expected
      behaviour and a test
- [ ] final cleanup of the web demo for the class presentation
- [ ] final report: everything above plus lessons learned
- [ ] class presentation (all members present a part)

## Working agreements

- one person owns each milestone's report; everyone reviews before submission
- every feature comes with a test scenario in `tests/run_tests.py`
- keep `README.md` runnable: if the start commands change, the README changes in the
  same commit
- branch per feature, short-lived, merged to `main` after someone else has run it
- the web demo and the Python prototype must keep the same protocol; if a message
  changes in one it changes in both

## Open questions

- do we keep the browser demo in later milestones or move the demo fully to Python?
- MQTT broker vs. writing our own pub-sub (course scope says either is fine)
- how many lots for the final demo? three would show scaling better than two
