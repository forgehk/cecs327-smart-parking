# Milestone 4: what changed

Coordination for the Distributed Smart Parking System (CECS 327). Everything from
Milestones 1 to 3 still runs; this milestone adds logical clocks, event ordering,
leader election, mutual exclusion and a shared resource that needs it.

Live site: https://cecs327-smart-parking.netlify.app (Milestone 4 tab)

## Logical clocks

- Every process keeps a Lamport clock (`prototype/comm/clock.py`).
- Sending is an event: `C = C + 1`, and the message goes out with that value.
- Receiving merges: `C = max(C, time in the message) + 1`.
- The message envelope has a new field, `lamport`. Every message of every
  milestone carries it, including retries and cached replies.
- Every log line records the process's Lamport time. The lot server and client
  consoles print it as `L=` next to the wall clock.
- A lot server that crashes and restarts continues its clock from its own log.

## Event ordering

- New script `prototype/event_order.py` merges the logs of all processes into one
  timeline sorted by Lamport time, then by process name.
- `event_order.py --check` verifies that every receive is later than its send and
  every reply later than its request.
- Filters: `--types`, `--process`, `--grep`, `--last`, `--no-info`.

## Leader election (Bully)

- New module `prototype/coord/election.py`. The running lot with the highest id is
  the leader: E > D > C > B > A.
- `ELECTION` goes to every lot with a higher id. A live one answers and takes over.
  If nobody higher answers within 1 s, the sender wins and sends `COORDINATOR` to
  everyone.
- Every follower sends `LEADER_PING` to the leader every 2 s. No answer within 1 s
  starts an election, so a dead leader is replaced in about 3 s.
- A lot server starts an election when it starts, so a restarted lot with the
  highest id takes the leadership back.
- `prototype/coord/wire.py` holds the shared send-one-message-and-wait-for-ACK code.

## Shared resource

- New: the campus overflow garage, spaces O-01 to O-05 (`prototype/overflow.py`).
  No lot owns it. Every lot server reads and writes the same table.
- Assigning a space is read, pick the first free one, write. Without coordination,
  several lot servers read the same free space and overwrite each other.

## Mutual exclusion (centralized)

- New module `prototype/coord/mutex.py`. The elected leader is the lock coordinator.
- A lot server sends `MUTEX_REQUEST` with a Lamport timestamp. The leader grants it
  at once if the lock is free, otherwise it queues the request.
- The queue is ordered by request timestamp, then lot id, not by arrival order.
- `MUTEX_RELEASE` hands the lock to the next lot with `MUTEX_GRANT`.
- If the leader crashes, the new leader asks every lot what it holds and what it
  waits for (`MUTEX_STATE`) and rebuilds the holder and the queue before granting.
- A lot only accepts a grant from the lot it currently follows, so a replaced
  coordinator cannot create a second holder.
- A lot that holds the lock and crashes loses it after 10 s.

## New commands

- `python3 client.py --lot B leader` shows who B believes the leader is.
- `python3 client.py --lot A elect` makes A start an election.
- `python3 client.py --id alice overflow` asks for an overflow space
  (`overflow release`, `overflow status` also work).
- `python3 mutex_race.py --no-lock` runs 6 drivers through 3 lots without the lock:
  all 6 are told O-01.
- `python3 mutex_race.py` runs the same race with the lock: 5 different spaces and
  1 driver told the garage is full.
- `python3 mutex_race.py --drivers 8 --kill-leader 0.6` kills the coordinator in
  the middle of the race: still no conflict.
- `python3 event_order.py --check` checks the clock condition on all logs.

## New message types

- `ELECTION`, `COORDINATOR`, `LEADER_PING` for the election.
- `MUTEX_REQUEST`, `MUTEX_GRANT`, `MUTEX_RELEASE`, `MUTEX_STATE` for the lock.
- Real examples of each are in `docs/MESSAGES.md`.

## Tests

- `tests/test_clock.py`: 27 checks on the clock rules, stamps, retries and causal
  chains over real sockets.
- `tests/test_election.py`: 15 checks with four nodes: crash, bully back, double
  crash, a lot alone, simultaneous elections.
- `tests/test_mutex.py`: 26 checks on the lock and the overflow table, including a
  coordinator crash and a holder crash.
- `tests/run_tests.py` has two new end-to-end scenarios with real processes:
  scenario 9 (kill the leader) and scenario 10 (the race, the lock, and the
  coordinator crashing mid race). All ten scenarios pass.

## Website

- New Milestone 4 section on the existing site, with a tab in the top bar.
- Coordination simulator: five lot servers with their Lamport clocks, the leader,
  the overflow garage and the lock queue. Buttons to send a message, crash the
  leader, restart lots, run the race with and without the lock, and crash the
  coordinator in the middle of a race. Clicking a lot crashes or starts it.
- A live space-time diagram and a merged log in Lamport order under the simulator.
- Real output from the prototype in tabs: leader crash, both races, coordinator
  crash mid race, ordered events, message types.
- Cards for the two clock rules, measured results, why Bully, why a centralized
  lock, and a table of failure cases.
- The Milestone 2 envelope table now lists `lamport`, and the test counts are
  updated.

## Files

- Added: `prototype/comm/clock.py`, `prototype/coord/` (`election.py`, `mutex.py`,
  `wire.py`), `prototype/overflow.py`, `prototype/event_order.py`,
  `prototype/mutex_race.py`, `prototype/tests/test_clock.py`,
  `prototype/tests/test_election.py`, `prototype/tests/test_mutex.py`,
  `web/js/m4.js`, `docs/M4_CHANGES.md`.
- Changed: `prototype/comm/envelope.py`, `prototype/comm/request_reply.py`,
  `prototype/common.py`, `prototype/lot_server.py`, `prototype/client.py`,
  `prototype/tests/run_tests.py`, `README.md`, `docs/MESSAGES.md`,
  `web/index.html`, `web/style.css`, `web/js/main.js`.

## What the report and video still need

- Report sections: clock design, timestamp updates, election algorithm, mutual
  exclusion algorithm, the shared resource, logs showing ordered events, and a
  failure test of the leader. The README and this file cover the content.
- Video: timestamps in messages, an election, a leader failure, a mutual exclusion
  example, and proof that conflicts are prevented. The website simulator and the
  commands above show all five.
