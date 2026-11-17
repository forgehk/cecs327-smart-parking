# Message reference (Milestone 2)
Every message is one JSON object, sent as a 4-byte big-endian length followed by the UTF-8 JSON body (`comm/framing.py`).
All messages share the envelope from `comm/envelope.py`:
| field | meaning |
|---|---|
| `type` | what kind of message, used to dispatch |
| `msg_id` | unique id (8 hex chars); a REPLY/ACK echoes it in `reply_to` |
| `sender` | process name, for the logs |
| `timestamp` | ISO 8601 with milliseconds, sender's clock |
| `lamport` | sender's Lamport clock when it sent the message (Milestone 4, `comm/clock.py`); optional, older logs don't have it |
| `reply_to` | `msg_id` of the message this answers, else `null` |
| `payload` | type-specific data |

Since Milestone 4 every log entry also has a `lamport` field: the process's Lamport time for that event
(the message's stamp for a send, `max(local, stamp) + 1` for a receive). `python event_order.py` merges all
logs into one timeline in that order; `python event_order.py --check` verifies every receive comes after its send.

The examples below are real lines from `docs/sample_logs_m2/` (one log entry = `ts`, `process`, `dir`, `peer`, `msg`).

## Request-reply (client <-> lot server)

### REQUEST  (from `client_alice.log`, dir=send)

```json
{
  "type": "REQUEST",
  "msg_id": "9040cd5e",
  "sender": "client_alice",
  "timestamp": "2026-10-11T23:24:45.413",
  "reply_to": null,
  "payload": {
    "op": "reserve",
    "args": {
      "client": "alice",
      "space": "A-03"
    }
  }
}
```

### REPLY  (from `client_alice.log`, dir=recv)

```json
{
  "type": "REPLY",
  "msg_id": "a68a299d",
  "sender": "lot_server_A",
  "timestamp": "2026-10-11T23:24:45.416",
  "reply_to": "9040cd5e",
  "payload": {
    "status": "OK",
    "result": {
      "op": "reserve",
      "lot": "A",
      "space": "A-03",
      "client": "alice",
      "granted": true,
      "reason": "reserved",
      "lease": 30.0
    }
  }
}
```

## Publish-subscribe (via the broker)

### SUBSCRIBE  (from `broker.log`, dir=recv)

```json
{
  "type": "SUBSCRIBE",
  "msg_id": "97bb94e6",
  "sender": "lot_server_B",
  "timestamp": "2026-10-11T23:24:38.063",
  "reply_to": null,
  "payload": {
    "topic": "lot/B/sensor"
  }
}
```

### ACK  (from `broker.log`, dir=send)

```json
{
  "type": "ACK",
  "msg_id": "2beed3f8",
  "sender": "broker",
  "timestamp": "2026-10-11T23:24:38.072",
  "reply_to": "97bb94e6",
  "payload": {
    "status": "OK",
    "topic": "lot/B/sensor"
  }
}
```

### PUBLISH  (from `broker.log`, dir=recv)

```json
{
  "type": "PUBLISH",
  "msg_id": "3954d403",
  "sender": "sensor_node_A",
  "timestamp": "2026-10-11T23:24:38.852",
  "reply_to": null,
  "payload": {
    "topic": "lot/A/sensor",
    "data": {
      "type": "heartbeat",
      "lot": "A",
      "seq": 1,
      "ts": 1791786278.852,
      "spaces": {
        "A-01": "free",
        "A-02": "occupied",
        "A-03": "free",
        "A-04": "free",
        "A-05": "free",
        "A-06": "free",
        "A-07": "free",
        "A-08": "free",
        "A-09": "occupied",
        "A-10": "free",
        "A-11": "free",
        "A-12": "free",
        "A-13": "free",
        "A-14": "free",
        "A-15": "free",
        "A-16": "free",
        "A-17": "occupied",
        "A-18": "free",
        "A-19": "occupied",
        "A-20": "free"
      }
    }
  }
}
```

### EVENT  (from `broker.log`, dir=send)

```json
{
  "type": "EVENT",
  "msg_id": "2cea38e4",
  "sender": "broker",
  "timestamp": "2026-10-11T23:24:41.864",
  "reply_to": null,
  "payload": {
    "topic": "lot/A/sensor",
    "data": {
      "type": "event",
      "lot": "A",
      "seq": 3,
      "ts": 1791786281.863,
      "space": "A-11",
      "state": "occupied"
    },
    "publisher": "sensor_node_A"
  }
}
```

### UNSUBSCRIBE  (sent by `client.py watch` on Ctrl-C)

```json
{
  "type": "UNSUBSCRIBE",
  "msg_id": "5d1c0b7a",
  "sender": "client_watcher",
  "timestamp": "2026-10-11T23:26:02.004",
  "reply_to": null,
  "payload": {
    "topic": "lot/A/status"
  }
}
```

## Group communication (via the registry)

### JOIN  (from `lot_server_A.log`, dir=send)

```json
{
  "type": "JOIN",
  "msg_id": "669e1803",
  "sender": "lot_server_A",
  "timestamp": "2026-10-11T23:24:38.068",
  "reply_to": null,
  "payload": {
    "group": "lot_servers",
    "address": "127.0.0.1:5001"
  }
}
```

### MEMBERS  (from `lot_server_A.log`, dir=send)

```json
{
  "type": "MEMBERS",
  "msg_id": "153f84c5",
  "sender": "lot_server_A",
  "timestamp": "2026-10-11T23:24:41.070",
  "reply_to": null,
  "payload": {
    "group": "lot_servers"
  }
}
```

### GROUP_MSG  (from `lot_server_A.log`, dir=recv)

```json
{
  "type": "GROUP_MSG",
  "msg_id": "42a612a9",
  "sender": "lot_server_B",
  "timestamp": "2026-10-11T23:24:41.066",
  "reply_to": null,
  "payload": {
    "group": "lot_servers",
    "data": {
      "kind": "summary",
      "lot": "B",
      "ts": 1791786281.064,
      "free": 12,
      "occupied": 8,
      "reserved": 0,
      "unknown": 0,
      "total": 20
    }
  }
}
```

### LEAVE  (sent by a lot server on a clean shutdown; a crashed server never sends it)

```json
{
  "type": "LEAVE",
  "msg_id": "b03e9f12",
  "sender": "lot_server_B",
  "timestamp": "2026-10-11T23:26:40.550",
  "reply_to": null,
  "payload": {
    "group": "lot_servers",
    "address": "127.0.0.1:5002"
  }
}
```

## Peer-to-peer overlay (Milestone 3, lot server <-> lot server)

All of these arrive on a lot server's normal port and are dispatched by type. JOIN,
NEIGHBOR, PING are answered with an ACK on the same connection; GOSSIP, LOOKUP, FOUND,
AGG and AGG_REPLY are one-way. Examples are from `docs/sample_logs_m3/`.

Lot B joins through A:

```json
{"type": "P2P_JOIN", "msg_id": "958487fc", "sender": "lot_server_B", "timestamp": "2026-11-01T12:49:25.041",
 "reply_to": null, "payload": {"id": "B", "address": "127.0.0.1:5002"}}
```

A's answer: a sample of its neighbors (none yet, B is the first) and its whole table:

```json
{"type": "ACK", "msg_id": "0a363886", "sender": "lot_server_A", "timestamp": "2026-11-01T12:49:25.042",
 "reply_to": "958487fc",
 "payload": {"status": "OK", "id": "A", "accepted": true, "neighbors": [],
             "table": {"lot/A/summary": {"value": {"free": 0, "occupied": 0, "reserved": 0, "unknown": 20, "total": 20, "lot": "A", "ts": 1793566164.6},
                                         "version": 1793566164600, "origin": "A", "ts": 1793566164.6}}}}
```

B asks E to be its neighbor (random sampling after C left); E accepts and sends its table:

```json
{"type": "P2P_NEIGHBOR", "msg_id": "535cbed7", "sender": "lot_server_B", "timestamp": "2026-11-01T12:49:40.061",
 "reply_to": null, "payload": {"id": "B", "address": "127.0.0.1:5002"}}
{"type": "ACK", "msg_id": "bac0c316", "sender": "lot_server_E", "timestamp": "2026-11-01T12:49:40.063",
 "reply_to": "535cbed7", "payload": {"status": "OK", "id": "E", "accepted": true, "table": {"lot/A/summary": "...", "lot/B/summary": "...", "lot/C/summary": "...", "lot/D/summary": "...", "lot/E/summary": "..."}}}
```

C leaves gracefully (one of these to each neighbor, no reply):

```json
{"type": "P2P_LEAVE", "msg_id": "8d4e4e6f", "sender": "lot_server_C", "timestamp": "2026-11-01T12:49:38.232",
 "reply_to": null, "payload": {"id": "C", "address": "127.0.0.1:5003"}}
```

A pings B every 5 s; the ACK carries B's degree, a random sample of B's other neighbors
(peer sampling) and B's table (anti-entropy):

```json
{"type": "P2P_PING", "msg_id": "06a545c5", "sender": "lot_server_A", "timestamp": "2026-11-01T12:49:29.427",
 "reply_to": null, "payload": {"id": "A", "address": "127.0.0.1:5001"}}
{"type": "ACK", "msg_id": "05ab51f9", "sender": "lot_server_B", "timestamp": "2026-11-01T12:49:29.429",
 "reply_to": "06a545c5",
 "payload": {"status": "OK", "id": "B", "degree": 2, "sample": [{"address": "127.0.0.1:5003", "id": "C"}],
             "table": {"lot/A/summary": "...", "lot/B/summary": "...", "lot/C/summary": "...", "lot/D/summary": "..."}}}
```

A rumor: C put its summary, A got it and forwards it (hop 1, path C->A). The same
msg_id travels with the rumor so a peer that sees it twice drops it:

```json
{"type": "P2P_GOSSIP", "msg_id": "1180831f", "sender": "lot_server_A", "timestamp": "2026-11-01T12:49:25.643",
 "reply_to": null,
 "payload": {"key": "lot/C/summary",
             "value": {"free": 0, "occupied": 0, "reserved": 0, "unknown": 20, "total": 20, "lot": "C", "ts": 1793566165.642},
             "version": 1793566165642, "origin": "C", "hop": 1, "path": ["C", "A"]}}
```

A lookup D started for lot B's summary (owner only), as forwarded by A with the TTL
already decremented, and B's answer sent straight back to D:

```json
{"type": "P2P_LOOKUP", "msg_id": "904cd5c0", "sender": "lot_server_A", "timestamp": "2026-11-01T12:49:38.056",
 "reply_to": null,
 "payload": {"qid": "02d26837", "key": "lot/B/summary", "ttl": 2, "asker": "127.0.0.1:5004", "path": ["D", "A"], "fresh": true}}
{"type": "P2P_FOUND", "msg_id": "a0ddc60a", "sender": "lot_server_B", "timestamp": "2026-11-01T12:49:38.058",
 "reply_to": null,
 "payload": {"qid": "02d26837", "key": "lot/B/summary",
             "value": {"free": 12, "occupied": 8, "reserved": 0, "unknown": 0, "total": 20, "lot": "B", "ts": 1793566177.054},
             "version": 1793566177054, "origin": "B", "holder": "B", "hops": 2, "path": ["D", "A", "B"]}}
```

Convergecast: A is the root; B already heard this qid from someone else, so it answers
`dup` at once instead of making A wait:

```json
{"type": "P2P_AGG", "msg_id": "222ee530", "sender": "lot_server_A", "timestamp": "2026-11-01T12:49:38.123",
 "reply_to": null, "payload": {"qid": "3a3c0f16", "root": "127.0.0.1:5001", "parent": "127.0.0.1:5001", "ttl": 3}}
{"type": "P2P_AGG_REPLY", "msg_id": "1d1887a4", "sender": "lot_server_B", "timestamp": "2026-11-01T12:49:38.128",
 "reply_to": null, "payload": {"qid": "3a3c0f16", "child": "127.0.0.1:5002", "dup": true}}
```

A real (non-dup) AGG_REPLY carries `{"qid", "child", "lots": ["C", "D"], "free": 26, "total": 40, "messages": 1}`.

## Leader election (Milestone 4, lot server <-> lot server)

Bully algorithm (`coord/election.py`). One connection per message, answered by an ACK on
the same connection (`coord/wire.py`). Priority is the lot letter: E > D > C > B > A.

| type | sent to | payload | ACK payload |
|---|---|---|---|
| `ELECTION` | every node with a higher id | `id`, `address`, `reason` | `{"status": "OK", "alive": true, "id"}`: "I'm alive and I take over" |
| `COORDINATOR` | every other node, by the winner | `id`, `address` | `{"status": "OK", "id"}` |
| `LEADER_PING` | the leader, by each follower every 2 s | `id` | `{"status": "OK", "id", "leader"}`; no ACK within 1 s = leader failed |

### ELECTION  (from `lot_server_A.log`, dir=send, after leader C crashed)

```json
{
  "type": "ELECTION",
  "msg_id": "d441451d",
  "sender": "lot_server_A",
  "timestamp": "2026-11-17T01:29:10.987",
  "lamport": 62,
  "reply_to": null,
  "payload": {"id": "A", "address": "127.0.0.1:5001", "reason": "leader C failed"}
}
```

### ACK to ELECTION  (from `lot_server_A.log`, dir=recv): B is alive and takes over

```json
{
  "type": "ACK",
  "msg_id": "da1d3031",
  "sender": "lot_server_B",
  "timestamp": "2026-11-17T01:29:10.994",
  "lamport": 64,
  "reply_to": "d441451d",
  "payload": {"status": "OK", "alive": true, "id": "B"}
}
```

### COORDINATOR  (from `lot_server_A.log`, dir=send)

```json
{
  "type": "COORDINATOR",
  "msg_id": "44cae3a7",
  "sender": "lot_server_A",
  "timestamp": "2026-11-17T01:29:06.992",
  "lamport": 18,
  "reply_to": null,
  "payload": {"id": "A", "address": "127.0.0.1:5001"}
}
```

### LEADER_PING  (from `lot_server_A.log`, dir=send)

```json
{
  "type": "LEADER_PING",
  "msg_id": "a36193bb",
  "sender": "lot_server_A",
  "timestamp": "2026-11-17T01:29:12.987",
  "lamport": 100,
  "reply_to": null,
  "payload": {"id": "A"}
}
```

## Mutual exclusion (Milestone 4, lot server <-> the leader)

Centralized algorithm (`coord/mutex.py`); the leader is the coordinator. It protects the
shared overflow garage table (`overflow.py`, resource name `overflow`).

| type | from -> to | payload | ACK payload |
|---|---|---|---|
| `MUTEX_REQUEST` | requester -> leader | `resource`, `node`, `address`, `req_id`, `ts` (the requester's Lamport time for the request, kept if it is re-sent to a new leader) | `granted` true, or false with `holder`, `position`, `queue` ordered by `(ts, node)`; `status` `RECOVERING` / `NOT_LEADER` = ask again |
| `MUTEX_GRANT` | leader -> next in line | `resource`, `req_id`, `ts`, `coordinator` | `accepted`: false if it is not from the node the requester thinks leads, or it no longer waits |
| `MUTEX_RELEASE` | holder -> leader | `resource`, `node`, `req_id` (also withdraws a request that gave up) | `{"status": "OK"}` |
| `MUTEX_STATE` | new leader -> every node | `coordinator` | `node`, `address`, `holding` and `waiting` lists of `{resource, req_id, ts}` |

### MUTEX_REQUEST  (from `lot_server_A.log`, dir=send)

```json
{
  "type": "MUTEX_REQUEST",
  "msg_id": "cd05e05d",
  "sender": "lot_server_A",
  "timestamp": "2026-11-17T01:45:19.612",
  "lamport": 74,
  "reply_to": null,
  "payload": {"resource": "overflow", "node": "A", "address": "127.0.0.1:5001", "req_id": "d148b04e", "ts": 73}
}
```

### ACK to MUTEX_REQUEST, queued  (from the requester's log, dir=recv)

```json
{
  "type": "ACK",
  "msg_id": "ce44fe58",
  "sender": "lot_server_C",
  "timestamp": "2026-11-17T01:45:20.899",
  "lamport": 125,
  "reply_to": "6484e247",
  "payload": {"status": "OK", "granted": false, "holder": "C", "position": 2, "queue": ["B(ts=103)", "A(ts=119)"]}
}
```

### MUTEX_RELEASE  (from `lot_server_A.log`, dir=send)

```json
{
  "type": "MUTEX_RELEASE",
  "msg_id": "04de608c",
  "sender": "lot_server_A",
  "timestamp": "2026-11-17T01:45:20.152",
  "lamport": 89,
  "reply_to": null,
  "payload": {"resource": "overflow", "node": "A", "req_id": "d148b04e"}
}
```

### After the coordinator crashed: MUTEX_STATE, A's answer, and the GRANT of that same request

New leader B asks every node (`lot_server_B.log`, dir=send):

```json
{"type": "MUTEX_STATE", "msg_id": "cf098306", "sender": "lot_server_B", "timestamp": "2026-11-17T01:45:27.191",
 "lamport": 286, "reply_to": null, "payload": {"coordinator": "B"}}
```

A is still waiting with the request it made before the crash (`lot_server_A.log`, dir=send):

```json
{"type": "ACK", "msg_id": "006728b7", "sender": "lot_server_A", "timestamp": "2026-11-17T01:45:27.200",
 "lamport": 288, "reply_to": "cf098306",
 "payload": {"status": "OK", "node": "A", "address": "127.0.0.1:5001", "holding": [],
             "waiting": [{"resource": "overflow", "req_id": "390ee355", "ts": 238}]}}
```

B queues it with its original `ts` and later grants exactly that request (`lot_server_B.log`, dir=send):

```json
{"type": "MUTEX_GRANT", "msg_id": "1d1d703e", "sender": "lot_server_B", "timestamp": "2026-11-17T01:45:28.211",
 "lamport": 305, "reply_to": null,
 "payload": {"resource": "overflow", "req_id": "390ee355", "ts": 238, "coordinator": "B"}}
```

## Payloads

| type | payload | answered by |
|---|---|---|
| REQUEST | `{"op": "find"\|"reserve"\|"release"\|"lookup"\|"peers"\|"total", "args": {...}}` | REPLY |
| REPLY | `{"status": "OK", "result": {...}}` or `{"status": "ERROR", "error": "text"}`; `duplicate: true` on a cached retry | - |
| SUBSCRIBE / UNSUBSCRIBE | `{"topic": "lot/A/sensor"}` | ACK |
| PUBLISH | `{"topic": ..., "data": {...}}` | ACK `{"delivered": n}` |
| EVENT | `{"topic": ..., "data": {...}, "publisher": sender}` | - |
| JOIN / LEAVE | `{"group": "lot_servers", "address": "127.0.0.1:5001"}` | ACK |
| MEMBERS | `{"group": ...}` | ACK `{"members": [...]}` |
| GROUP_MSG | `{"group": ..., "data": {...}}` | - |
| P2P_JOIN | `{"id": "B", "address": "127.0.0.1:5002"}` | ACK `{"id", "accepted", "neighbors": [...], "table": {...}}` |
| P2P_NEIGHBOR | `{"id", "address"}` | ACK `{"id", "accepted", "table"}` |
| P2P_LEAVE | `{"id", "address"}` | - |
| P2P_PING | `{"id", "address"}` | ACK `{"id", "degree", "sample": [...], "table"}` |
| P2P_GOSSIP | `{"key", "value", "version", "origin", "hop", "path"}` | - |
| P2P_LOOKUP | `{"qid", "key", "ttl", "asker", "path", "fresh"}` | P2P_FOUND to the asker |
| P2P_FOUND | `{"qid", "key", "value", "version", "origin", "holder", "hops", "path"}` | - |
| P2P_AGG | `{"qid", "root", "parent", "ttl"}` | P2P_AGG_REPLY to the parent |
| P2P_AGG_REPLY | `{"qid", "child", "lots", "free", "total", "messages"}` or `{"qid", "child", "dup": true}` | - |

Business outcomes are inside `result`, not in `status`: a denied reservation is
`status: OK, result: {granted: false, reason: "reserved"}`. `status: ERROR` is only for
protocol problems (unknown operation, bad arguments, handler crash).

## Topics and groups

| name | publisher | subscribers | data |
|---|---|---|---|
| `lot/<lot>/sensor` | sensor node | that lot's server | `{"type": "heartbeat", "seq", "spaces": {...}}` or `{"type": "event", "seq", "space", "state"}` |
| `lot/<lot>/status` | lot server | `client.py watch`, anyone | `{"space", "state", "reserved_by", "publisher"}` |
| group `lot_servers` | every lot server | registry only | membership directory; since Milestone 3 used to find a bootstrap peer |
| overlay key `lot/<lot>/summary` | that lot's server (put every 3 s) | every lot server (replica) | `{"free", "occupied", "reserved", "unknown", "total", "lot", "ts"}` with a version |

## Log line format

```json
{"ts": 1791778886.1, "lamport": 42, "process": "lot_server_A", "dir": "send", "peer": "127.0.0.1:5002", "msg": { ...envelope... }}
```

`lamport` (since Milestone 4) is the process's Lamport time for this event.

`dir` is `send`, `recv`, `dropped` (sensor omission) or `info` (a note: retry, timeout,
unreachable peer, bad frame).
