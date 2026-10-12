# Message reference (Milestone 2)
Every message is one JSON object, sent as a 4-byte big-endian length followed by the UTF-8 JSON body (`comm/framing.py`).
All messages share the envelope from `comm/envelope.py`:
| field | meaning |
|---|---|
| `type` | what kind of message, used to dispatch |
| `msg_id` | unique id (8 hex chars); a REPLY/ACK echoes it in `reply_to` |
| `sender` | process name, for the logs |
| `timestamp` | ISO 8601 with milliseconds, sender's clock |
| `reply_to` | `msg_id` of the message this answers, else `null` |
| `payload` | type-specific data |

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

## Payloads

| type | payload | answered by |
|---|---|---|
| REQUEST | `{"op": "find"\|"reserve"\|"release", "args": {"client": ..., "space": ...}}` | REPLY |
| REPLY | `{"status": "OK", "result": {...}}` or `{"status": "ERROR", "error": "text"}`; `duplicate: true` on a cached retry | - |
| SUBSCRIBE / UNSUBSCRIBE | `{"topic": "lot/A/sensor"}` | ACK |
| PUBLISH | `{"topic": ..., "data": {...}}` | ACK `{"delivered": n}` |
| EVENT | `{"topic": ..., "data": {...}, "publisher": sender}` | - |
| JOIN / LEAVE | `{"group": "lot_servers", "address": "127.0.0.1:5001"}` | ACK |
| MEMBERS | `{"group": ...}` | ACK `{"members": [...]}` |
| GROUP_MSG | `{"group": ..., "data": {...}}` | - |

Business outcomes are inside `result`, not in `status`: a denied reservation is
`status: OK, result: {granted: false, reason: "reserved"}`. `status: ERROR` is only for
protocol problems (unknown operation, bad arguments, handler crash).

## Topics and groups

| name | publisher | subscribers | data |
|---|---|---|---|
| `lot/<lot>/sensor` | sensor node | that lot's server | `{"type": "heartbeat", "seq", "spaces": {...}}` or `{"type": "event", "seq", "space", "state"}` |
| `lot/<lot>/status` | lot server | `client.py watch`, anyone | `{"space", "state", "reserved_by", "publisher"}` |
| group `lot_servers` | every lot server | every lot server | `{"kind": "summary", "lot", "free", "occupied", "reserved", "unknown", "total"}` |

## Log line format

```json
{"ts": 1791778886.1, "process": "lot_server_A", "dir": "send", "peer": "127.0.0.1:5002", "msg": { ...envelope... }}
```

`dir` is `send`, `recv`, `dropped` (sensor omission) or `info` (a note: retry, timeout,
unreachable peer, bad frame).
