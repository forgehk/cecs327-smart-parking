// Lot server process (Web Worker). Same rules as prototype/lot_server.py.
// It only talks to the outside world through postMessage, which the page
// treats as "the network".

let NAME, LOT, PEERS = [], CFG;
let spaces = {};
let seenRequests = {};
let peerSummaries = {};
let lastSensorSeq = -1;
let lastState = {};
let peerStale = {};

function now() {
  return Date.now() / 1000;
}

function send(to, msg) {
  postMessage({ kind: "send", to: to, msg: msg });
}

function disk(dir, peer, msg) {
  postMessage({
    kind: "disk",
    line: { ts: +now().toFixed(3), process: NAME, dir: dir, peer: peer, msg: msg },
  });
}

function say(text, level) {
  postMessage({ kind: "console", text: text, level: level || "info" });
}

// ---- state ----

function stateOf(sp, t) {
  t = t || now();
  if (sp.reserved_by && t < sp.lease_end) return "reserved";
  if (t - sp.sensor_ts > CFG.delta) return "unknown";
  return sp.sensor_state;
}

function summary(t) {
  t = t || now();
  const c = { free: 0, occupied: 0, reserved: 0, unknown: 0, total: 0 };
  for (const sp of Object.values(spaces)) {
    c[stateOf(sp, t)]++;
    c.total++;
  }
  return c;
}

function applySensor(msg, t) {
  if (msg.type === "heartbeat") {
    for (const sid in msg.spaces) {
      if (spaces[sid]) {
        spaces[sid].sensor_state = msg.spaces[sid];
        spaces[sid].sensor_ts = t;
      }
    }
  } else if (msg.type === "event" && spaces[msg.space]) {
    spaces[msg.space].sensor_state = msg.state;
    spaces[msg.space].sensor_ts = t;
  }
}

function replay(lines) {
  let n = 0;
  for (const line of lines) {
    const m = line.msg || {};
    if (line.dir === "recv" && (m.type === "heartbeat" || m.type === "event")) {
      applySensor(m, line.ts);
      n++;
    } else if (line.dir === "send" && m.type === "reply" && m.status === "OK") {
      const sp = spaces[m.space];
      if (!sp) continue;
      if (m.op === "reserve") {
        sp.reserved_by = m.client;
        sp.lease_end = line.ts + CFG.lease;
        n++;
      } else if (m.op === "release") {
        sp.reserved_by = null;
        sp.lease_end = 0;
        n++;
      }
    }
  }
  if (n > 0) {
    const s = summary();
    say("RECOVERY: replayed " + n + " log entries -> " + s.free + " free, " + s.occupied +
        " occupied, " + s.reserved + " reserved, " + s.unknown + " unknown" +
        " (old sensor data counts as unknown until the next heartbeat)", "recover");
  }
}

function peerView(t) {
  t = t || now();
  const out = {};
  for (const lot in peerSummaries) {
    const [summ, received] = peerSummaries[lot];
    const age = +(t - received).toFixed(1);
    out[lot] = { free: summ.free, unknown: summ.unknown, age: age, stale: age > CFG.peerStale };
  }
  for (const lot of PEERS) {
    if (!out[lot]) out[lot] = { free: null, unknown: null, age: null, stale: true };
  }
  return out;
}

// ---- client requests ----

function processClient(msg) {
  const t = now();
  const op = msg.type;
  const client = msg.client || "?";
  const sid = msg.space;
  const reply = { type: "reply", op: op, req_id: msg.req_id, client: client, lot: LOT, space: sid, ts: +t.toFixed(3) };

  if (op === "find") {
    const free = [];
    for (const sp of Object.values(spaces)) {
      if (stateOf(sp, t) === "free") free.push(sp.id);
    }
    say("FIND from " + client + ": " + free.length + " free");
    reply.status = "OK";
    reply.free = free;
    reply.lot_summary = summary(t);
    reply.other_lots = peerView(t);
    return reply;
  }

  const sp = spaces[sid];
  if (!sp) {
    reply.status = "DENIED";
    reply.reason = "no such space in this lot";
    return reply;
  }
  const st = stateOf(sp, t);

  if (op === "reserve") {
    if (st === "reserved" && sp.reserved_by === client) {
      reply.status = "OK";
      reply.reason = "already held by you";
      reply.lease = CFG.lease;
    } else if (st === "free") {
      sp.reserved_by = client;
      sp.lease_end = t + CFG.lease;
      reply.status = "OK";
      reply.reason = "reserved";
      reply.lease = CFG.lease;
      say("RESERVE " + sid + " by " + client + ": OK (" + CFG.lease + "s lease)", "ok");
    } else {
      reply.status = "DENIED";
      reply.reason = st;
      say("RESERVE " + sid + " by " + client + ": DENIED (" + st + ")", "warn");
    }
  } else if (op === "release") {
    if (st === "reserved" && sp.reserved_by === client) {
      sp.reserved_by = null;
      sp.lease_end = 0;
      reply.status = "OK";
      reply.reason = "released";
      say("RELEASE " + sid + " by " + client + ": OK", "ok");
    } else {
      reply.status = "DENIED";
      reply.reason = st === "reserved" ? "not reserved by you" : st;
      say("RELEASE " + sid + " by " + client + ": DENIED", "warn");
    }
  } else {
    reply.status = "DENIED";
    reply.reason = "unknown op " + op;
  }
  return reply;
}

// ---- background loops ----

function gossip() {
  const msg = Object.assign({ type: "summary", lot: LOT, ts: +now().toFixed(3) }, summary());
  for (const lot of PEERS) {
    const to = "lot_server_" + lot;
    disk("send", to, msg);
    send(to, msg);
  }
}

function monitor() {
  const t = now();
  for (const sid in spaces) {
    const sp = spaces[sid];
    const st = stateOf(sp, t);
    if (st === lastState[sid]) continue;
    if (st === "unknown") {
      say("STALE " + sid + ": nothing from sensor for >" + CFG.delta + "s -> unknown", "warn");
    } else if (lastState[sid] === "reserved") {
      say("LEASE " + sid + ": reservation by " + sp.reserved_by + " expired -> " + st, "warn");
      sp.reserved_by = null;
      sp.lease_end = 0;
    }
    lastState[sid] = st;
  }

  const pv = peerView(t);
  for (const lot of PEERS) {
    if (pv[lot].age === null) continue;
    const was = peerStale[lot] || false;
    if (pv[lot].stale && !was) {
      say("PEER " + lot + ": last summary is " + pv[lot].age + "s old -> STALE (still serving own lot)", "warn");
    } else if (!pv[lot].stale && was) {
      say("PEER " + lot + ": fresh summary, no longer stale", "ok");
    }
    peerStale[lot] = pv[lot].stale;
  }
  snapshot();
}

function snapshot() {
  const t = now();
  const list = Object.values(spaces).map(function (sp) {
    const st = stateOf(sp, t);
    return {
      id: sp.id,
      state: st,
      sensor_state: sp.sensor_state,
      age: sp.sensor_ts ? +(t - sp.sensor_ts).toFixed(1) : null,
      reserved_by: st === "reserved" ? sp.reserved_by : null,
      lease_left: st === "reserved" ? Math.max(0, Math.round(sp.lease_end - t)) : null,
    };
  });
  postMessage({ kind: "state", lot: LOT, summary: summary(t), peers: peerView(t), spaces: list });
}

// ---- message handling ----

onmessage = function (e) {
  const d = e.data;

  if (d.kind === "init") {
    NAME = d.name;
    LOT = d.lot;
    PEERS = d.peers;
    CFG = d.cfg;
    for (let i = 1; i <= CFG.spaces; i++) {
      const id = LOT + "-" + String(i).padStart(2, "0");
      spaces[id] = { id: id, sensor_state: "unknown", sensor_ts: 0, reserved_by: null, lease_end: 0 };
    }
    replay(d.disk || []);
    for (const sid in spaces) lastState[sid] = stateOf(spaces[sid]);
    say("listening, peers=" + (PEERS.join(",") || "none") + ", delta=" + CFG.delta + "s, lease=" + CFG.lease + "s");
    setInterval(gossip, CFG.peerInterval * 1000);
    setInterval(monitor, 1000);
    setInterval(snapshot, 400);
    snapshot();
    return;
  }

  if (d.kind !== "msg") return;
  const from = d.from;
  const msg = d.msg;
  disk("recv", from, msg);

  if (msg.type === "heartbeat" || msg.type === "event") {
    const t = now();
    const unknownBefore = summary(t).unknown;
    applySensor(msg, t);
    if (msg.type === "event") say("EVENT " + msg.space + " -> " + msg.state + " (seq " + msg.seq + ")");
    if (lastSensorSeq >= 0 && msg.seq > lastSensorSeq + 1) {
      say("OMISSION: sensor seq jumped " + lastSensorSeq + " -> " + msg.seq + ", next heartbeat repairs it", "warn");
    }
    lastSensorSeq = msg.seq;
    const unknownAfter = summary(t).unknown;
    if (unknownBefore > unknownAfter) {
      say((unknownBefore - unknownAfter) + " space(s) no longer unknown after fresh report", "ok");
    }
  } else if (msg.type === "find" || msg.type === "reserve" || msg.type === "release") {
    let reply;
    if (msg.req_id && seenRequests[msg.req_id]) {
      reply = Object.assign({}, seenRequests[msg.req_id], { duplicate: true });
      say("DUPLICATE req " + msg.req_id + " -> resending cached reply", "warn");
    } else {
      reply = processClient(msg);
      if (msg.req_id) seenRequests[msg.req_id] = reply;
    }
    disk("send", from, reply);
    send(from, reply);
  } else if (msg.type === "summary") {
    peerSummaries[msg.lot] = [msg, now()];
    const ack = { type: "summary_ack", lot: LOT, ts: +now().toFixed(3) };
    disk("send", from, ack);
    send(from, ack);
  }
  snapshot();
};
