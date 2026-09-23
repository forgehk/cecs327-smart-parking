// Client process (Web Worker). Same behaviour as prototype/client.py:
// one req_id per request, wait for the reply, retry with the same id on timeout.

let NAME, CFG;
let pending = {};

function now() {
  return Date.now() / 1000;
}

function say(text, level) {
  postMessage({ kind: "console", text: text, level: level || "info" });
}

function disk(dir, peer, msg) {
  postMessage({
    kind: "disk",
    line: { ts: +now().toFixed(3), process: NAME, dir: dir, peer: peer, msg: msg },
  });
}

function attempt(reqId) {
  const p = pending[reqId];
  if (!p) return;
  p.attempt++;

  if (p.attempt > CFG.retries) {
    delete pending[reqId];
    say(p.client + " <- FAILED, no reply after " + CFG.retries + " attempts (" + p.to + " down or link broken)", "err");
    postMessage({ kind: "reply", uiId: p.uiId, reply: null, text: "FAILED: no reply after " + CFG.retries + " attempts" });
    return;
  }
  if (p.attempt > 1) {
    say("timeout after " + CFG.timeout + "s, retry " + p.attempt + "/" + CFG.retries + " with same req_id " + reqId, "warn");
  }
  disk("send", p.to, p.msg);
  postMessage({ kind: "send", to: p.to, msg: p.msg });
  p.timer = setTimeout(function () { attempt(reqId); }, CFG.timeout * 1000);
}

function describeReply(r) {
  if (r.op === "find") {
    const s = r.lot_summary;
    let text = "OK Lot " + r.lot + ": " + s.free + " free, " + s.occupied + " occupied, " +
      s.reserved + " reserved, " + s.unknown + " unknown. Free: " + (r.free.join(", ") || "(none)") + ". ";
    const others = [];
    for (const lot in r.other_lots) {
      const v = r.other_lots[lot];
      if (v.free === null) others.push("Lot " + lot + ": no summary (peer unreachable)");
      else others.push("Lot " + lot + ": " + v.free + " free, " + v.unknown + " unknown (" + v.age + "s old" + (v.stale ? ", STALE" : "") + ")");
    }
    return text + others.join("; ");
  }
  let text = r.status + " " + (r.reason || "");
  if (r.lease) text += " (lease " + r.lease + "s)";
  return text;
}

onmessage = function (e) {
  const d = e.data;

  if (d.kind === "init") {
    NAME = d.name;
    CFG = d.cfg;
    say("ready");
    return;
  }

  if (d.kind === "cmd" && d.cmd === "request") {
    const targetLot = d.space ? d.space.split("-")[0] : d.lot;
    const to = "lot_server_" + targetLot;
    const reqId = Math.random().toString(16).slice(2, 10);
    const msg = { type: d.op, req_id: reqId, client: d.client, ts: +now().toFixed(3) };
    if (d.space) msg.space = d.space;
    pending[reqId] = { attempt: 0, msg: msg, to: to, uiId: d.uiId, simulateLost: d.simulateLost, client: d.client, timer: null };
    say(d.client + " -> " + d.op + " " + (d.space || "") + " (req_id " + reqId + ") to " + to);
    attempt(reqId);
    return;
  }

  if (d.kind === "msg" && d.msg.type === "reply") {
    const r = d.msg;
    const p = pending[r.req_id];
    disk("recv", d.from, r);
    if (!p) return;
    if (p.simulateLost && p.attempt === 1) {
      say("(demo) reply to " + r.req_id + " lost on the wire, will retry with the same req_id", "warn");
      return;
    }
    clearTimeout(p.timer);
    delete pending[r.req_id];
    let text = describeReply(r);
    if (r.duplicate) text += " - duplicate: server returned its cached reply";
    say(p.client + " <- " + text, r.status === "OK" ? "ok" : "warn");
    postMessage({ kind: "reply", uiId: p.uiId, reply: r, text: text });
  }
};
