// Sensor node process (Web Worker). Same behaviour as prototype/sensor_node.py:
// random occupancy changes -> "event", full snapshot every heartbeat interval.
// Everything lives in memory, so killing the worker loses it all.

let NAME, LOT, CFG, SERVER;
let spaces = {};
let seq = 0;
let dropRate = 0;

function now() {
  return Date.now() / 1000;
}

function say(text, level) {
  postMessage({ kind: "console", text: text, level: level || "info" });
}

function send(msg) {
  postMessage({
    kind: "disk",
    line: { ts: +now().toFixed(3), process: NAME, dir: "send", peer: SERVER, msg: msg },
  });
  postMessage({ kind: "send", to: SERVER, msg: msg });
}

function reportTruth() {
  // lets the page draw a small "car is physically here" marker
  postMessage({ kind: "truth", lot: LOT, spaces: Object.assign({}, spaces) });
}

function heartbeat() {
  seq++;
  send({ type: "heartbeat", lot: LOT, seq: seq, ts: +now().toFixed(3), spaces: Object.assign({}, spaces) });
}

function emitEvent(sid) {
  seq++;
  const msg = { type: "event", lot: LOT, seq: seq, ts: +now().toFixed(3), space: sid, state: spaces[sid] };
  reportTruth();
  if (Math.random() < dropRate) {
    say("DROPPED event " + seq + ": " + sid + " -> " + spaces[sid] + " (omission)", "warn");
    postMessage({
      kind: "disk",
      line: { ts: +now().toFixed(3), process: NAME, dir: "dropped", peer: SERVER, msg: msg },
    });
    return;
  }
  say("event " + seq + ": " + sid + " -> " + spaces[sid]);
  send(msg);
}

function tick() {
  if (Math.random() >= CFG.flipRate) return;
  const ids = Object.keys(spaces);
  const sid = ids[Math.floor(Math.random() * ids.length)];
  spaces[sid] = spaces[sid] === "occupied" ? "free" : "occupied";
  emitEvent(sid);
}

onmessage = function (e) {
  const d = e.data;

  if (d.kind === "init") {
    NAME = d.name;
    LOT = d.lot;
    CFG = d.cfg;
    SERVER = "lot_server_" + LOT;
    for (let i = 1; i <= CFG.spaces; i++) {
      const id = LOT + "-" + String(i).padStart(2, "0");
      spaces[id] = Math.random() < 0.35 ? "occupied" : "free";
    }
    say("starting with " + CFG.spaces + " spaces, heartbeat every " + CFG.heartbeat + "s");
    heartbeat();
    setInterval(heartbeat, CFG.heartbeat * 1000);
    setInterval(tick, 1000);
    reportTruth();
    return;
  }

  if (d.kind === "cmd" && d.cmd === "dropRate") {
    dropRate = d.value;
  } else if (d.kind === "cmd" && d.cmd === "toggle") {
    // the page asked us to park / unpark a car at a specific space
    const sid = d.space;
    if (!(sid in spaces)) return;
    spaces[sid] = spaces[sid] === "occupied" ? "free" : "occupied";
    emitEvent(sid);
  }
};
