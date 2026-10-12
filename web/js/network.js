// The page plays the role of the network. Workers can't talk to each other
// directly, so every message goes worker -> page -> worker. That is where we
// add latency, drop messages, cut links, and write everything to the log view.
//
// The page keeps no parking state. All of that is inside the workers.

const workers = {};      // name -> Worker, missing when the process is dead
const disks = {};        // name -> array of log lines (the process's "file on disk")
const lotState = {};     // lot -> last state snapshot from that server (for drawing)
const truth = {};        // lot -> what the sensor says is physically there

const linkUp = {};
for (const k in LINKS) linkUp[k] = true;

let latency = 250;       // ms
let dropRate = 0;        // 0..1
let msgCount = 0;

function describeMsg(m) {
  switch (m.type) {
    case "heartbeat": {
      const c = {};
      for (const s of Object.values(m.spaces)) c[s] = (c[s] || 0) + 1;
      return "seq " + m.seq + " snapshot: " + (c.free || 0) + " free, " + (c.occupied || 0) + " occupied";
    }
    case "event":
      return "seq " + m.seq + " " + m.space + " -> " + m.state;
    case "summary":
      return "Lot " + m.lot + ": " + m.free + " free, " + m.occupied + " occupied, " + m.reserved + " reserved, " + m.unknown + " unknown";
    case "summary_ack":
      return "ack from Lot " + m.lot;
    case "find":
      return "req " + m.req_id + " from " + m.client;
    case "reserve":
    case "release":
      return "req " + m.req_id + " " + m.client + " -> " + m.space;
    case "reply":
      if (m.op === "find") {
        const others = Object.entries(m.other_lots).map(function (kv) {
          return kv[0] + "=" + (kv[1].free === null ? "?" : kv[1].free) + (kv[1].stale ? " STALE" : "");
        });
        return "req " + m.req_id + " OK: " + m.free.length + " free in Lot " + m.lot + "; others: " + others.join(" ");
      }
      return "req " + m.req_id + " " + m.status + " " + (m.reason || "") + (m.duplicate ? " (duplicate, cached reply)" : "");
    default:
      return JSON.stringify(m);
  }
}

function netSend(from, to, msg) {
  msgCount++;
  $("#msgcount").textContent = msgCount + " msgs";

  let kind = "peer";
  if ((PROCS[from] && PROCS[from].kind === "sensor") || (PROCS[to] && PROCS[to].kind === "sensor")) kind = "sensor";
  else if (from === "client" || to === "client") kind = "client";

  const key = linkKey(from, to);
  let dropped = null;
  if (!workers[to]) dropped = "undeliverable: " + to + " is down";
  else if (linkUp[key] === false) dropped = "link " + LINKS[key] + " is cut";
  else if (Math.random() < dropRate) dropped = "lost in the network";

  logRow({
    cls: kind + (dropped ? " dropped" : ""),
    from: from,
    to: to,
    type: msg.type,
    detail: describeMsg(msg) + (dropped ? "  ✕ " + dropped : ""),
    raw: msg,
  });
  animateDot(from, to, dropped ? "dropped" : kind);
  if (dropped) return;

  setTimeout(function () {
    if (workers[to]) workers[to].postMessage({ kind: "msg", from: from, msg: msg });
  }, latency);
}

function startProcess(name) {
  if (workers[name]) return;
  const p = PROCS[name];
  const w = new Worker(p.file);
  workers[name] = w;
  disks[name] = disks[name] || [];

  w.onmessage = function (e) {
    const d = e.data;
    if (d.kind === "send") {
      netSend(name, d.to, d.msg);
    } else if (d.kind === "disk") {
      disks[name].push(d.line);
    } else if (d.kind === "console") {
      logRow({ cls: "console " + (d.level || ""), from: name, to: "", type: "console", detail: d.text });
    } else if (d.kind === "state") {
      lotState[d.lot] = d;
      renderLot(d.lot);
    } else if (d.kind === "truth") {
      truth[d.lot] = d.spaces;
      renderLot(d.lot);
    } else if (d.kind === "reply") {
      onClientReply(d);
    }
  };

  const peers = [];
  if (p.kind === "server") {
    for (const q of Object.values(PROCS)) {
      if (q.kind === "server" && q.lot !== p.lot) peers.push(q.lot);
    }
  }
  w.postMessage({
    kind: "init",
    name: name,
    lot: p.lot,
    peers: peers,
    cfg: CFG,
    disk: p.kind === "server" ? disks[name].slice() : [],   // servers get their log back
  });
  if (p.kind === "sensor") {
    w.postMessage({ kind: "cmd", cmd: "dropRate", value: +$("#omit").value });
  }

  let note = "started";
  if (p.kind === "server" && disks[name].length) note += ", replaying " + disks[name].length + " log lines from disk";
  logRow({ cls: "console demo", from: name, to: "", type: "process", detail: note });
  renderProcs();
}

function killProcess(name) {
  const w = workers[name];
  if (!w) return;
  w.terminate();
  delete workers[name];
  if (PROCS[name].kind === "server") {
    delete lotState[PROCS[name].lot];
    renderLot(PROCS[name].lot);
  }
  let note = "KILLED (crash), memory is gone";
  if (PROCS[name].kind === "server") note += ", disk log survives";
  logRow({ cls: "console err", from: name, to: "", type: "process", detail: note });
  renderProcs();
}

function setLink(key, up) {
  linkUp[key] = up;
  const edge = document.getElementById("edge-" + key);
  if (edge) edge.classList.toggle("cut", !up);
  logRow({
    cls: "console demo", from: "network", to: "", type: "link",
    detail: LINKS[key] + (up ? " restored" : " CUT, messages on it are dropped"),
  });
}

// full reset: every process restarted from scratch, logs on disk wiped, links restored
function resetDemo() {
  for (const n in PROCS) {
    if (workers[n]) { workers[n].terminate(); delete workers[n]; }
    disks[n] = [];
  }
  for (const l in lotState) delete lotState[l];
  for (const l in truth) delete truth[l];
  for (const k in LINKS) {
    linkUp[k] = true;
    const edge = document.getElementById("edge-" + k);
    if (edge) edge.classList.remove("cut");
  }
  selectedSpace = null;
  for (const l in recent) { recent[l] = []; lotLive[l] = false; }
  for (const k in lastTileState) delete lastTileState[k];
  msgCount = 0;
  $("#msgcount").textContent = "0 msgs";
  clearLog();
  logRow({ cls: "console demo", from: "demo", to: "", type: "reset", detail: "reset: all processes restarted, disks wiped, links restored" });
  ["lot_server_A", "lot_server_B", "sensor_node_A", "sensor_node_B", "client"].forEach(startProcess);
  renderLot("A");
  renderLot("B");
  showReply({ text: "-", reply: null, neutral: true });
  toast("Demo reset", "ok");
}

function downloadLogs() {
  const all = [];
  for (const name in disks) all.push.apply(all, disks[name]);
  all.sort(function (a, b) { return a.ts - b.ts; });
  const text = all.map(function (l) { return JSON.stringify(l); }).join("\n") + "\n";
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([text], { type: "application/x-ndjson" }));
  a.download = "smart_parking_logs.jsonl";
  a.click();
}
