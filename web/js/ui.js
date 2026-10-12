// Rendering: process strip, diagram state, lot maps with their action bar,
// client replies, message log.

let selectedSpace = null;
let driver = "alice";
let replyWaiters = {};
let uiSeq = 0;
const DRIVERS = ["alice", "bob", "carol"];
const lastTileState = {};   // space id -> last state drawn, to pulse tiles that change
const recent = { A: [], B: [] };   // lot -> last few state changes, newest first
const lotLive = {};                // lot -> true once the server has heard from its sensor (so startup is not logged as changes)

// ---- processes ----

function renderProcs() {
  let strip = "";
  for (const n in PROCS) {
    const up = !!workers[n];
    strip += '<span class="proc ' + (up ? "" : "dead") + '" data-p="' + n + '" title="click to ' + (up ? "crash" : "restart") + '"><i></i>' +
      n + "<small>" + (up ? "running" : "down") + "</small></span>";
  }
  $("#procstrip").innerHTML = strip;
  $("#procstrip").querySelectorAll(".proc").forEach(function (b) {
    b.onclick = function () { toggleProcess(b.dataset.p); };
  });
  for (const n in PROCS) {
    const g = document.getElementById("node-" + n);
    if (!g) continue;
    g.classList.toggle("dead", !workers[n]);
    g.querySelector(".st").textContent = workers[n] ? "● running" : "✕ down";
    g.querySelector(".hint").textContent = workers[n] ? "click to crash" : "click to restart";
  }
}

function toggleProcess(name) {
  if (workers[name]) {
    killProcess(name);
    toast(PROCS[name].label + " crashed", "err");
  } else {
    startProcess(name);
    toast(PROCS[name].label + " restarted", "ok");
  }
}

// ---- lot maps ----

const STATE_ICON = { free: "", occupied: "🚗", reserved: "🔒", unknown: "?" };

function tileMeta(sp) {
  if (sp.state === "reserved") return sp.reserved_by + " · " + sp.lease_left + "s";
  if (sp.state === "unknown") return sp.age === null ? "no report" : "silent " + sp.age + "s";
  return sp.age + "s ago";
}

function renderLot(lot) {
  const el = $("#lot" + lot);
  const s = lotState[lot];
  const serverName = "lot_server_" + lot;

  if (!s) {
    el.innerHTML = '<div class="card-head"><h3>Lot ' + lot + '</h3><span class="r" style="color:#fca5a5">server down</span></div>' +
      '<div class="lot-down"><b>Lot server ' + lot + " is not running.</b><br>Clients get no reply for this lot; the other lot keeps working.<br><br>" +
      '<button class="ok sm" data-restart="' + serverName + '">restart lot server ' + lot + "</button></div>";
    el.querySelector("button").onclick = function () { toggleProcess(serverName); };
    return;
  }
  const sm = s.summary;
  const tr = truth[lot] || {};
  const wasLive = lotLive[lot];
  if (sm.unknown < sm.total) lotLive[lot] = true;
  if (!wasLive) for (const sp of s.spaces) lastTileState[sp.id] = undefined;

  let tiles = "";
  for (const sp of s.spaces) {
    const changed = lotLive[lot] && lastTileState[sp.id] !== undefined && lastTileState[sp.id] !== sp.state;
    if (changed) {
      const t = new Date();
      recent[lot].unshift(t.toTimeString().slice(0, 8) + "  " + sp.id + "  " + lastTileState[sp.id] + " → " + sp.state + (sp.reserved_by ? " (" + sp.reserved_by + ")" : ""));
      recent[lot].length = Math.min(recent[lot].length, 6);
    }
    lastTileState[sp.id] = sp.state;
    tiles += '<div class="tile ' + sp.state + (selectedSpace === sp.id ? " sel" : "") + (changed ? " changed" : "") + '" data-id="' + sp.id + '" title="' +
      sp.id + ": " + sp.state + (sp.reserved_by ? " by " + sp.reserved_by : "") + (sp.age !== null ? " · sensor report " + sp.age + "s ago" : "") +
      (tr[sp.id] === "occupied" ? " · car physically present" : "") + '">' +
      '<span class="id">' + sp.id + '<span class="ico">' + STATE_ICON[sp.state] + "</span></span>" +
      '<span class="car ' + (tr[sp.id] === "occupied" ? "on" : "") + '"></span>' +
      '<span class="meta">' + tileMeta(sp) + "</span></div>";
  }

  let others = [];
  for (const l in s.peers) {
    const v = s.peers[l];
    if (v.free === null) others.push("Lot " + l + ': <span class="stale">no summary yet / peer unreachable</span>');
    else others.push("Lot " + l + " as told by its server: " + v.free + " free, " + v.unknown + " unknown · " + v.age + "s old " + (v.stale ? '<span class="stale">STALE</span>' : ""));
  }

  const mine = selectedSpace && selectedSpace.split("-")[0] === lot;
  let actions;
  if (mine) {
    actions =
      '<div class="row"><span class="none">selected <span class="sel-id">' + selectedSpace + "</span></span>" +
      '<span class="who">driver <select id="who-' + lot + '">' + DRIVERS.map(function (d) { return "<option" + (d === driver ? " selected" : "") + ">" + d + "</option>"; }).join("") + "</select></span></div>" +
      '<div class="row">' +
      '<button class="ok" data-act="reserve">reserve</button>' +
      '<button data-act="release">release</button>' +
      '<button data-act="lost" title="Sends the request, ignores the first reply, then retries with the same req_id">reserve · simulate lost reply</button>' +
      '<button data-act="car" title="Tell the sensor node a car arrived at / left this space">car arrives / leaves</button>' +
      "</div>";
  } else {
    actions = '<div class="row"><span class="none">click a space to reserve, release or park a car</span></div>';
  }

  el.innerHTML =
    '<div class="card-head"><h3>Lot ' + lot + '</h3><span class="r">' + sm.total + ' spaces <button class="primary sm" data-act="find">find free</button></span></div>' +
    '<div class="lot-sum"><span class="f"><b>' + sm.free + "</b>free</span>" +
    '<span class="o"><b>' + sm.occupied + "</b>occupied</span>" +
    '<span class="rv"><b>' + sm.reserved + "</b>reserved</span>" +
    '<span class="u"><b>' + sm.unknown + "</b>unknown</span></div>" +
    '<div class="grid">' + tiles + "</div>" +
    '<div class="lot-legend"><span class="f">free</span><span class="o">occupied</span><span class="r">reserved (lease)</span>' +
    '<span class="u">unknown (silent &gt; Δ)</span><span class="c">car physically there</span></div>' +
    '<div class="peerview">' + others.join("<br>") + "</div>" +
    '<div class="actions">' + actions + "</div>" +
    '<div class="recent"><div class="lbl">recent changes</div>' + (recent[lot].length ? recent[lot].map(function (r) { return "<div>" + escapeHtml(r) + "</div>"; }).join("") : "<div class=\"none\">none yet</div>") + "</div>";

  el.querySelectorAll(".tile").forEach(function (t) {
    t.onclick = function () {
      selectedSpace = selectedSpace === t.dataset.id ? null : t.dataset.id;
      renderLot("A");
      renderLot("B");
    };
  });
  el.querySelectorAll("button[data-act]").forEach(function (b) {
    b.onclick = function () {
      const act = b.dataset.act;
      if (act === "find") { request("find", null, false, lot); return; }
      const sid = selectedOrWarn();
      if (!sid) return;
      if (act === "reserve") request("reserve", sid);
      else if (act === "release") request("release", sid);
      else if (act === "lost") request("reserve", sid, true);
      else if (act === "car") toggleCar(sid);
    };
  });
  const who = el.querySelector("select");
  if (who) who.onchange = function () { driver = who.value; };
}

function toggleCar(sid) {
  const w = workers["sensor_node_" + sid.split("-")[0]];
  if (!w) {
    showReply({ text: "that lot's sensor node is down, nothing can see the car", reply: null });
    return;
  }
  w.postMessage({ kind: "cmd", cmd: "toggle", space: sid });
  toast("sensor: car toggled at " + sid);
}

function setDriver(name) {
  driver = name;
  renderLot("A");
  renderLot("B");
}

// ---- client replies ----

function showReply(d) {
  const r = $("#reply");
  const cls = d.neutral ? "" : (!d.reply ? "err" : d.reply.status === "OK" ? "ok" : "warn");
  r.innerHTML = '<span class="lbl">last client reply</span>' + escapeHtml(d.text);
  r.className = "reply " + cls;
  if (!d.neutral && !d.pending) toast(d.text, cls);
}

function onClientReply(d) {
  const waiter = replyWaiters[d.uiId];
  if (waiter) {
    delete replyWaiters[d.uiId];
    waiter(d);
  }
  showReply(d);
}

// send a request through the client process; resolves when the reply (or failure) comes back
function request(op, space, simulateLost, lot) {
  if (!workers.client) {
    showReply({ text: "client process is down, restart it", reply: null });
    return Promise.resolve({ reply: null });
  }
  const uiId = ++uiSeq;
  workers.client.postMessage({
    kind: "cmd", cmd: "request", op: op, space: space || null,
    client: driver, lot: lot || "A", simulateLost: !!simulateLost, uiId: uiId,
  });
  showReply({ text: driver + " → " + op + " " + (space || ("lot " + (lot || "A"))) + " … waiting for reply", reply: null, pending: true, neutral: true });
  return new Promise(function (resolve) { replyWaiters[uiId] = resolve; });
}

function selectedOrWarn() {
  if (!selectedSpace) toast("pick a space first (click a tile)", "warn");
  return selectedSpace;
}

// ---- message log ----

const CHIPS = ["all", "sensor", "client", "peer", "console", "dropped"];
let logFilter = "all";
let procFilter = "all";
let logQuery = "";
let logRows = 0;
let logPaused = false;

function escapeHtml(s) {
  return String(s).replace(/[&<>"]/g, function (c) {
    return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
  });
}

function rowVisible(tr) {
  if (logFilter === "dropped" ? !tr.classList.contains("dropped") : (logFilter !== "all" && tr.dataset.k !== logFilter)) return false;
  if (procFilter !== "all" && tr.dataset.from !== procFilter && tr.dataset.to !== procFilter) return false;
  if (logQuery && tr.textContent.toLowerCase().indexOf(logQuery) < 0) return false;
  return true;
}

function applyLogFilters() {
  $("#log").querySelectorAll("tr.row").forEach(function (tr) {
    const v = rowVisible(tr);
    tr.style.display = v ? "" : "none";
    if (tr.nextSibling && tr.nextSibling.classList.contains("exp")) tr.nextSibling.style.display = v ? "" : "none";
  });
}

function logRow(r) {
  const tb = $("#log");
  const tr = document.createElement("tr");
  tr.className = "row " + r.cls;
  tr.dataset.k = r.cls.split(" ")[0];
  tr.dataset.from = r.from;
  tr.dataset.to = r.to || "";
  const t = new Date();
  const ts = t.toTimeString().slice(0, 8) + "." + String(t.getMilliseconds()).padStart(3, "0");
  tr.innerHTML = "<td>" + ts + "</td><td>" + r.from + (r.to ? " → " + r.to : "") + "</td>" +
    '<td class="type">' + r.type + '</td><td class="detail">' + escapeHtml(r.detail) + "</td>";
  tr.style.display = rowVisible(tr) ? "" : "none";
  tr.onclick = function () {
    const next = tr.nextSibling;
    if (next && next.classList.contains("exp")) { next.remove(); return; }
    const ex = document.createElement("tr");
    ex.className = "exp";
    const body = r.raw ? JSON.stringify(r.raw, null, 2) : r.detail;
    ex.innerHTML = '<td colspan="4">' + escapeHtml(body) + "</td>";
    tr.after(ex);
  };
  tb.appendChild(tr);
  logRows++;
  while (logRows > 800) {
    const first = tb.firstChild;
    if (first.nextSibling && first.nextSibling.classList.contains("exp")) tb.removeChild(first.nextSibling);
    tb.removeChild(first);
    logRows--;
  }
  $("#logcount").textContent = logRows + " rows";
  if ($("#autoscroll").checked && !logPaused) $("#logwrap").scrollTop = $("#logwrap").scrollHeight;
}

function clearLog() {
  $("#log").innerHTML = "";
  logRows = 0;
  $("#logcount").textContent = "0 rows";
}

function renderChips() {
  $("#chips").innerHTML = CHIPS.map(function (c) {
    return '<span class="chip ' + (logFilter === c ? "on" : "") + '" data-c="' + c + '">' + c + "</span>";
  }).join("");
  $("#chips").querySelectorAll(".chip").forEach(function (ch) {
    ch.onclick = function () { logFilter = ch.dataset.c; renderChips(); applyLogFilters(); };
  });
  const procs = ["all"].concat(Object.keys(PROCS));
  $("#pchips").innerHTML = procs.map(function (p) {
    return '<span class="chip ' + (procFilter === p ? "on" : "") + '" data-p="' + p + '" title="only rows sent or received by this process">' + (p === "all" ? "any process" : p) + "</span>";
  }).join("");
  $("#pchips").querySelectorAll(".chip").forEach(function (ch) {
    ch.onclick = function () { procFilter = ch.dataset.p; renderChips(); applyLogFilters(); };
  });
}
