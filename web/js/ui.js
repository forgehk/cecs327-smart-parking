// Rendering: process pills, fault controls, lot maps, client panel, message log.

let selectedSpace = null;
let replyWaiters = {};
let uiSeq = 0;

// ---- processes and links ----

function renderProcs() {
  let pills = "";
  let rows = "";
  for (const n in PROCS) {
    const up = !!workers[n];
    pills += '<span class="pill ' + (up ? "" : "dead") + '"><i></i>' + n + "</span>";
    rows += "<span>" + PROCS[n].label + ' <span class="hint">' + (up ? "running" : "DOWN") + "</span></span>" +
      '<button class="' + (up ? "danger" : "ok") + '" data-p="' + n + '">' + (up ? "kill" : "restart") + "</button>";
  }
  $("#pills").innerHTML = pills;
  $("#procs").innerHTML = rows;
  $("#procs").querySelectorAll("button").forEach(function (b) {
    b.onclick = function () {
      if (workers[b.dataset.p]) killProcess(b.dataset.p);
      else startProcess(b.dataset.p);
    };
  });
  for (const n in PROCS) {
    const g = document.getElementById("node-" + n);
    if (!g) continue;
    g.classList.toggle("dead", !workers[n]);
    g.querySelector(".st").textContent = workers[n] ? "● running" : "✕ down";
  }
}

function renderLinks() {
  let html = "";
  for (const k in LINKS) {
    html += '<label><input type="checkbox" data-l="' + k + '" ' + (linkUp[k] ? "checked" : "") + "> " + LINKS[k] + "</label>";
  }
  $("#links").innerHTML = html;
  $("#links").querySelectorAll("input").forEach(function (i) {
    i.onchange = function () { setLink(i.dataset.l, i.checked); };
  });
}

// ---- lot maps ----

function tileMeta(sp) {
  if (sp.state === "reserved") return sp.reserved_by + " " + sp.lease_left + "s";
  if (sp.state === "unknown") return sp.age === null ? "no report" : "silent " + sp.age + "s";
  return sp.age + "s ago";
}

function renderLot(lot) {
  const el = $("#lot" + lot);
  const s = lotState[lot];
  if (!s) {
    el.innerHTML = "<h2>Lot " + lot + ' <span class="r" style="color:var(--err)">server down</span></h2>' +
      '<div class="hint">Lot server ' + lot + " is not running. Clients get no reply for this lot; the other lot keeps working.</div>";
    return;
  }
  const sm = s.summary;
  const tr = truth[lot] || {};

  let tiles = "";
  for (const sp of s.spaces) {
    tiles += '<div class="tile ' + sp.state + (selectedSpace === sp.id ? " sel" : "") + '" data-id="' + sp.id + '" title="' +
      sp.id + ": " + sp.state + (sp.age !== null ? " (sensor report " + sp.age + "s ago)" : "") + '">' +
      '<span class="id">' + sp.id + "</span>" +
      '<span class="car ' + (tr[sp.id] === "occupied" ? "on" : "") + '" title="car physically present"></span>' +
      '<span class="meta">' + tileMeta(sp) + "</span></div>";
  }

  let others = [];
  for (const l in s.peers) {
    const v = s.peers[l];
    if (v.free === null) others.push("Lot " + l + ': <span class="stale">no summary yet / peer unreachable</span>');
    else others.push("Lot " + l + " (as told by its server): " + v.free + " free, " + v.unknown + " unknown · " + v.age + "s old " + (v.stale ? '<span class="stale">STALE</span>' : ""));
  }

  el.innerHTML =
    "<h2>Lot " + lot + ' <span class="r">' + sm.total + " spaces</span></h2>" +
    '<div class="lot-sum"><span style="color:var(--free)"><b>' + sm.free + "</b> free</span>" +
    '<span style="color:#cbd5e1"><b>' + sm.occupied + "</b> occupied</span>" +
    '<span style="color:#93c5fd"><b>' + sm.reserved + "</b> reserved</span>" +
    '<span style="color:var(--unknown)"><b>' + sm.unknown + "</b> unknown</span></div>" +
    '<div class="grid">' + tiles + "</div>" +
    '<div class="legend"><span class="f">free</span><span class="o">occupied</span><span class="r">reserved (lease)</span>' +
    '<span class="u">unknown (no report within Δ)</span><span>◦ = car physically present</span></div>' +
    '<div class="peerview">' + others.join("<br>") + "</div>";

  el.querySelectorAll(".tile").forEach(function (t) {
    t.onclick = function () {
      selectedSpace = t.dataset.id;
      $("#space").value = selectedSpace;
      renderLot("A");
      renderLot("B");
    };
  });
}

// ---- client panel ----

function showReply(d) {
  const r = $("#reply");
  r.textContent = d.text;
  r.className = "reply " + (!d.reply ? "err" : d.reply.status === "OK" ? "ok" : "warn");
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
    client: $("#who").value, lot: lot || "A", simulateLost: !!simulateLost, uiId: uiId,
  });
  $("#reply").textContent = op + " " + (space || "") + " ... waiting for reply";
  $("#reply").className = "reply";
  return new Promise(function (resolve) { replyWaiters[uiId] = resolve; });
}

function selectedOrWarn() {
  const v = $("#space").value.trim().toUpperCase();
  if (!v) showReply({ text: "pick a space first (click a tile)", reply: null });
  return v;
}

// ---- message log ----

const CHIPS = ["all", "sensor", "client", "peer", "console", "dropped"];
let logFilter = "all";
let logRows = 0;

function escapeHtml(s) {
  return String(s).replace(/[&<>]/g, function (c) {
    return { "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c];
  });
}

function rowVisible(tr) {
  if (logFilter === "all") return true;
  if (logFilter === "dropped") return tr.classList.contains("dropped");
  return tr.dataset.k === logFilter;
}

function logRow(r) {
  const tb = $("#log");
  const tr = document.createElement("tr");
  tr.className = r.cls;
  tr.dataset.k = r.cls.split(" ")[0];
  const t = new Date();
  const ts = t.toTimeString().slice(0, 8) + "." + String(t.getMilliseconds()).padStart(3, "0");
  tr.innerHTML = "<td>" + ts + "</td><td>" + r.from + (r.to ? " → " + r.to : "") + "</td>" +
    '<td class="type">' + r.type + '</td><td class="detail">' + escapeHtml(r.detail) + "</td>";
  tr.style.display = rowVisible(tr) ? "" : "none";
  tb.appendChild(tr);
  logRows++;
  while (logRows > 600) {
    tb.removeChild(tb.firstChild);
    logRows--;
  }
  if ($("#autoscroll").checked) $("#logwrap").scrollTop = $("#logwrap").scrollHeight;
}

function renderChips() {
  $("#chips").innerHTML = CHIPS.map(function (c) {
    return '<span class="chip ' + (logFilter === c ? "on" : "") + '" data-c="' + c + '">' + c + "</span>";
  }).join("");
  $("#chips").querySelectorAll(".chip").forEach(function (ch) {
    ch.onclick = function () {
      logFilter = ch.dataset.c;
      renderChips();
      $("#log").querySelectorAll("tr").forEach(function (tr) {
        tr.style.display = rowVisible(tr) ? "" : "none";
      });
    };
  });
}
