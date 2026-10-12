// Scripted demo scenarios. Each one is a list of steps. The runner shows the
// steps, highlights the one it is on, points at the control it uses, and can
// be stopped between steps.

let scenarioRunning = false;
let stopRequested = false;

function sleep(ms) {
  return new Promise(function (r) { setTimeout(r, ms); });
}

// wait with a progress bar under the current step; resolves early if stopped
async function wait(ms, label) {
  const step = $("#steps .step.now");
  let bar = null;
  if (step) {
    const w = document.createElement("div");
    w.className = "wait";
    w.innerHTML = "<i></i>";
    step.querySelector("span").appendChild(w);
    if (label) {
      const s = document.createElement("span");
      s.className = "sub";
      s.textContent = label;
      step.querySelector("span").appendChild(s);
    }
    bar = w.firstChild;
  }
  const t0 = performance.now();
  while (performance.now() - t0 < ms) {
    if (stopRequested) return;
    if (bar) bar.style.width = Math.min(100, (performance.now() - t0) / ms * 100) + "%";
    await sleep(100);
  }
  if (bar) bar.style.width = "100%";
}

function firstFree(lot) {
  const s = lotState[lot];
  if (!s) return null;
  const sp = s.spaces.find(function (x) { return x.state === "free"; });
  return sp ? sp.id : null;
}

function selectSpace(sid) {
  selectedSpace = sid;
  renderLot("A");
  renderLot("B");
  highlight(document.querySelector('.tile[data-id="' + sid + '"]'));
}

function hlNode(name) { highlight(document.getElementById("node-" + name)); }
function hlEdge(key) { highlight(document.getElementById("edge-" + key)); }
function hlLot(lot) { highlight($("#lot" + lot)); }

function note(text) {
  logRow({ cls: "console demo", from: "demo", to: "", type: "narrator", detail: text });
}

// ---- the scenarios ----

const SCENARIOS = [
  {
    title: "Request / response, one driver per space",
    steps: [
      { text: "Client asks Lot A for its free spaces.", run: async function (c) { hlLot("A"); await request("find", null, false, "A"); await wait(600); } },
      { text: "alice reserves the first free space. Expect OK with a 30 s lease.", run: async function (c) {
        c.sid = firstFree("A"); if (!c.sid) throw new Error("No free space in Lot A right now, try again.");
        setDriver("alice"); selectSpace(c.sid); await request("reserve", c.sid); await wait(600); } },
      { text: "bob tries the same space. Expect DENIED reserved: the lot server is the only writer for its spaces and handles the two requests one after the other.", run: async function (c) { setDriver("bob"); await request("reserve", c.sid); await wait(600); } },
      { text: "bob tries to release it. DENIED, it is not his.", run: async function (c) { await request("release", c.sid); await wait(600); } },
      { text: "alice releases it. OK, the tile goes back to free.", run: async function (c) { setDriver("alice"); await request("release", c.sid); } },
    ],
  },
  {
    title: "Lost reply, idempotent retry",
    steps: [
      { text: "carol reserves a space, but the client throws away the first reply on purpose.", run: async function (c) {
        c.sid = firstFree("A"); if (!c.sid) throw new Error("No free space in Lot A right now.");
        setDriver("carol"); selectSpace(c.sid); c.p = request("reserve", c.sid, true); await wait(300); } },
      { text: "After the 2 s timeout the client retries with the same req_id. Watch the log: the server answers DUPLICATE from its reply cache.", run: async function (c) { hlEdge("client|lot_server_A"); await c.p; } },
      { text: "Result: exactly one reservation, held by carol. A retry can never double-book.", run: async function (c) { highlight(document.querySelector('.tile[data-id="' + c.sid + '"]')); await wait(500); } },
    ],
  },
  {
    title: "Sensor node crash: spaces become unknown",
    steps: [
      { text: "Crash sensor node A. The lot server just hears nothing; it cannot tell a dead sensor from a cut link.", run: async function () { hlNode("sensor_node_A"); await wait(800); killProcess("sensor_node_A"); } },
      { text: "Rule: no report within Δ = 6 s means a space becomes unknown. Silence never turns into free.", run: async function () { hlLot("A"); await wait(7000, "waiting 7 s for Δ to expire"); } },
      { text: "A find on Lot A now returns zero free spaces. The total still shows 20 so you can see how much visibility was lost. Lot B is unaffected.", run: async function () { await request("find", null, false, "A"); await wait(1200); } },
      { text: "Restart the sensor. Its first heartbeat carries a full snapshot, so every space leaves unknown at once.", run: async function () { hlNode("sensor_node_A"); startProcess("sensor_node_A"); await wait(1500); } },
    ],
  },
  {
    title: "Lot server crash and recovery from log",
    steps: [
      { text: "alice reserves a space in Lot A.", run: async function (c) {
        c.sid = firstFree("A"); if (!c.sid) throw new Error("No free space in Lot A right now.");
        setDriver("alice"); selectSpace(c.sid); await request("reserve", c.sid); await wait(600); } },
      { text: "Crash lot server A. Its in-memory table is gone. Lot B keeps serving.", run: async function () { hlNode("lot_server_A"); await wait(800); killProcess("lot_server_A"); await wait(500); } },
      { text: "A request to Lot A now times out 3 times, 2 s each. A timeout only says how long we waited, not why.", run: async function () { hlEdge("client|lot_server_A"); await request("find", null, false, "A"); } },
      { text: "Restart lot server A. It replays its log (heartbeats, events and the OK replies it sent) and rebuilds its table.", run: async function () { hlNode("lot_server_A"); startProcess("lot_server_A"); await wait(1500); } },
      { text: "bob tries to reserve the same space. Expect DENIED reserved: alice's reservation survived the crash.", run: async function (c) { setDriver("bob"); selectSpace(c.sid); await request("reserve", c.sid); } },
    ],
  },
  {
    title: "Peer link cut: other lot's data goes stale",
    steps: [
      { text: "Cut the peer link between the two lot servers. Each keeps serving its own lot but stops hearing about the other.", run: async function () { hlEdge("lot_server_A|lot_server_B"); await wait(600); setLink("lot_server_A|lot_server_B", false); } },
      { text: "After 9 s (three missed summaries) the other lot's numbers are marked STALE. We show that to the client instead of hiding it.", run: async function () { hlLot("A"); await wait(10000, "waiting 10 s for three missed summaries"); } },
      { text: "A find on Lot A still works; the Lot B numbers in the reply are flagged STALE.", run: async function () { await request("find", null, false, "A"); await wait(1200); } },
      { text: "Restore the link. The next summary clears the flag. Losing the peer link degrades the service, it does not break it.", run: async function () { hlEdge("lot_server_A|lot_server_B"); setLink("lot_server_A|lot_server_B", true); await wait(3500); } },
    ],
  },
];

// ---- runner ----

function renderScenarios() {
  $("#scen").innerHTML = SCENARIOS.map(function (s, i) {
    return '<button data-i="' + i + '"><span class="n">' + (i + 1) + "</span>" + s.title + "</button>";
  }).join("");
  $("#scen").querySelectorAll("button").forEach(function (b) {
    b.onclick = function () { runScenario(+b.dataset.i); };
  });
  $("#btnStop").onclick = function () { stopRequested = true; };
  $("#try").querySelectorAll(".card").forEach(function (c) {
    c.onclick = function () {
      document.getElementById("milestone-1").scrollIntoView({ behavior: "smooth" });
      setTimeout(function () { runScenario(+c.dataset.scen - 1); }, 500);
    };
  });
}

async function runScenario(i) {
  if (scenarioRunning) { toast("a scenario is already running", "warn"); return; }
  const sc = SCENARIOS[i];
  scenarioRunning = true;
  stopRequested = false;
  const buttons = $("#scen").querySelectorAll("button");
  buttons.forEach(function (x, j) { x.classList.toggle("running", j === i); });

  const runner = $("#runner");
  runner.hidden = false;
  $("#runnerTitle").textContent = (i + 1) + ". " + sc.title;
  $("#steps").innerHTML = sc.steps.map(function (s) { return '<div class="step"><i></i><span>' + s.text + "</span></div>"; }).join("");
  const rows = $("#steps").querySelectorAll(".step");
  highlight($("#runner"));
  note("▶ Scenario " + (i + 1) + ": " + sc.title);

  const ctx = {};
  try {
    for (let k = 0; k < sc.steps.length; k++) {
      if (stopRequested) break;
      rows[k].classList.add("now");
      note((k + 1) + ". " + sc.steps[k].text);
      await sc.steps[k].run(ctx);
      rows[k].classList.remove("now");
      rows[k].classList.add("done");
    }
  } catch (e) {
    toast(e.message, "warn");
    note("stopped: " + e.message);
  } finally {
    scenarioRunning = false;
    buttons.forEach(function (x) { x.classList.remove("running"); });
    $("#runnerTitle").textContent = (stopRequested ? "stopped: " : "done: ") + sc.title;
    note(stopRequested ? "■ scenario stopped" : "■ end of scenario");
    $("#steps").querySelectorAll(".step.now").forEach(function (r) { r.classList.remove("now"); });
  }
}
