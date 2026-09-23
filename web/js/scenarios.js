// Scripted demo scenarios. Each one drives the system through the same
// controls a person would use and writes a short explanation as it goes.

function sleep(ms) {
  return new Promise(function (r) { setTimeout(r, ms); });
}

function narrate(text) {
  const n = $("#narr");
  const d = document.createElement("div");
  d.textContent = text;
  n.appendChild(d);
  n.scrollTop = n.scrollHeight;
  logRow({ cls: "console demo", from: "demo", to: "", type: "narrator", detail: text });
}

function firstFree(lot) {
  const s = lotState[lot];
  if (!s) return null;
  const sp = s.spaces.find(function (x) { return x.state === "free"; });
  return sp ? sp.id : null;
}

function setDriver(name) {
  $("#who").value = name;
}

async function scenarioRequestResponse() {
  narrate("1. Client asks Lot A for free spaces.");
  await request("find", null, false, "A");
  await sleep(800);

  const sid = firstFree("A");
  if (!sid) { narrate("No free space in Lot A right now, try again."); return; }

  setDriver("alice");
  narrate("2. alice reserves " + sid + ", expect OK with a 30 s lease.");
  await request("reserve", sid);
  await sleep(600);

  setDriver("bob");
  narrate("3. bob tries the same space, expect DENIED reserved. The lot server is the only writer for its spaces, so it just handles the two requests one after the other.");
  await request("reserve", sid);
  await sleep(600);

  narrate("4. bob tries to release it: DENIED, it's not his.");
  await request("release", sid);
  await sleep(600);

  setDriver("alice");
  narrate("5. alice releases " + sid + ": OK. The tile goes blue and back.");
  await request("release", sid);
}

async function scenarioLostReply() {
  const sid = firstFree("A");
  if (!sid) { narrate("No free space in Lot A right now."); return; }
  setDriver("carol");
  narrate("carol reserves " + sid + ", but the client pretends the reply was lost. After the 2 s timeout it retries with the same req_id.");
  narrate("Watch the log: the server says DUPLICATE and re-sends its cached reply. No second reservation.");
  await request("reserve", sid, true);
  await sleep(500);
  narrate("Done: exactly one reservation, held by carol.");
}

async function scenarioSensorCrash() {
  narrate("Kill sensor node A. The server just hears nothing; it can't tell a dead sensor from a cut link.");
  killProcess("sensor_node_A");
  narrate("Rule: no report within Δ = 6 s means the space becomes unknown. Silence never turns into free. Waiting 7 s...");
  await sleep(7000);
  narrate("A find on Lot A now returns zero free spaces, but the total still shows 20 so you can see how much visibility was lost. Lot B is not affected.");
  await request("find", null, false, "A");
  await sleep(1500);
  narrate("Restart the sensor. Its first heartbeat carries a full snapshot so every space leaves unknown at once.");
  startProcess("sensor_node_A");
}

async function scenarioServerCrash() {
  const sid = firstFree("A");
  if (!sid) { narrate("No free space in Lot A right now."); return; }
  setDriver("alice");
  narrate("alice reserves " + sid + ".");
  await request("reserve", sid);
  await sleep(700);

  narrate("Kill lot server A: its in-memory table is gone. Lot B keeps serving.");
  killProcess("lot_server_A");
  narrate("A request to Lot A now times out 3 times (2 s each). A timeout only tells us how long we waited, not why.");
  await request("find", null, false, "A");

  narrate("Restart lot server A. It replays its log (heartbeats, events and the OK replies it sent) and rebuilds the table.");
  startProcess("lot_server_A");
  await sleep(1500);

  setDriver("bob");
  narrate("bob tries to reserve " + sid + ": expect DENIED reserved, alice's reservation survived the crash.");
  await request("reserve", sid);
}

async function scenarioPeerLink() {
  narrate("Cut the peer link between the two lot servers. Each keeps serving its own lot but stops hearing about the other.");
  const cb = $('#links input[data-l="lot_server_A|lot_server_B"]');
  cb.checked = false;
  cb.dispatchEvent(new Event("change"));
  narrate("After 9 s (three missed summaries) the other lot's numbers are marked STALE. We show that to the client instead of hiding it. Waiting 10 s...");
  await sleep(10000);
  await request("find", null, false, "A");
  await sleep(1500);
  narrate("Restore the link. The next summary clears the STALE flag. Losing the peer link degrades the service, it doesn't break it.");
  cb.checked = true;
  cb.dispatchEvent(new Event("change"));
}

const SCENARIOS = [
  ["Request / response, one driver per space", scenarioRequestResponse],
  ["Lost reply, idempotent retry", scenarioLostReply],
  ["Sensor node crash: spaces become unknown", scenarioSensorCrash],
  ["Lot server crash and recovery from log", scenarioServerCrash],
  ["Peer link cut: other lot's data goes stale", scenarioPeerLink],
];

let scenarioRunning = false;

function renderScenarios() {
  $("#scen").innerHTML = SCENARIOS.map(function (s, i) {
    return '<button data-i="' + i + '"><span class="n">' + (i + 1) + "</span>" + s[0] + "</button>";
  }).join("");

  $("#scen").querySelectorAll("button").forEach(function (b) {
    b.onclick = async function () {
      if (scenarioRunning) return;
      scenarioRunning = true;
      const buttons = $("#scen").querySelectorAll("button");
      buttons.forEach(function (x) { x.disabled = true; });
      $("#narr").innerHTML = "";
      const i = +b.dataset.i;
      narrate("▶ Scenario " + (i + 1) + ": " + SCENARIOS[i][0]);
      try {
        await SCENARIOS[i][1]();
      } finally {
        scenarioRunning = false;
        buttons.forEach(function (x) { x.disabled = false; });
        narrate("■ end of scenario");
      }
    };
  });
}
