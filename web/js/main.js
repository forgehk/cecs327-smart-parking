// Page setup: draw everything, wire up the controls, start the processes.

drawDiagram();
renderChips();
renderLinks();
renderScenarios();

$("#latency").oninput = function (e) {
  latency = +e.target.value;
  $("#latencyv").textContent = latency + " ms";
};

$("#drop").oninput = function (e) {
  dropRate = +e.target.value;
  $("#dropv").textContent = Math.round(dropRate * 100) + "%";
};

$("#omit").oninput = function (e) {
  $("#omitv").textContent = Math.round(e.target.value * 100) + "%";
  ["sensor_node_A", "sensor_node_B"].forEach(function (n) {
    if (workers[n]) workers[n].postMessage({ kind: "cmd", cmd: "dropRate", value: +e.target.value });
  });
};

$("#btnFindA").onclick = function () { request("find", null, false, "A"); };
$("#btnFindB").onclick = function () { request("find", null, false, "B"); };

$("#btnReserve").onclick = function () {
  const sid = selectedOrWarn();
  if (sid) request("reserve", sid);
};

$("#btnRelease").onclick = function () {
  const sid = selectedOrWarn();
  if (sid) request("release", sid);
};

$("#btnLost").onclick = function () {
  const sid = selectedOrWarn();
  if (sid) request("reserve", sid, true);
};

$("#btnCar").onclick = function () {
  const sid = selectedOrWarn();
  if (!sid) return;
  const w = workers["sensor_node_" + sid.split("-")[0]];
  if (!w) {
    showReply({ text: "that lot's sensor node is down, nothing can see the car", reply: null });
    return;
  }
  w.postMessage({ kind: "cmd", cmd: "toggle", space: sid });
};

$("#btnDownload").onclick = downloadLogs;
$("#btnClear").onclick = function () {
  $("#log").innerHTML = "";
  logRows = 0;
};

["lot_server_A", "lot_server_B", "sensor_node_A", "sensor_node_B", "client"].forEach(startProcess);
renderLot("A");
renderLot("B");
