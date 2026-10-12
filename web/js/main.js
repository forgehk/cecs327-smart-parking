// Page setup: draw everything, wire up the controls, start the processes.

drawDiagram();
renderChips();
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

$("#btnDownload").onclick = downloadLogs;
$("#btnClear").onclick = clearLog;
$("#btnReset").onclick = resetDemo;

$("#logq").oninput = function (e) {
  logQuery = e.target.value.trim().toLowerCase();
  applyLogFilters();
};
$("#logwrap").onmouseenter = function () { logPaused = true; $("#logwrap").classList.add("paused"); };
$("#logwrap").onmouseleave = function () {
  logPaused = false;
  $("#logwrap").classList.remove("paused");
  if ($("#autoscroll").checked) $("#logwrap").scrollTop = $("#logwrap").scrollHeight;
};

// keyboard: 1-5 scenarios, R reset, / search, Esc clear selection
document.addEventListener("keydown", function (e) {
  const tag = (e.target.tagName || "").toLowerCase();
  if (tag === "input" || tag === "select" || tag === "textarea") {
    if (e.key === "Escape") e.target.blur();
    return;
  }
  if (e.key >= "1" && e.key <= "5") runScenario(+e.key - 1);
  else if (e.key === "r" || e.key === "R") resetDemo();
  else if (e.key === "/") { e.preventDefault(); $("#logq").focus(); }
  else if (e.key === "Escape") { selectedSpace = null; renderLot("A"); renderLot("B"); }
});

// active tab follows scroll position
const sections = ["overview", "milestone-1", "milestone-2"].map(function (id) { return document.getElementById(id); });
function updateTabs() {
  const y = window.scrollY + 120;
  let cur = sections[0];
  sections.forEach(function (s) { if (s.offsetTop <= y) cur = s; });
  $("#tabs").querySelectorAll("a").forEach(function (a) {
    a.classList.toggle("on", a.getAttribute("href") === "#" + cur.id);
  });
}
window.addEventListener("scroll", updateTabs, { passive: true });
updateTabs();

["lot_server_A", "lot_server_B", "sensor_node_A", "sensor_node_B", "client"].forEach(startProcess);
renderLot("A");
renderLot("B");
