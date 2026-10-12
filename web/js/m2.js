// Milestone 2: communication model and sequence diagrams (drawn into the #milestone-2 section)
(function () {
const C = { client: "#ca8a04", server: "#1d4ed8", sensor: "#059669", broker: "#7c3aed", registry: "#dc2626" };
const NS = "http://www.w3.org/2000/svg";
function el(tag, attrs, text) {
  const e = document.createElementNS(NS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (text !== undefined) e.textContent = text;
  return e;
}

// ---- sequence diagram renderer ----
// actors: [{id, label, sub, color}]
// steps:  {from,to,label,sub,dashed,fail} | {note, at, text, w, side} | {gap}
// the viewBox height is set to fit the steps.
function sequence(svgId, actors, steps, opts) {
  const svg = document.getElementById(svgId);
  const W = 1000, margin = 120;
  const n = actors.length;
  const gapX = (W - 2 * margin) / (n - 1);
  const x = {}; actors.forEach((a, i) => x[a.id] = margin + i * gapX);
  const top = 70; let y = top + 30;
  const rowH = (opts && opts.rowH) || 42;
  const body = el("g", {});

  for (const s of steps) {
    if (s.gap) { y += s.gap; continue; }
    if (s.note) {
      const g = el("g", { class: "note" });
      const w = s.w || 300, cx = x[s.at || s.note];
      const lines = s.text.split("\n");
      const nx = s.side === "left" ? cx - w - 12 : cx + 12;
      g.appendChild(el("rect", { x: nx, y: y - 4, width: w, height: 14 * lines.length + 10 }));
      lines.forEach((t, i) => g.appendChild(el("text", { x: nx + 8, y: y + 11 + i * 14 }, t)));
      body.appendChild(g);
      y += 14 * lines.length + 30;
      continue;
    }
    const x1 = x[s.from], x2 = x[s.to];
    const g = el("g", { class: "msg" + (s.dashed ? " dashed" : "") + (s.fail ? " fail" : "") });
    if (s.from === s.to) {
      const dir = (x1 > W / 2) ? -1 : 1;     // loop to the left for actors on the right
      const p = `M ${x1} ${y} h ${40 * dir} v 22 h ${-40 * dir}`;
      g.appendChild(el("path", { d: p, "marker-end": "url(#arrow)" }));
      g.appendChild(el("text", { x: x1 + 46 * dir, y: y + 14, style: "text-anchor:" + (dir > 0 ? "start" : "end") }, s.label));
      body.appendChild(g);
      y += rowH;
      continue;
    }
    g.appendChild(el("line", { x1, y1: y, x2: x2 + (x2 > x1 ? -4 : 4), y2: y, "marker-end": s.fail ? "url(#arrowfail)" : "url(#arrow)" }));
    const mx = (x1 + x2) / 2;
    g.appendChild(el("text", { x: mx, y: y - 6 }, s.label));
    if (s.sub) g.appendChild(el("text", { class: "sub", x: mx, y: y + 14 }, s.sub));
    body.appendChild(g);
    y += rowH;
  }
  const H = y + 10;
  svg.setAttribute("viewBox", "0 0 " + W + " " + H);

  const defs = el("defs", {});
  defs.innerHTML = '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#334155"/></marker>' +
    '<marker id="arrowfail" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#dc2626"/></marker>';
  svg.appendChild(defs);
  actors.forEach(a => svg.appendChild(el("line", { class: "lifeline", x1: x[a.id], y1: top, x2: x[a.id], y2: H - 10 })));
  actors.forEach(a => {
    const g = el("g", { class: "actor" });
    g.appendChild(el("rect", { x: x[a.id] - 85, y: 18, width: 170, height: 44, stroke: a.color }));
    g.appendChild(el("text", { x: x[a.id], y: 38, fill: a.color }, a.label));
    if (a.sub) g.appendChild(el("text", { x: x[a.id], y: 54, fill: "#5b6680", style: "font-size:10.5px;font-weight:400" }, a.sub));
    svg.appendChild(g);
  });
  svg.appendChild(body);
}

// ---- communication model ----
function model() {
  const svg = document.getElementById("model");
  const box = (x, y, w, h, color, title, lines) => {
    const g = el("g", { class: "actor" });
    g.appendChild(el("rect", { x, y, width: w, height: h, stroke: color }));
    g.appendChild(el("text", { x: x + w / 2, y: y + 20, fill: color }, title));
    lines.forEach((t, i) => g.appendChild(el("text", { x: x + w / 2, y: y + 36 + i * 13, fill: "#5b6680", style: "font-size:10.5px;font-weight:400" }, t)));
    svg.appendChild(g);
  };
  const label = (g, lx, ly, text) => {
    const w = text.length * 6.6 + 8;
    g.appendChild(el("rect", { x: lx - w / 2, y: ly - 11, width: w, height: 15, fill: "#fff", rx: 3 }));
    g.appendChild(el("text", { x: lx, y: ly, style: "font-size:10.5px" }, text));
  };
  const arrow = (x1, y1, x2, y2, text, color, both, dashed, lx, ly) => {
    const g = el("g", { class: "msg" + (dashed ? " dashed" : "") });
    const l = el("line", { x1, y1, x2, y2, stroke: color, "marker-end": "url(#arrow)" });
    if (both) l.setAttribute("marker-start", "url(#arrow)");
    g.appendChild(l);
    if (text) label(g, lx !== undefined ? lx : (x1 + x2) / 2, ly !== undefined ? ly : (y1 + y2) / 2 - 6, text);
    svg.appendChild(g);
  };
  const path = (d, text, color, dashed, lx, ly) => {
    const g = el("g", { class: "msg" + (dashed ? " dashed" : "") });
    g.appendChild(el("path", { d, stroke: color, "marker-end": "url(#arrow)" }));
    if (text) label(g, lx, ly, text);
    svg.appendChild(g);
  };
  const defs = el("defs", {});
  defs.innerHTML = '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#334155"/></marker>';
  svg.appendChild(defs);

  box(40, 40, 170, 64, C.sensor, "sensor node A", ["publisher", "lot/A/sensor"]);
  box(790, 40, 170, 64, C.sensor, "sensor node B", ["publisher", "lot/B/sensor"]);
  box(400, 40, 200, 64, C.broker, "broker  :5000", ["topic -> subscribers", "PUBLISH -> EVENT"]);
  box(40, 220, 190, 80, C.server, "lot server A  :5001", ["service: find/reserve/release", "subscriber: lot/A/sensor", "publisher: lot/A/status"]);
  box(770, 220, 190, 80, C.server, "lot server B  :5002", ["service: find/reserve/release", "subscriber: lot/B/sensor", "publisher: lot/B/status"]);
  box(400, 340, 200, 60, C.registry, "registry  :5010", ["group lot_servers", "JOIN / LEAVE / MEMBERS"]);
  box(400, 450, 200, 60, C.client, "client (driver app)", ["request-reply to a lot server", "watch: subscriber lot/<lot>/status"]);

  // sensors -> broker
  arrow(210, 72, 398, 72, "PUBLISH (pub-sub)", C.sensor);
  arrow(790, 72, 602, 72, "PUBLISH (pub-sub)", C.sensor);
  // broker -> lot servers (EVENT) and lot servers -> broker (PUBLISH status)
  arrow(470, 104, 200, 218, "EVENT lot/A/sensor", C.broker, false, false, 370, 150);
  arrow(530, 104, 800, 218, "EVENT lot/B/sensor", C.broker, false, false, 630, 150);
  arrow(90, 220, 410, 106, "PUBLISH lot/A/status", C.server, false, true, 190, 200);
  arrow(910, 220, 590, 106, "PUBLISH lot/B/status", C.server, false, true, 810, 200);
  // group
  arrow(230, 262, 770, 262, "GROUP_MSG summary (new connection per member)", "#334155", true, false, 500, 254);
  arrow(230, 290, 398, 352, "JOIN / MEMBERS", C.registry, false, true, 300, 338);
  arrow(770, 290, 602, 352, "JOIN / MEMBERS", C.registry, false, true, 700, 338);
  // client
  arrow(410, 452, 160, 302, "REQUEST / REPLY", C.client, true, false, 230, 400);
  arrow(590, 452, 840, 302, "REQUEST / REPLY", C.client, true, false, 770, 400);
  path("M 600 490 H 690 V 90 H 602", "SUBSCRIBE lot/<lot>/status (watch)", C.client, true, 690, 430);
  svg.appendChild(el("text", { x: 500, y: 532, "text-anchor": "middle", style: "font-size:11px", fill: "#5b6680" },
    "all links: TCP, 4-byte length + JSON envelope · solid = always on · dashed = opened on demand"));
}

model();

sequence("rr",
  [{ id: "c", label: "client_alice", color: C.client, sub: "client.py reserve A-03" },
   { id: "s", label: "lot_server_A :5001", color: C.server, sub: "Service, thread pool" }],
  [
    { from: "c", to: "s", label: "TCP connect", sub: "timeout 2 s" },
    { from: "c", to: "s", label: "REQUEST msg_id=9040cd5e {op: reserve, args: {...}}", sub: "args = {client: alice, space: A-03}" },
    { note: "s", text: "op_reserve: space free -> reserved_by=alice, lease 30 s\nreply cached under msg_id 9040cd5e", w: 330, side: "left" },
    { from: "s", to: "c", label: "REPLY reply_to=9040cd5e {status: OK, result: {...}}", sub: "result = {granted: true, reason: reserved, lease: 30}" },
    { from: "s", to: "c", label: "close", dashed: true },
    { gap: 10 },
    { note: "c", text: "Lost-reply case (--simulate-lost-reply):", w: 260 },
    { from: "c", to: "s", label: "REQUEST msg_id=d25f5c0c  {op: reserve, ...}" },
    { from: "s", to: "c", label: "REPLY reply_to=d25f5c0c", sub: "lost on the wire", fail: true },
    { from: "c", to: "c", label: "timeout 2 s -> reconnect, same msg_id" },
    { from: "c", to: "s", label: "REQUEST msg_id=d25f5c0c  (retry)" },
    { note: "s", text: "msg_id already in reply cache -> not executed again", w: 300, side: "left" },
    { from: "s", to: "c", label: "REPLY reply_to=d25f5c0c {status: OK, ..., duplicate: true}" },
    { gap: 4 },
    { note: "c", text: "Errors: unknown op / bad args -> REPLY {status: ERROR, error}\nno reply after 3 tries -> ServiceUnavailable, logged", w: 330 },
  ], { rowH: 40 });

sequence("ps",
  [{ id: "p", label: "sensor_node_A", color: C.sensor, sub: "publisher" },
   { id: "b", label: "broker :5000", color: C.broker, sub: "topics -> subscribers" },
   { id: "s1", label: "lot_server_A", color: C.server, sub: "subscriber (long-lived)" },
   { id: "s2", label: "client_watcher", color: C.client, sub: "subscriber (long-lived)" }],
  [
    { from: "s1", to: "b", label: "SUBSCRIBE {topic: lot/A/sensor}" },
    { from: "b", to: "s1", label: "ACK" },
    { from: "s2", to: "b", label: "SUBSCRIBE {topic: lot/A/sensor}", sub: "a second subscriber on the same topic" },
    { from: "b", to: "s2", label: "ACK" },
    { gap: 6 },
    { from: "p", to: "b", label: "PUBLISH {topic: lot/A/sensor, data: {...}}", sub: "data = {type: event, space: A-11, state: occupied}" },
    { note: "b", text: "lookup topic -> 2 subscribers\none EVENT per subscriber, one log line per delivery", w: 320 },
    { from: "b", to: "s1", label: "EVENT {topic, data, publisher: sensor_node_A}" },
    { from: "b", to: "s2", label: "EVENT {topic, data, publisher: sensor_node_A}" },
    { from: "b", to: "p", label: "ACK {delivered: 2}" },
    { gap: 6 },
    { from: "s2", to: "s2", label: "crashes (no UNSUBSCRIBE)" },
    { from: "p", to: "b", label: "PUBLISH {topic: lot/A/sensor, ...}" },
    { from: "b", to: "s1", label: "EVENT" },
    { from: "b", to: "s2", label: "EVENT", sub: "send fails", fail: true },
    { note: "b", text: "log: subscriber unreachable, removed from lot/A/sensor", w: 330 },
    { from: "b", to: "p", label: "ACK {delivered: 1}" },
  ], { rowH: 36 });

sequence("grp",
  [{ id: "a", label: "lot_server_A", color: C.server, sub: "sender" },
   { id: "r", label: "registry :5010", color: C.registry, sub: "group -> members" },
   { id: "b", label: "lot_server_B", color: C.server, sub: "member" },
   { id: "c", label: "lot_server_C", color: C.server, sub: "member (will crash)" }],
  [
    { from: "a", to: "r", label: "JOIN {group: lot_servers, address: 127.0.0.1:5001}" },
    { from: "r", to: "a", label: "ACK {members: [...]}" },
    { from: "b", to: "r", label: "JOIN {address: 127.0.0.1:5002}" },
    { from: "r", to: "b", label: "ACK" },
    { from: "c", to: "r", label: "JOIN {address: 127.0.0.1:5003}" },
    { from: "r", to: "c", label: "ACK" },
    { gap: 6 },
    { note: "a", text: "every 3 s: group_send(summary)", w: 220 },
    { from: "a", to: "r", label: "MEMBERS {group: lot_servers}" },
    { from: "r", to: "a", label: "ACK {members: [5001, 5002, 5003]}" },
    { from: "a", to: "b", label: "GROUP_MSG {group, data: {kind: summary, lot: A, ...}}", sub: "new connection, one log line" },
    { from: "c", to: "c", label: "crashed" },
    { from: "a", to: "c", label: "GROUP_MSG {...}", sub: "connection refused", fail: true },
    { note: "a", text: "log: member 127.0.0.1:5003 unreachable, skipped\nsend still completed for B. C stays in the registry\n(it never sent LEAVE) until it is removed", w: 340 },
  ], { rowH: 36 });

// ---- PNG download ----
document.querySelectorAll("button[data-dl]").forEach(b => b.onclick = () => {
  const svg = document.getElementById(b.dataset.dl);
  const clone = svg.cloneNode(true);
  const style = document.createElement("style");
  style.textContent = Array.from(document.styleSheets[0].cssRules).map(r => r.cssText.replace(/#milestone-2 /g, "")).join("\n");
  clone.insertBefore(style, clone.firstChild);
  clone.setAttribute("xmlns", NS);
  const vb = svg.viewBox.baseVal;
  const data = new XMLSerializer().serializeToString(clone);
  const img = new Image();
  img.onload = () => {
    const canvas = document.createElement("canvas");
    canvas.width = vb.width * 2; canvas.height = vb.height * 2;
    const ctx = canvas.getContext("2d");
    ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
    const a = document.createElement("a");
    a.download = "m2_" + b.dataset.dl + ".png";
    a.href = canvas.toDataURL("image/png");
    a.click();
  };
  img.src = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(data);
});
})();
