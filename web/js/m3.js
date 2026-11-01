// Milestone 3: overlay simulator. Same rules as prototype/p2p/overlay.py, drawn in SVG.
// Starts from the mesh run_demo.sh builds (A-B, A-C, A-D, B-C, C-D); E is not running yet.
(function () {
  const NS = "http://www.w3.org/2000/svg";
  const IDS = ["A", "B", "C", "D", "E"];
  const POS = { A: [340, 78], B: [585, 190], C: [500, 378], D: [180, 378], E: [95, 190] };
  const FREE = { A: 14, B: 12, C: 11, D: 15, E: 15 };
  const INITIAL = [["A", "B"], ["A", "C"], ["A", "D"], ["B", "C"], ["C", "D"]];
  const MAXN = 2;          // links a node opens itself; incoming links are always accepted
  const R = 30;            // node radius
  const HOP = 720;         // ms per hop at normal speed

  const svg = document.getElementById("overlay");
  if (!svg) return;

  let S = {};              // id -> {status: up | dead | left | off, nbrs: Set}
  let known = new Set();   // lots that have ever been up (their summaries live on as replicas)
  let marks = {};          // id -> Set of css classes for the current operation
  let freshEdges = new Set();
  let treeEdges = new Set();
  let speed = 1, TTL = 3, PF = 0.7;
  let busy = false, t0 = performance.now(), lastOp = "none yet";

  // ------------------------------------------------------------------ helpers
  function el(tag, attrs, text) {
    const e = document.createElementNS(NS, tag);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    if (text !== undefined) e.textContent = text;
    return e;
  }
  const up = (id) => S[id] && S[id].status === "up";
  const ekey = (a, b) => [a, b].sort().join("");
  const pause = (ms) => new Promise((r) => setTimeout(r, ms / speed));
  function shuffle(a) {
    a = a.slice();
    for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; }
    return a;
  }
  function link(a, b) { S[a].nbrs.add(b); S[b].nbrs.add(a); freshEdges.add(ekey(a, b)); }
  function mark(id, cls) { (marks[id] = marks[id] || new Set()).add(cls); render(); }
  function clearMarks() {
    marks = {}; freshEdges = new Set(); treeEdges = new Set();
    svg.querySelectorAll(".oarc").forEach((p) => p.remove());
    render();
  }

  // ------------------------------------------------------------------ log
  const logEl = document.getElementById("m3log");
  function log(who, text, cls) {
    const t = ((performance.now() - t0) / 1000).toFixed(1).padStart(5, " ") + "s";
    const d = document.createElement("div");
    d.innerHTML = '<span class="t">' + t + '</span><span class="who">' + who + "</span>" +
      '<span class="' + (cls || "") + '">' + text + "</span>";
    logEl.appendChild(d);
    logEl.scrollTop = logEl.scrollHeight;
  }

  // ------------------------------------------------------------------ drawing
  const gEdges = el("g", {}), gArcs = el("g", {}), gNodes = el("g", {}), gDots = el("g", {});
  svg.append(gEdges, gArcs, gNodes, gDots);

  function edges() {
    const out = new Map();
    // drawn from the live side: a link exists while a running lot still lists the other end
    IDS.filter(up).forEach((u) => {
      S[u].nbrs.forEach((v) => {
        if (S[v].status === "off" || S[v].status === "left") return;
        out.set(ekey(u, v), [u, v].sort());
      });
    });
    return [...out.values()];
  }

  function render() {
    gEdges.innerHTML = "";
    edges().forEach(([a, b]) => {
      const k = ekey(a, b);
      let cls = "oedge";
      if (!up(a) || !up(b)) cls += " half";
      else if (treeEdges.has(k)) cls += " tree";
      else if (freshEdges.has(k)) cls += " fresh";
      gEdges.appendChild(el("line", { class: cls, x1: POS[a][0], y1: POS[a][1], x2: POS[b][0], y2: POS[b][1] }));
    });
    gNodes.innerHTML = "";
    IDS.forEach((id) => {
      const n = S[id], [x, y] = POS[id];
      const cls = ["onode"];
      if (n.status === "dead") cls.push("dead");
      if (n.status === "off" || n.status === "left") cls.push("off");
      (marks[id] || new Set()).forEach((c) => cls.push(c));
      const g = el("g", { class: cls.join(" "), "data-id": id });
      g.appendChild(el("circle", { class: "ring", cx: x, cy: y, r: 34 }));
      g.appendChild(el("circle", { class: "body", cx: x, cy: y, r: R }));
      g.appendChild(el("text", { class: "name", x: x, y: y }, n.status === "dead" ? id + "✕" : id));
      const status = { up: FREE[id] + " free", dead: "crashed", left: "left", off: "not running" }[n.status];
      g.appendChild(el("text", { class: "sub", x: x, y: y + R + 17 }, status));
      if (n.status === "up") {
        g.appendChild(el("circle", { class: "badge", cx: x + 24, cy: y - 24, r: 11 }));
        g.appendChild(el("text", { class: "deg", x: x + 24, y: y - 23.5 }, String(n.nbrs.size)));
      }
      const title = el("title", {}, n.status === "up" ? "Lot " + id + ": click to crash" : "Lot " + id + ": click to start it through a random peer");
      g.appendChild(title);
      g.addEventListener("click", () => onNodeClick(id));
      gNodes.appendChild(g);
    });
    updateMetrics();
    updateButtons();
  }

  function pulse(id) {
    const ring = gNodes.querySelector('g[data-id="' + id + '"] .ring');
    if (ring) { ring.classList.remove("pulse"); void ring.getBoundingClientRect(); ring.classList.add("pulse"); }
  }

  function flash(id) {
    const [x, y] = POS[id];
    const c = el("circle", { class: "oflash", cx: x, cy: y, r: R + 6 });
    gDots.appendChild(c);
    setTimeout(() => c.remove(), 420 / speed);
  }

  function ends(from, to) {
    const [x1, y1] = POS[from], [x2, y2] = POS[to];
    const d = Math.hypot(x2 - x1, y2 - y1) || 1, ux = (x2 - x1) / d, uy = (y2 - y1) / d;
    return [[x1 + ux * (R + 4), y1 + uy * (R + 4)], [x2 - ux * (R + 4), y2 - uy * (R + 4)]];
  }

  // one message along an overlay link; resolves true if the receiver was up
  function fly(from, to, kind, label) {
    return new Promise((res) => {
      const ok = up(to);
      const [a, b] = ends(from, to);
      const dot = el("circle", { class: "odot " + kind, cx: a[0], cy: a[1], r: 6 });
      gDots.appendChild(dot);
      const txt = label ? el("text", { class: "olabel" }, label) : null;
      if (txt) gDots.appendChild(txt);
      const dur = HOP / speed, frac = ok ? 1 : 0.6, start = performance.now();
      function step(now) {
        const k = Math.min(1, (now - start) / dur);
        const e = (k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2) * frac;
        const x = a[0] + (b[0] - a[0]) * e, y = a[1] + (b[1] - a[1]) * e;
        dot.setAttribute("cx", x); dot.setAttribute("cy", y);
        if (txt) { txt.setAttribute("x", x); txt.setAttribute("y", y - 11); }
        if (k < 1) return requestAnimationFrame(step);
        if (!ok) {
          dot.setAttribute("class", "odot bad");
          setTimeout(() => { dot.remove(); if (txt) txt.remove(); }, 260 / speed);
        } else { dot.remove(); if (txt) txt.remove(); }
        res(ok);
      }
      requestAnimationFrame(step);
    });
  }

  // a direct TCP message that does not follow overlay links (P2P_FOUND to the asker)
  function arc(from, to, kind) {
    return new Promise((res) => {
      const [a, b] = ends(from, to);
      const mx = (a[0] + b[0]) / 2, my = (a[1] + b[1]) / 2;
      const cx = 340, cy = 240;                          // bend away from the center of the drawing
      const ox = mx - cx, oy = my - cy, ol = Math.hypot(ox, oy) || 1;
      const qx = mx + (ox / ol) * 70, qy = my + (oy / ol) * 70;
      const path = el("path", { class: "oarc", d: "M" + a[0] + " " + a[1] + " Q" + qx + " " + qy + " " + b[0] + " " + b[1] });
      gArcs.appendChild(path);
      const len = path.getTotalLength();
      const dot = el("circle", { class: "odot " + kind, r: 6 });
      gDots.appendChild(dot);
      const dur = (HOP * 1.3) / speed, start = performance.now();
      function step(now) {
        const k = Math.min(1, (now - start) / dur);
        const p = path.getPointAtLength(len * k);
        dot.setAttribute("cx", p.x); dot.setAttribute("cy", p.y);
        if (k < 1) return requestAnimationFrame(step);
        dot.remove();
        res(true);
      }
      requestAnimationFrame(step);
    });
  }

  // ------------------------------------------------------------------ metrics
  function graph() {
    const adj = {};
    IDS.filter(up).forEach((u) => { adj[u] = new Set(); });
    edges().forEach(([a, b]) => { if (up(a) && up(b)) { adj[a].add(b); adj[b].add(a); } });
    return adj;
  }
  function bfs(adj, s) {
    const dist = { [s]: 0 }, q = [s];
    while (q.length) { const u = q.shift(); adj[u].forEach((v) => { if (!(v in dist)) { dist[v] = dist[u] + 1; q.push(v); } }); }
    return dist;
  }
  function stats() {
    const adj = graph(), ids = Object.keys(adj);
    let m = 0; ids.forEach((u) => (m += adj[u].size)); m /= 2;
    let diam = 0, sum = 0, pairs = 0, connected = true;
    ids.forEach((u) => {
      const d = bfs(adj, u);
      if (Object.keys(d).length !== ids.length) connected = false;
      ids.forEach((v) => { if (u < v && v in d) { diam = Math.max(diam, d[v]); sum += d[v]; pairs++; } });
    });
    return { adj, ids, m, diam, avg: pairs ? sum / pairs : 0, connected };
  }

  const metricsEl = document.getElementById("m3metrics");
  function updateMetrics() {
    const st = stats();
    const deg = st.ids.map((u) => u + " " + S[u].nbrs.size).join(", ") || "none";
    const miss = st.ids.map((u) => u + " " + Math.pow(1 - PF, S[u].nbrs.size).toFixed(3).replace(/^0/, "")).join(", ") || "none";
    const rows = [
      ["lots up", st.ids.length + " of 5"],
      ["links", st.m + (st.connected ? ", connected" : ", NOT connected")],
      ["degree", deg],
      ["diameter", st.diam + " hop" + (st.diam === 1 ? "" : "s") + ", avg " + st.avg.toFixed(2)],
      ["P(no copy)", miss],
      ["pings per round", String(2 * st.m)],
      ["last operation", lastOp],
    ];
    metricsEl.innerHTML = rows.map((r) => "<tr><td>" + r[0] + "</td><td>" + r[1] + "</td></tr>").join("");
    document.getElementById("m3state").textContent =
      st.ids.length + " lots, " + st.m + " links, diameter " + st.diam + (st.connected ? "" : ", split");
  }

  // ------------------------------------------------------------------ operations
  async function doJoin(node, boot) {
    S[node].status = "up";
    S[node].nbrs = new Set();
    known.add(node);
    clearMarks();
    pulse(node);
    if (!boot) { log(node, "first peer in the overlay, waiting for others"); lastOp = node + " started alone"; return; }
    log(node, "P2P_JOIN to bootstrap " + boot);
    mark(node, "asker");
    if (!(await fly(node, boot, "ctl", "JOIN"))) { log(node, "bootstrap " + boot + " did not answer", "bad"); return; }
    const sample = shuffle([...S[boot].nbrs].filter((x) => up(x) && x !== node)).slice(0, MAXN);
    link(node, boot);
    render();
    const table = [...known].filter((x) => x !== node).length;
    log(boot, "JOIN from " + node + ": gave " + sample.length + " neighbor pointer(s) and " + table + " table entries");
    await fly(boot, node, "ctl", "ACK");
    log(node, "took " + table + " replica(s) from " + boot);
    let msgs = 2;
    for (const c of sample) {
      if (S[node].nbrs.size >= MAXN) break;
      await fly(node, c, "ctl", "NEIGHBOR");
      link(node, c);
      render();
      await fly(c, node, "ctl", "ACK");
      msgs += 2;
    }
    log(node, "joined: neighbors " + [...S[node].nbrs].sort().join(", ") + " (" + msgs + " messages)", "ok");
    lastOp = node + " joined, " + msgs + " messages";
  }

  async function doLeave(node) {
    clearMarks();
    const nb = [...S[node].nbrs];
    S[node].status = "left";
    render();
    log(node, "P2P_LEAVE to " + nb.length + " neighbor(s), then exit");
    await Promise.all(nb.map((n) => fly(node, n, "ctl", "LEAVE").then((ok) => {
      if (ok) { S[n].nbrs.delete(node); log(n, "neighbor " + node + " left gracefully, degree now " + S[n].nbrs.size); }
      render();
    })));
    S[node].nbrs.clear();
    render();
    lastOp = node + " left, " + nb.length + " messages";
    await pause(600);
    await doPing(true);
  }

  async function doCrash(node) {
    clearMarks();
    S[node].status = "dead";
    render();
    log(node, "crashed (kill -9), nothing is sent", "bad");
    lastOp = node + " crashed, 0 messages";
    await pause(900);
    await doPing(true);
  }

  async function doPing(auto) {
    if (!auto) clearMarks();
    const pings = [];
    IDS.filter(up).forEach((u) => S[u].nbrs.forEach((v) => pings.push({ u, v })));
    log("all", "ping round: " + pings.length + " P2P_PING (one per neighbor per lot)");
    const res = await Promise.all(pings.map((p) => fly(p.u, p.v, "ctl")));
    const samples = {};
    let removed = 0;
    pings.forEach((p, i) => {
      if (!res[i]) {
        if (S[p.u].nbrs.delete(p.v)) {
          removed++;
          log(p.u, "no answer from " + p.v + ", dead pointer removed, degree now " + S[p.u].nbrs.size, "bad");
        }
      } else {
        (samples[p.u] = samples[p.u] || []).push(...[...S[p.v].nbrs].filter((x) => x !== p.u && up(x)));
      }
    });
    render();
    await Promise.all(pings.filter((p, i) => res[i]).map((p) => fly(p.v, p.u, "ctl")));
    let added = 0;
    for (const u of shuffle(IDS.filter(up))) {
      if (S[u].nbrs.size >= MAXN) continue;
      const cands = shuffle((samples[u] || []).filter((x) => x !== u && !S[u].nbrs.has(x) && up(x)));
      if (!cands.length) {
        if (S[u].nbrs.size === 0) log(u, "no neighbors left and no sample to try: isolated until it rejoins", "bad");
        continue;
      }
      const c = cands[0];
      await fly(u, c, "ctl", "NEIGHBOR");
      link(u, c);
      render();
      await fly(c, u, "ctl", "ACK");
      added++;
      log(u, "random sampling: linked to " + c + " (from a ping answer) to keep the mesh connected", "ok");
    }
    if (!removed && !added) log("all", "every neighbor answered, nothing to repair", "ok");
    const st = stats();
    lastOp = "ping round: " + pings.length + " pings, " + removed + " removed, " + added + " new links";
    if (!st.connected) log("all", "overlay is split into islands", "bad");
  }

  async function doLookup(src, tgt) {
    clearMarks();
    mark(src, "asker");
    mark(tgt, "target");
    const nb = S[src].nbrs.size;
    log(src, "P2P_LOOKUP lot/" + tgt + "/summary (owner only): flooding " + nb + " neighbor(s), TTL " + TTL);
    const seen = new Set([src]);
    let msgs = 0, dups = 0, expired = 0, failed = 0, found = null, foundP = null;
    let frontier = [{ n: src, ttl: TTL, path: [src] }];
    while (frontier.length) {
      const sends = [];
      frontier.forEach((f) => S[f.n].nbrs.forEach((n) => { if (!f.path.includes(n)) sends.push({ from: f.n, to: n, ttl: f.ttl, path: f.path }); }));
      if (!sends.length) break;
      msgs += sends.length;
      const res = await Promise.all(sends.map((s) => fly(s.from, s.to, "lookup", "TTL " + s.ttl)));
      const next = [];
      sends.forEach((s, i) => {
        if (!res[i]) { failed++; return; }
        if (seen.has(s.to)) { dups++; flash(s.to); return; }
        seen.add(s.to);
        const path = s.path.concat(s.to);
        if (s.to === tgt) {
          if (!found) { found = path; mark(tgt, "found"); foundP = arc(tgt, src, "found"); }
          return;
        }
        if (s.ttl - 1 <= 0) { expired++; mark(s.to, "ttl0"); return; }
        next.push({ n: s.to, ttl: s.ttl - 1, path });
      });
      frontier = next;
    }
    if (foundP) await foundP;
    const extra = dups + " duplicate(s) dropped" + (expired ? ", " + expired + " expired at TTL 0" : "") + (failed ? ", " + failed + " to a dead peer" : "");
    if (found) {
      log(src, "P2P_FOUND from " + tgt + " after " + (found.length - 1) + " hop(s) via " + found.join("->") + ". " + msgs + " lookup messages, " + extra, "ok");
      lastOp = "lookup " + src + " for " + tgt + ": " + (found.length - 1) + " hops, " + msgs + " msgs";
    } else {
      log(src, "not found within TTL " + TTL + ". " + msgs + " lookup messages, " + extra, "bad");
      lastOp = "lookup " + src + " for " + tgt + ": not found, " + msgs + " msgs";
    }
  }

  async function doGossip(origin) {
    clearMarks();
    const informed = new Set([origin]);
    mark(origin, "informed");
    log(origin, "put lot/" + origin + "/summary with a new version: P2P_GOSSIP to " + S[origin].nbrs.size + " neighbor(s)");
    let msgs = 0, dups = 0, skipped = 0, failed = 0;
    let frontier = [{ n: origin, path: [origin], first: true }];
    while (frontier.length) {
      const sends = [];
      frontier.forEach((f) => S[f.n].nbrs.forEach((n) => {
        if (f.path.includes(n)) return;
        if (!f.first && Math.random() >= PF) { skipped++; return; }
        sends.push({ from: f.n, to: n, path: f.path });
      }));
      if (!sends.length) break;
      msgs += sends.length;
      const res = await Promise.all(sends.map((s) => fly(s.from, s.to, "gossip")));
      const next = [];
      sends.forEach((s, i) => {
        if (!res[i]) { failed++; return; }
        if (informed.has(s.to)) { dups++; flash(s.to); return; }
        informed.add(s.to);
        mark(s.to, "informed");
        next.push({ n: s.to, path: s.path.concat(s.to) });
      });
      frontier = next;
    }
    const live = IDS.filter(up), missed = live.filter((x) => !informed.has(x));
    log(origin, "rumor reached " + informed.size + " of " + live.length + " live lots. " + msgs + " messages, " + dups +
      " duplicate(s), " + skipped + " forward(s) skipped by p_f" +
      (missed.length ? ". Missed: " + missed.join(", ") + ", picked up by the next round or the next ping" : ""), missed.length ? "note" : "ok");
    lastOp = "gossip from " + origin + ": " + informed.size + "/" + live.length + " lots, " + msgs + " msgs";
  }

  async function doTotal(root) {
    clearMarks();
    mark(root, "asker");
    log(root, "convergecast: P2P_AGG flooded with TTL " + TTL + ", first sender becomes the parent");
    const parent = { [root]: null }, depth = { [root]: 0 };
    let frontier = [{ n: root, ttl: TTL }], dups = 0, down = 0;
    while (frontier.length) {
      const sends = [];
      frontier.forEach((f) => { if (f.ttl > 0) S[f.n].nbrs.forEach((n) => { if (n !== parent[f.n]) sends.push({ from: f.n, to: n, ttl: f.ttl }); }); });
      if (!sends.length) break;
      down += sends.length;
      const res = await Promise.all(sends.map((s) => fly(s.from, s.to, "agg")));
      const next = [], dupBacks = [];
      sends.forEach((s, i) => {
        if (!res[i]) return;
        if (s.to in parent) { dups++; dupBacks.push(fly(s.to, s.from, "bad", "dup")); return; }
        parent[s.to] = s.from;
        depth[s.to] = depth[s.from] + 1;
        treeEdges.add(ekey(s.from, s.to));
        next.push({ n: s.to, ttl: s.ttl - 1 });
      });
      render();
      await Promise.all(dupBacks);
      frontier = next;
    }
    const part = {};
    Object.keys(parent).forEach((n) => { part[n] = { free: FREE[n], total: 20, lots: [n] }; });
    const maxd = Math.max(...Object.values(depth));
    for (let d = maxd; d >= 1; d--) {
      const level = Object.keys(depth).filter((n) => depth[n] === d);
      await Promise.all(level.map((n) => fly(n, parent[n], "found", part[n].free + "/" + part[n].total)));
      level.forEach((n) => {
        const p = part[parent[n]];
        p.free += part[n].free; p.total += part[n].total; p.lots = p.lots.concat(part[n].lots);
        mark(n, "found");
      });
    }
    const r = part[root], replies = Object.keys(parent).length - 1;
    mark(root, "found");
    log(root, "TOTAL " + r.free + " free of " + r.total + " across lots " + r.lots.sort().join(", ") + ". " +
      replies + " replies up the tree (N-1), " + dups + " duplicate AGG answered at once", "ok");
    lastOp = "total from " + root + ": " + r.free + "/" + r.total + ", " + replies + " replies";
  }

  // ------------------------------------------------------------------ controls
  const fromSel = document.getElementById("m3from"), forSel = document.getElementById("m3for");
  IDS.forEach((id) => { fromSel.add(new Option("lot " + id, id)); forSel.add(new Option("lot " + id, id)); });
  fromSel.value = "D"; forSel.value = "B";

  const btn = (name) => document.querySelector('#milestone-3 [data-m3="' + name + '"]');
  function joinBoot() { return up("C") ? "C" : IDS.find(up) || null; }

  function updateButtons() {
    const b = joinBoot();
    btn("join").textContent = "E joins through " + (b || "nobody");
    btn("join").disabled = busy || up("E");
    btn("leave").disabled = busy || !up("C");
    btn("crash").disabled = busy || !up("D");
    btn("ping").disabled = busy || !IDS.some(up);
    btn("lookup").disabled = busy || !up(fromSel.value) || fromSel.value === forSel.value;
    btn("gossip").disabled = busy || !up(fromSel.value);
    btn("total").disabled = busy || !up(fromSel.value);
    btn("reset").disabled = busy;
  }

  async function run(fn) {
    if (busy) return;
    busy = true;
    updateButtons();
    try { await fn(); } finally { busy = false; render(); }
  }

  function onNodeClick(id) {
    if (busy) return;
    if (up(id)) run(() => doCrash(id));
    else {
      const peers = IDS.filter((x) => up(x) && x !== id);
      run(() => doJoin(id, peers.length ? shuffle(peers)[0] : null));
    }
  }

  const ACTIONS = {
    join: () => doJoin("E", joinBoot()),
    leave: () => doLeave("C"),
    crash: () => doCrash("D"),
    ping: () => doPing(false),
    lookup: () => doLookup(fromSel.value, forSel.value),
    gossip: () => doGossip(fromSel.value),
    total: () => doTotal(fromSel.value),
    reset: async () => reset(),
  };
  document.querySelectorAll("#milestone-3 [data-m3]").forEach((b) => {
    b.addEventListener("click", () => run(ACTIONS[b.dataset.m3]));
  });
  fromSel.onchange = forSel.onchange = updateButtons;

  const ttlIn = document.getElementById("m3ttl"), pfIn = document.getElementById("m3pf");
  ttlIn.oninput = () => { TTL = +ttlIn.value; document.getElementById("m3ttlv").textContent = TTL; };
  pfIn.oninput = () => { PF = +pfIn.value; document.getElementById("m3pfv").textContent = PF.toFixed(1); updateMetrics(); };
  document.querySelectorAll("#m3speed .chip").forEach((c) => {
    c.onclick = () => {
      speed = +c.dataset.s;
      document.querySelectorAll("#m3speed .chip").forEach((x) => x.classList.toggle("on", x === c));
    };
  });

  function reset() {
    S = {};
    IDS.forEach((id) => { S[id] = { status: id === "E" ? "off" : "up", nbrs: new Set() }; });
    INITIAL.forEach(([a, b]) => { S[a].nbrs.add(b); S[b].nbrs.add(a); });
    known = new Set(["A", "B", "C", "D"]);
    lastOp = "none yet";
    logEl.innerHTML = "";
    t0 = performance.now();
    clearMarks();
    log("all", "mesh from run_demo.sh: A first, B joined through A, C through B, D through A");
    log("all", "links A-B, A-C, A-D, B-C, C-D. Each lot holds the other lots' summaries as replicas");
  }

  // ------------------------------------------------------------------ tabs and trace
  document.querySelectorAll("#m3tabs button").forEach((b) => {
    b.onclick = () => {
      document.querySelectorAll("#m3tabs button").forEach((x) => x.classList.toggle("on", x === b));
      document.querySelectorAll("#milestone-3 .vpane3, #milestone-3 .vtext3").forEach((p) => p.classList.toggle("on", p.dataset.v === b.dataset.v));
    };
  });

  const TRACE = [
    ["12:49:24", "A", "P2P first peer in the overlay, waiting for others to join"],
    ["12:49:25", "B", "P2P JOIN via bootstrap 127.0.0.1:5001"],
    ["12:49:25", "A", "P2P JOIN from B: gave 0 neighbor pointer(s) and 1 table entries"],
    ["12:49:25", "B", "P2P took 1 replica(s) from 127.0.0.1:5001"],
    ["12:49:25", "C", "P2P JOIN via bootstrap 127.0.0.1:5002"],
    ["12:49:25", "B", "P2P JOIN from C: gave 1 neighbor pointer(s) and 2 table entries"],
    ["12:49:25", "C", "P2P neighbor + A, degree now 2        (the pointer B handed out)"],
    ["12:49:26", "D", "P2P JOIN via bootstrap 127.0.0.1:5001"],
    ["12:49:26", "D", "P2P joined: neighbors ['A', 'C']"],
    ["", "", "mesh: A-B A-C A-D B-C C-D   degree A 3, B 2, C 3, D 2"],
    ["12:49:35", "E", "P2P JOIN via bootstrap 127.0.0.1:5003"],
    ["12:49:35", "C", "P2P JOIN from E: gave 2 neighbor pointer(s) and 4 table entries"],
    ["12:49:35", "E", "P2P took 4 replica(s) from 127.0.0.1:5003"],
    ["12:49:35", "E", "P2P joined: neighbors ['A', 'C']"],
    ["12:49:38", "D", "P2P LOOKUP lot/B/summary: owner only, flooding 2 neighbor(s) with TTL 3"],
    ["12:49:38", "D", "P2P LOOKUP lot/B/summary: found at B after 2 hop(s) via D->A->B"],
    ["12:49:38", "C", "P2P LEAVE sent to 4 neighbor(s)"],
    ["12:49:38", "A", "P2P neighbor - C: left gracefully, degree now 3"],
    ["12:49:38", "B", "P2P neighbor - C: left gracefully, degree now 1"],
    ["12:49:38", "E", "P2P neighbor - C: left gracefully, degree now 1"],
    ["12:49:40", "B", "P2P random sampling: linked to E to keep the mesh connected"],
    ["12:49:40", "E", "P2P random sampling: linked to B to keep the mesh connected"],
    ["", "", "D is killed with kill -9, nothing is sent"],
    ["12:49:44", "A", "P2P neighbor - D: no answer to ping, dead pointer removed"],
    ["12:49:45", "E", "P2P neighbor - D: no answer to ping, dead pointer removed"],
    ["", "", "mesh: A-B A-E B-E   still one piece, find still answers for C and D, marked STALE"],
  ];
  document.getElementById("m3trace").innerHTML = TRACE.map((l) => l[0]
    ? '<span class="t">' + l[0] + '</span>  <span class="who">lot_server_' + l[1] + "</span>  " + l[2].replace(/</g, "&lt;")
    : '<span class="gap">' + " ".repeat(10) + l[2] + "</span>").join("\n");

  reset();
})();
