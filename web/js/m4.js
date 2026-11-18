// Milestone 4: coordination simulator. Same rules as prototype/comm/clock.py,
// prototype/coord/election.py and prototype/coord/mutex.py, drawn in SVG.
// Starts where run_demo.sh ends: lots A to D running, D is the leader, E is not running.
(function () {
  const svg = document.getElementById("coord");
  if (!svg) return;

  const NS = "http://www.w3.org/2000/svg";
  const IDS = ["A", "B", "C", "D", "E"];
  const POS = { A: [110, 178], B: [280, 178], C: [450, 178], D: [620, 178], E: [790, 178] };   // one row, messages arc above it
  const R = 28;                 // node radius
  const HOP = 620;              // ms per message at normal speed
  const WORK = 650;             // ms between reading and writing the table (0.3 s in the prototype)
  const RETRY = 1400;           // a waiting lot checks who the coordinator is this often
  const SPACES = ["O-01", "O-02", "O-03", "O-04", "O-05"];
  const G = { x: 215, y: 274, w: 470, h: 192 };       // the garage panel
  const SLOT = { w: 84, h: 56, gap: 8, x0: 224, y: G.y + 36 };
  const ABORT = { abort: true };

  let N, LOCK, TABLE, VERSION, RESULTS, VERDICT, EV, LOGS;
  let lockMode = "idle";        // idle | lock | nolock
  let gen = 0, busy = false, speed = 1, inflight = 0, evSeq = 0;

  // ------------------------------------------------------------------ small helpers
  function el(tag, attrs, text) {
    const e = document.createElementNS(NS, tag);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    if (text !== undefined) e.textContent = text;
    return e;
  }
  const $ = (s) => document.querySelector(s);
  const up = (id) => N[id].status === "up";
  const proc = (id) => "lot_server_" + id;
  const bg = (p) => { p.catch(function () {}); };
  const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

  function pause(ms) {
    const g = gen;
    return new Promise((res, rej) => setTimeout(() => (g === gen ? res() : rej(ABORT)), ms / speed));
  }
  function leaderNow() {
    const l = IDS.filter((x) => up(x) && N[x].leader === x);
    return l.length ? l[l.length - 1] : null;
  }

  // ------------------------------------------------------------------ state
  function init() {
    N = {};
    IDS.forEach((id) => {
      N[id] = { status: id === "E" ? "off" : "up", c: 0, leader: id === "E" ? null : "D",
                electing: false, gotCoord: false, pending: null, holding: null, cs: false };
    });
    // clocks after the start-up elections
    N.A.c = 8; N.B.c = 8; N.C.c = 9; N.D.c = 12;
    LOCK = { coord: "D", ready: true, holder: null, queue: [] };
    TABLE = {}; VERSION = 0; RESULTS = []; VERDICT = null; EV = []; LOGS = [];
    lockMode = "idle"; evSeq = 0; inflight = 0;
  }

  // ------------------------------------------------------------------ clocks and records
  function tick(id) {
    N[id].c += 1;
    hot(id);
    return N[id].c;
  }
  function merge(id, stamp) {
    N[id].c = Math.max(N[id].c, stamp) + 1;
    hot(id);
    return N[id].c;
  }
  // one line of the merged timeline: kind is send | recv | local | mark
  function rec(id, kind, text, L, o) {
    const e = Object.assign({ i: evSeq++, id: id, kind: kind, text: text, L: L, cls: "plain" }, o || {});
    EV.push(e);
    if (EV.length > 600) EV.shift();
    dirty();
    return e;
  }
  // one console line of a lot server
  function log(id, text, cls) {
    LOGS.push({ id: id, L: N[id].c, text: text, cls: cls || "" });
    if (LOGS.length > 400) LOGS.shift();
    dirty();
  }
  function note(text, cls) {
    LOGS.push({ id: null, L: null, text: text, cls: cls || "note" });
    dirty();
  }

  // ------------------------------------------------------------------ scene: built once, updated in place
  const gBack = el("g", {}), gGarage = el("g", {}), gNodes = el("g", {}), gDots = el("g", {});
  svg.append(gBack, gGarage, gNodes, gDots);
  const nodeEl = {}, slotEl = [];
  let gQueue, tVersion, tLock, tCoord, tVerdict, lockPill;

  function build() {
    // garage panel
    gGarage.appendChild(el("rect", { class: "cg-panel", x: G.x, y: G.y, width: G.w, height: G.h, rx: 14 }));
    gGarage.appendChild(el("text", { class: "cg-title", x: G.x + 16, y: G.y + 23 }, "OVERFLOW GARAGE"));
    gGarage.appendChild(el("text", { class: "cg-sub", x: G.x + 150, y: G.y + 23 }, "one table, written by every lot"));
    tVersion = el("text", { class: "cg-ver", x: G.x + G.w - 16, y: G.y + 23 }, "version 0");
    gGarage.appendChild(tVersion);
    SPACES.forEach((s, i) => {
      const x = SLOT.x0 + i * (SLOT.w + SLOT.gap);
      const g = el("g", { class: "cslot" });
      g.appendChild(el("rect", { x: x, y: SLOT.y, width: SLOT.w, height: SLOT.h, rx: 9 }));
      g.appendChild(el("text", { class: "sid", x: x + SLOT.w / 2, y: SLOT.y + 19 }, s));
      const who = el("text", { class: "who", x: x + SLOT.w / 2, y: SLOT.y + 40 }, "free");
      g.appendChild(who);
      const badge = el("text", { class: "badge", x: x + SLOT.w / 2, y: SLOT.y + SLOT.h + 13 }, "");
      g.appendChild(badge);
      gGarage.appendChild(g);
      slotEl.push({ g: g, who: who, badge: badge });
    });
    const ly = SLOT.y + SLOT.h + 30;
    lockPill = el("rect", { class: "clock-pill", x: G.x + 14, y: ly - 14, width: 52, height: 20, rx: 10 });
    gGarage.appendChild(lockPill);
    gGarage.appendChild(el("text", { class: "clock-pill-t", x: G.x + 40, y: ly }, "LOCK"));
    tLock = el("text", { class: "cg-lock", x: G.x + 76, y: ly }, "free");
    gGarage.appendChild(tLock);
    tCoord = el("text", { class: "cg-coord", x: G.x + G.w - 16, y: ly }, "");
    gGarage.appendChild(tCoord);
    gGarage.appendChild(el("text", { class: "cg-qlabel", x: G.x + 16, y: ly + 27 }, "queue"));
    gQueue = el("g", {});
    gGarage.appendChild(gQueue);
    tVerdict = el("text", { class: "cg-verdict", x: G.x + G.w / 2, y: G.y + G.h - 12 }, "");
    gGarage.appendChild(tVerdict);

    // lot servers
    IDS.forEach((id) => {
      const [x, y] = POS[id];
      const g = el("g", { class: "cnode", "data-id": id });
      g.appendChild(el("circle", { class: "crown", cx: x, cy: y, r: R + 6 }));
      g.appendChild(el("circle", { class: "body", cx: x, cy: y, r: R }));
      g.appendChild(el("text", { class: "name", x: x, y: y }, id));
      const tag = el("g", { class: "tag" });
      tag.appendChild(el("rect", { x: x - 32, y: y + R + 37, width: 64, height: 18, rx: 9 }));
      tag.appendChild(el("text", { x: x, y: y + R + 46 }, "LEADER"));
      g.appendChild(tag);
      const chip = el("g", { class: "lc" });
      chip.appendChild(el("rect", { x: x - 31, y: y + R + 8, width: 62, height: 22, rx: 11 }));
      const ct = el("text", { x: x, y: y + R + 19 }, "L=0");
      chip.appendChild(ct);
      g.appendChild(chip);
      const sub = el("text", { class: "sub", x: x, y: y + R + 47 }, "");
      g.appendChild(sub);
      g.addEventListener("click", () => clickNode(id));
      gNodes.appendChild(g);
      nodeEl[id] = { g: g, chip: chip, ct: ct, sub: sub };
    });
  }

  function hot(id) {
    const c = nodeEl[id].chip;
    c.classList.remove("hot");
    void c.getBoundingClientRect();
    c.classList.add("hot");
    nodeEl[id].ct.textContent = "L=" + N[id].c;
  }

  function render() {
    IDS.forEach((id) => {
      const n = N[id], e = nodeEl[id];
      const cls = ["cnode", n.status];
      const isLeader = up(id) && n.leader === id;
      if (isLeader) cls.push("leader");
      if (up(id) && n.electing) cls.push("electing");
      if (up(id) && n.cs) cls.push("incs");
      else if (up(id) && n.pending) cls.push("waiting");
      e.g.setAttribute("class", cls.join(" "));
      e.ct.textContent = "L=" + n.c;
      let sub = "";
      if (n.status === "dead") sub = "crashed";
      else if (n.status === "off") sub = "not running";
      else if (n.cs) sub = "in the critical section";
      else if (n.pending) sub = "waiting for the lock";
      else if (n.electing) sub = "election running";
      else if (isLeader) sub = "";
      else sub = n.leader ? "leader is " + n.leader : "no leader known";
      e.sub.textContent = sub;
    });

    // the shared table
    tVersion.textContent = "version " + VERSION;
    const told = {};
    RESULTS.forEach((r) => { if (r.told !== "full") (told[r.told] = told[r.told] || []).push(r.driver); });
    SPACES.forEach((s, i) => {
      const owner = TABLE[s], n = (told[s] || []).length;
      slotEl[i].who.textContent = owner || "free";
      slotEl[i].g.setAttribute("class", "cslot" + (owner ? " taken" : "") + (n > 1 ? " conflict" : ""));
      slotEl[i].badge.textContent = n > 1 ? "told to " + n + " drivers" : "";
    });

    // the lock at the coordinator
    const ldr = leaderNow();
    let lockText, pill = "clock-pill";
    if (lockMode === "nolock") { lockText = "not used in this race"; pill += " off"; }
    else if (!ldr) { lockText = "no coordinator, election running"; pill += " rebuilding"; }
    else if (!LOCK.ready) { lockText = "new coordinator is rebuilding its state"; pill += " rebuilding"; }
    else if (LOCK.holder) {
      const h = N[LOCK.holder.id];
      const d = (h.holding && h.holding.driver) || (h.pending && h.pending.driver);
      lockText = "held by lot " + LOCK.holder.id + (d ? " for " + d : "");
      pill += " held";
    } else lockText = "free";
    lockPill.setAttribute("class", pill);
    tLock.textContent = lockText;
    tCoord.textContent = ldr ? "coordinator: lot " + ldr : "";
    gQueue.innerHTML = "";
    let qx = G.x + 66;
    const qy = SLOT.y + SLOT.h + 57;
    if (lockMode === "nolock" || !LOCK.queue.length) {
      gQueue.appendChild(el("text", { class: "cg-qempty", x: qx, y: qy }, lockMode === "nolock" ? "nobody asks the coordinator" : "empty"));
    } else {
      LOCK.queue.forEach((q, i) => {
        const w = 86;
        gQueue.appendChild(el("rect", { class: "cq" + (i === 0 ? " next" : ""), x: qx, y: qy - 14, width: w, height: 20, rx: 10 }));
        gQueue.appendChild(el("text", { class: "cq-t", x: qx + w / 2, y: qy }, q.id + "  ts=" + q.ts));
        qx += w + 6;
      });
    }
    tVerdict.textContent = VERDICT ? VERDICT.text : "";
    tVerdict.setAttribute("class", "cg-verdict" + (VERDICT ? " " + VERDICT.cls : ""));
    dirty();
  }

  // ------------------------------------------------------------------ flying messages
  // lot to lot: an arc above the row, higher for lots that are further apart.
  // lot to garage: a straight line down to the nearest point of the panel.
  function route(from, to) {
    if (typeof to !== "string") {
      const a = POS[from];
      return (t) => [a[0] + (to[0] - a[0]) * t, a[1] + R + 5 + (to[1] - a[1] - R - 5) * t];
    }
    const a = POS[from], b = POS[to], y0 = a[1] - R - 6;
    const h = 22 + Math.abs(b[0] - a[0]) * 0.16, mx = (a[0] + b[0]) / 2;
    return (t) => {
      const u = 1 - t;
      return [u * u * a[0] + 2 * u * t * mx + t * t * b[0], u * u * y0 + 2 * u * t * (y0 - 2 * h) + t * t * y0];
    };
  }
  const gate = (id) => [Math.max(G.x + 30, Math.min(G.x + G.w - 30, POS[id][0])), G.y];

  // resolves true if the receiver was running when the message arrived
  function fly(from, to, kind, label, quiet) {
    const g = gen;
    return new Promise((res, rej) => {
      const target = typeof to === "string" ? to : null;
      const okStart = !target || up(target);
      const at = route(from, to), a = at(0);
      const dot = el("circle", { class: "cdot " + kind + (quiet ? " quiet" : ""), cx: a[0], cy: a[1], r: quiet ? 4 : 6 });
      const txt = label ? el("text", { class: "clabel" }, label) : null;
      gDots.appendChild(dot);
      if (txt) gDots.appendChild(txt);
      inflight++;
      const dur = (quiet ? HOP * 0.7 : HOP) / speed, frac = okStart ? 1 : 0.62, start = performance.now();
      function done(ok) {
        dot.remove();
        if (txt) txt.remove();
        if (g !== gen) return rej(ABORT);
        inflight--;
        res(ok);
      }
      function step(now) {
        if (g !== gen) return done(false);
        const k = Math.min(1, (now - start) / dur);
        const e = (k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2) * frac;
        const [x, y] = at(e);
        dot.setAttribute("cx", x); dot.setAttribute("cy", y);
        if (txt) { txt.setAttribute("x", x); txt.setAttribute("y", y - 11); }
        if (k < 1) return requestAnimationFrame(step);
        if (okStart && (!target || up(target))) return done(true);
        dot.setAttribute("class", "cdot bad");
        if (txt) txt.setAttribute("class", "clabel bad");
        setTimeout(() => done(false), 300 / speed);
      }
      requestAnimationFrame(step);
    });
  }

  // one message between two lot servers. Sending ticks the sender's clock and the
  // message carries that time; receiving sets the clock to max(own, message) + 1.
  async function send(from, to, type, kind, o) {
    o = o || {};
    if (!up(from)) return null;
    const stamp = tick(from);
    const s = rec(from, "send", type + " -> " + to + (o.detail ? "  " + o.detail : ""), stamp, { cls: kind, quiet: !!o.quiet });
    const label = o.quiet ? (o.label || "") : (o.label || type) + (o.nostamp ? "" : "  L=" + stamp);
    const ok = await fly(from, to, kind, label, o.quiet);
    if (!ok) { s.fail = true; dirty(); return null; }
    const before = N[to].c;
    const at = merge(to, stamp);
    rec(to, "recv", type + " <- " + from + "  (sent at " + stamp + ")", at, { cls: kind, quiet: !!o.quiet, link: s });
    render();
    return { stamp: stamp, at: at, before: before };
  }
  const ack = (from, to, label) => send(from, to, "ACK", "ack", { quiet: true, label: label || "" });

  // ------------------------------------------------------------------ Bully election (coord/election.py)
  function setLeader(id, lid, why) {
    const n = N[id], old = n.leader;
    n.leader = lid;
    if (old !== lid) {
      log(id, "LEADER " + (old || "none") + " -> " + (lid || "none") + " (" + why + ")", lid === id ? "ok" : "");
      if (LOCK.coord === id && lid !== id && lid) {
        LOCK.ready = false;
        log(id, "MUTEX stepping down, " + lid + " coordinates now");
      }
    }
    render();
  }

  async function elect(id, reason) {
    const n = N[id];
    if (!up(id) || n.electing) return;
    n.electing = true;
    render();
    log(id, "ELECTION started: " + reason);
    try {
      while (up(id)) {
        n.gotCoord = false;
        const higher = IDS.filter((x) => x > id);
        let alive = [];
        if (higher.length) {
          log(id, "ELECTION -> " + higher.join(", ") + " (every lot with a higher id)");
          const res = await Promise.all(higher.map(async (h) => {
            const m = await send(id, h, "ELECTION", "elect");
            if (!m) return null;
            log(h, "ELECTION from " + id + ": answered OK, taking over the election");
            const a = ack(h, id, "OK");
            bg(elect(h, "ELECTION from lower lot " + id));
            return (await a) ? h : null;
          }));
          alive = res.filter(Boolean);
        }
        if (!up(id)) return;
        if (!alive.length) {
          if (higher.length) await pause(350);          // nobody higher answered within the timeout
          await win(id);
          return;
        }
        log(id, "ELECTION: " + alive.join(", ") + " answered OK, waiting for a COORDINATOR");
        let waited = 0;
        while (!n.gotCoord && up(id) && waited < 14000) { await pause(120); waited += 120; }
        if (n.gotCoord || !up(id)) return;
        log(id, "ELECTION: no COORDINATOR arrived, starting again");
      }
    } finally {
      n.electing = false;
      render();
    }
  }

  async function win(id) {
    setLeader(id, id, "no higher lot answered, I am the coordinator");
    LOCK = { coord: id, ready: false, holder: null, queue: [] };
    render();
    const others = IDS.filter((x) => x !== id);
    log(id, "COORDINATOR -> " + others.join(", "));
    const res = await Promise.all(others.map(async (o) => {
      const m = await send(id, o, "COORDINATOR", "coord");
      if (!m) return null;
      if (o > id) {
        // a lower lot thinks it won: bully it
        log(o, "COORDINATOR from " + id + ", but my id is higher: new election");
        bg(elect(o, "lower lot " + id + " claimed leadership"));
      } else {
        // switch first, then ACK: when the winner has every ACK, every lot has switched
        setLeader(o, id, "COORDINATOR from " + id);
        N[o].gotCoord = true;
      }
      return (await ack(o, id)) ? o : null;
    }));
    const heard = res.filter(Boolean);
    log(id, "COORDINATOR acknowledged by " + (heard.join(", ") || "nobody, I am alone"));
    if (up(id) && N[id].leader === id && LOCK.coord === id) await rebuild(id);
  }

  // a new coordinator asks every lot what it holds and what it waits for (MUTEX_STATE)
  async function rebuild(id) {
    const lots = IDS.filter(up);
    const snap = (o) => ({ id: o, holding: N[o].holding ? N[o].holding.ts : null, pending: N[o].pending ? N[o].pending.ts : null });
    const res = await Promise.all(lots.map(async (o) => {
      if (o === id) return snap(o);
      const m = await send(id, o, "MUTEX_STATE", "state");
      if (!m) return null;
      const s = snap(o);
      await ack(o, id);
      return s;
    }));
    if (!up(id) || LOCK.coord !== id) return;
    const got = res.filter(Boolean);
    const h = got.find((s) => s.holding !== null);
    LOCK.holder = h ? { id: h.id, ts: h.holding } : null;
    LOCK.queue = got.filter((s) => s.pending !== null && !(h && h.id === s.id))
      .map((s) => ({ id: s.id, ts: s.pending })).sort(byTs);
    LOCK.ready = true;
    log(id, "MUTEX I coordinate now; state from " + got.length + " lot(s): holder " +
      (LOCK.holder ? LOCK.holder.id : "none") + ", queue [" + LOCK.queue.map(qs).join(", ") + "]", "ok");
    render();
    grantNext(id);
  }
  const byTs = (a, b) => a.ts - b.ts || (a.id < b.id ? -1 : 1);
  const qs = (q) => q.id + "(ts=" + q.ts + ")";

  // ------------------------------------------------------------------ centralized mutual exclusion (coord/mutex.py)
  // coordinator side
  function coordRequest(c, from, ts) {
    if (LOCK.coord !== c || !LOCK.ready || !up(c)) return "recovering";
    if (LOCK.holder && LOCK.holder.id === from) return "granted";
    if (!LOCK.holder && !LOCK.queue.length) {
      LOCK.holder = { id: from, ts: ts };
      log(c, "MUTEX overflow: REQUEST from " + from + " (ts=" + ts + ") -> GRANT (free)");
      render();
      return "granted";
    }
    if (!LOCK.queue.some((q) => q.id === from)) {
      LOCK.queue.push({ id: from, ts: ts });
      LOCK.queue.sort(byTs);
      log(c, "MUTEX overflow: REQUEST from " + from + " (ts=" + ts + ") -> queued behind " +
        (LOCK.holder ? LOCK.holder.id : "?") + ", queue [" + LOCK.queue.map(qs).join(", ") + "]");
    }
    render();
    return "queued";
  }
  function coordRelease(c, from) {
    if (LOCK.coord !== c || !LOCK.ready || !up(c)) return false;
    if (LOCK.holder && LOCK.holder.id === from) {
      LOCK.holder = null;
      log(c, "MUTEX overflow: RELEASE by " + from + (LOCK.queue.length ? ", next: " + LOCK.queue[0].id : ", nobody waiting"));
      render();
      grantNext(c);
    }
    return true;
  }
  function grantNext(c) {
    if (LOCK.coord !== c || !LOCK.ready || LOCK.holder || !LOCK.queue.length || !up(c)) return;
    const nx = LOCK.queue.shift();
    LOCK.holder = { id: nx.id, ts: nx.ts };
    log(c, "MUTEX overflow: GRANT -> " + nx.id + " (ts=" + nx.ts + ")");
    render();
    bg((async () => {
      let ok = true;
      if (nx.id !== c) ok = !!(await send(c, nx.id, "MUTEX_GRANT", "grant", { label: "GRANT" }));
      const n = N[nx.id];
      // a lot only accepts a grant from the lot it currently believes is the leader
      if (ok && up(nx.id) && n.pending && n.pending.ts === nx.ts && n.leader === c) n.pending.granted = true;
    })());
  }

  // requester side
  async function acquire(id, driver) {
    const ts = tick(id);
    rec(id, "local", "wants the overflow lock, request ts=" + ts, ts, { cls: "req" });
    const p = N[id].pending = { ts: ts, driver: driver, granted: false };
    render();
    let first = true, queued = false;
    while (!p.granted) {
      const c = N[id].leader;
      let r = "fail";
      if (c && (c === id || up(c))) {
        if (first) log(id, "MUTEX want overflow: REQUEST ts=" + ts + " -> coordinator " + c);
        else log(id, "MUTEX re-sends its request (same ts=" + ts + ") -> coordinator " + c);
        first = false;
        if (c === id) r = coordRequest(c, id, ts);
        else {
          const m = await send(id, c, "MUTEX_REQUEST", "req", { label: "REQUEST ts=" + ts, nostamp: true, detail: "ts=" + ts });
          if (m) {
            r = coordRequest(c, id, ts);
            if (!(await ack(c, id, r === "granted" ? "granted" : r === "queued" ? "queued" : ""))) r = "fail";
          }
        }
      }
      if (p.granted || r === "granted") break;
      queued = true;
      if (r === "queued") {
        while (!p.granted && N[id].leader === c) await pause(100);
      } else {
        await pause(RETRY);
      }
    }
    p.granted = true;
    N[id].holding = { ts: ts, driver: driver };
    N[id].pending = null;
    log(id, "MUTEX GRANTED overflow (ts=" + ts + ")" + (queued ? " after waiting in the queue" : " at once"), "ok");
    render();
  }

  async function release(id) {
    for (;;) {
      const c = N[id].leader;
      if (c && (c === id || up(c))) {
        if (c === id) { if (coordRelease(c, id)) break; }
        else {
          const m = await send(id, c, "MUTEX_RELEASE", "rel", { label: "RELEASE" });
          if (m && coordRelease(c, id)) { await ack(c, id); break; }
        }
      }
      await pause(RETRY);
    }
    N[id].holding = null;
    log(id, "MUTEX released overflow");
    render();
  }

  // ------------------------------------------------------------------ the shared resource (overflow.py)
  // read the table, pick the first free space, write it back: the critical section
  async function assign(id, driver, useLock, hooks) {
    if (useLock) await acquire(id, driver);
    N[id].cs = true;
    const Lin = tick(id);
    rec(id, "local", "CS enter: assign for " + driver, Lin, { cls: "cs" });
    log(id, "CS enter overflow: assign for " + driver + (useLock ? "" : " WITHOUT the lock"), useLock ? "" : "bad");
    render();
    await fly(id, gate(id), "rw", "read");
    const snapshot = Object.assign({}, TABLE), ver = VERSION;
    const free = SPACES.find((s) => !snapshot[s]);
    await pause(WORK);
    let told = "full";
    if (free) {
      await fly(id, gate(id), "rw", "write " + free);
      snapshot[free] = driver;
      TABLE = snapshot;                 // the whole table goes back: a stale copy overwrites newer writes
      VERSION = ver + 1;
      told = free;
    }
    const Lout = tick(id);
    rec(id, "local", "CS exit: " + driver + " -> " + (free || "garage full") + " (table version " + VERSION + ")", Lout, { cls: "cs" });
    log(id, "CS exit  overflow: assign " + driver + " -> " + (free || "overflow garage full") + " (table version " + VERSION + ")");
    N[id].cs = false;
    RESULTS.push({ driver: driver, via: id, told: told, version: VERSION });
    render();
    if (hooks && hooks.afterCS) hooks.afterCS();
    if (useLock) await release(id);
  }

  function judge() {
    const told = {};
    RESULTS.forEach((r) => { if (r.told !== "full") (told[r.told] = told[r.told] || []).push(r); });
    const conflicts = Object.keys(told).filter((s) => told[s].length > 1);
    const lost = RESULTS.filter((r) => r.told !== "full" && TABLE[r.told] !== r.driver);
    const full = RESULTS.filter((r) => r.told === "full").length;
    if (conflicts.length || lost.length) {
      conflicts.forEach((s) => note("CONFLICT: " + s + " was given to " + told[s].length + " drivers: " +
        told[s].map((r) => r.driver + " (via " + r.via + ")").join(", "), "bad"));
      lost.forEach((r) => note("LOST UPDATE: " + r.driver + " was told " + r.told + ", but the table says " + (TABLE[r.told] || "nobody") + " has it", "bad"));
      VERDICT = { cls: "bad", text: "CONFLICT: " + conflicts.map((s) => s + " was given to " + told[s].length + " drivers").join(", ") +
        (lost.length ? ". " + lost.length + " lost update" + (lost.length > 1 ? "s" : "") : "") };
    } else {
      const n = RESULTS.length - full;
      VERDICT = { cls: "ok", text: "no conflict: " + n + " driver" + (n === 1 ? "" : "s") + " got " + n + " different space" + (n === 1 ? "" : "s") +
        (full ? ", " + full + " told the garage is full" : "") };
      note(VERDICT.text + ", the table agrees with every reply", "ok");
    }
    render();
  }

  async function race(useLock, drivers, kill) {
    let lots = ["A", "B", "C"].filter(up);
    if (!lots.length) lots = IDS.filter(up).slice(0, 3);
    const ldr = leaderNow();
    if (kill) {
      lots = IDS.filter((x) => up(x) && x !== ldr).slice(0, 3);
      if (!ldr || !lots.length) { note("Start at least two lots first: one to crash and one to ask through."); return; }
    }
    if (!lots.length) { note("No lot server is running. Start one first."); return; }
    TABLE = {}; VERSION = 0; RESULTS = []; VERDICT = null;
    lockMode = useLock ? "lock" : "nolock";
    render();
    note(drivers + " drivers ask lots " + lots.join(", ") + " for an overflow space at the same time, " +
      (useLock ? "with the lock" : "WITHOUT the lock"), useLock ? "note" : "bad");
    const per = {};
    for (let d = 0; d < drivers; d++) (per[lots[d % lots.length]] = per[lots[d % lots.length]] || []).push("driver" + (d + 1));
    let done = 0, killed = false;
    const hooks = {
      afterCS: () => {
        done++;
        if (kill && !killed && done >= 2 && up(ldr)) {
          killed = true;
          note("the coordinator, lot " + ldr + ", crashes in the middle of the race", "bad");
          crash(ldr);
          bg((async () => { await pause(500); await detect(ldr); })());
        }
      }
    };
    await Promise.all(lots.map(async (id, li) => {
      if (useLock) {
        await pause(li * 90);
        for (const d of per[id]) await assign(id, d, true, hooks);     // one request per lot at a time
      } else {
        await Promise.all(per[id].map(async (d, di) => { await pause(li * 90 + di * 220); await assign(id, d, false, hooks); }));
      }
    }));
    await settle();
    judge();
  }

  // ------------------------------------------------------------------ failures
  function crash(id) {
    const n = N[id];
    n.status = "dead"; n.electing = false; n.pending = null; n.holding = null; n.cs = false;
    rec(id, "mark", "crashed", null, { cls: "bad" });
    LOGS.push({ id: id, L: n.c, text: "process killed", cls: "bad" });
    if (LOCK.coord === id) { LOCK.ready = false; LOCK.holder = null; LOCK.queue = []; }
    render();
  }

  // a follower's LEADER_PING gets no answer: it starts an election
  async function detect(dead) {
    const f = IDS.filter(up);
    if (!f.length) return;
    const d = f[0];
    log(d, "LEADER_PING -> " + dead + " (every follower pings the leader every 2 s)");
    const m = await send(d, dead, "LEADER_PING", "ping");
    if (m) { await ack(dead, d); return; }
    log(d, "LEADER " + dead + " did not answer the ping within 1 s: it failed", "bad");
    setLeader(d, null, "leader " + dead + " unreachable");
    await elect(d, "leader " + dead + " failed");
  }

  async function startLot(id) {
    const n = N[id];
    const fresh = n.status === "off";
    n.status = "up"; n.leader = null; n.electing = false; n.pending = null; n.holding = null; n.gotCoord = false;
    rec(id, "mark", "started", null, { cls: "ok" });
    log(id, fresh ? "started for the first time, clock at L=0" : "restarted, clock continues from its own log at L=" + n.c, "ok");
    render();
    await pause(300);
    await elect(id, "starting up");
  }

  // wait until every election, state rebuild and message has finished
  async function settle() {
    let quiet = 0, waited = 0;
    while (quiet < 3 && waited < 60000) {
      await pause(110);
      waited += 110;
      const ldr = leaderNow();
      const calm = inflight === 0 && !IDS.some((x) => up(x) && N[x].electing) && (!IDS.some(up) || (ldr && LOCK.ready && LOCK.coord === ldr));
      quiet = calm ? quiet + 1 : 0;
    }
    render();
  }

  // ------------------------------------------------------------------ operations behind the buttons
  const OPS = {
    async send(far) {
      const a = $("#m4from").value, b = $("#m4to").value;
      if (a === b) { note("Pick two different lots."); return; }
      if (!up(a)) { note("Lot " + a + " is not running, it cannot send."); return; }
      if (far) {
        N[a].c += 40;
        hot(a);
        log(a, "40 local events happened here, the clock is far ahead now");
        render();
        await pause(350);
      }
      const m = await send(a, b, "MESSAGE", "plain");
      if (!m) { note("Lot " + b + " is not running, the message was lost. " + a + " still counted the send: L=" + N[a].c + ".", "bad"); return; }
      log(a, "send rule: C = C + 1 = " + m.stamp + ", the message carries L=" + m.stamp);
      log(b, "receive rule: C = max(" + m.before + ", " + m.stamp + ") + 1 = " + m.at, "ok");
      await ack(b, a);
    },
    async crash() {
      const ldr = leaderNow();
      if (!ldr) { note("There is no leader to crash."); return; }
      note("kill -9 on the leader, lot " + ldr, "bad");
      crash(ldr);
      await pause(600);
      await detect(ldr);
      await settle();
    },
    async restart() {
      const dead = IDS.filter((x) => N[x].status === "dead");
      if (!dead.length) { note("No crashed lot to restart. Crash one first, or click E to start a fifth lot."); return; }
      for (const id of dead) { bg(startLot(id)); await pause(250); }
      await settle();
    },
    async elect() {
      const f = IDS.filter(up);
      if (!f.length) { note("No lot server is running."); return; }
      note("client.py --lot " + f[0] + " elect");
      await elect(f[0], "asked by a client");
      await settle();
    },
    async racebad() { await race(false, 6, false); },
    async racegood() { await race(true, 6, false); },
    async racekill() { await race(true, 8, true); }
  };

  async function run(fn) {
    if (busy) return;
    const g = gen;
    busy = true;
    setButtons();
    try { await fn(); } catch (e) { if (e !== ABORT) throw e; }
    finally { if (g === gen) { busy = false; setButtons(); render(); } }
  }

  function clickNode(id) {
    if (busy) return;
    run(async () => {
      if (up(id)) {
        const wasLeader = leaderNow() === id;
        note("kill -9 on lot " + id, "bad");
        crash(id);
        if (wasLeader) { await pause(600); await detect(id); }
        await settle();
      } else {
        await startLot(id);
        await settle();
      }
    });
  }

  function setButtons() {
    document.querySelectorAll("[data-m4]").forEach((b) => { if (b.dataset.m4 !== "reset") b.disabled = busy; });
    svg.classList.toggle("busy", busy);
  }

  function reset() {
    gen++;
    gDots.innerHTML = "";
    busy = false;
    init();
    IDS.forEach((id) => nodeEl[id].chip.classList.remove("hot"));
    note("Lots A to D are running and D is the leader. E is a fifth lot that has not been started.");
    setButtons();
    render();
  }

  // ------------------------------------------------------------------ panels: live state, race result, console, timeline
  let rafPending = false;
  function dirty() {
    if (rafPending) return;
    rafPending = true;
    requestAnimationFrame(() => { rafPending = false; drawState(); drawRace(); drawConsole(); drawOrdered(); drawTimeline(); });
  }

  function drawState() {
    const ldr = leaderNow();
    const followers = IDS.filter((x) => up(x) && x !== ldr && N[x].leader === ldr);
    const rows = [
      ["leader", ldr ? "lot " + ldr + (followers.length ? ", followed by " + followers.join(", ") : ", alone") : "none, election running"],
      ["lock", lockMode === "nolock" ? "not used" : !ldr || !LOCK.ready ? "rebuilding" : LOCK.holder ? "held by " + LOCK.holder.id : "free"],
      ["queue", LOCK.queue.length ? LOCK.queue.map((q) => q.id + " ts=" + q.ts).join(", ") : "empty"],
      ["table", "version " + VERSION + ", " + Object.keys(TABLE).length + " of 5 taken"],
      ["clocks", IDS.filter((x) => N[x].status !== "off").map((x) => x + " " + N[x].c).join(", ")]
    ];
    $("#m4state").innerHTML = rows.map((r) => "<tr><td>" + r[0] + "</td><td>" + esc(r[1]) + "</td></tr>").join("");
    $("#m4head").textContent = ldr ? "leader " + ldr + ", " + IDS.filter(up).length + " lots running" : "no leader";
  }

  function drawRace() {
    const box = $("#m4race");
    if (!RESULTS.length) { box.innerHTML = '<div class="m4empty">Run a race to see who was told which space.</div>'; return; }
    const told = {};
    RESULTS.forEach((r) => { if (r.told !== "full") told[r.told] = (told[r.told] || 0) + 1; });
    const rows = RESULTS.slice().sort((a, b) => +a.driver.slice(6) - +b.driver.slice(6)).map((r) => {
      const bad = r.told !== "full" && (told[r.told] > 1 || (VERDICT && TABLE[r.told] !== r.driver));
      return '<tr class="' + (bad ? "bad" : r.told === "full" ? "full" : "ok") + '"><td>' + r.driver + "</td><td>" + r.via + "</td><td>" +
        (r.told === "full" ? "garage full" : r.told) + "</td><td>" + r.version + "</td></tr>";
    }).join("");
    box.innerHTML = '<table class="m4res"><tr><th>driver</th><th>via</th><th>told</th><th>version</th></tr>' + rows + "</table>" +
      (VERDICT ? '<div class="m4verdict ' + VERDICT.cls + '">' + esc(VERDICT.text) + "</div>" : "");
  }

  function drawConsole() {
    const box = $("#m4console");
    const stick = box.scrollTop + box.clientHeight >= box.scrollHeight - 30;
    box.innerHTML = LOGS.slice(-220).map((l) => {
      if (!l.id) return '<div class="say ' + l.cls + '">' + esc(l.text) + "</div>";
      return '<div><span class="t">L=' + l.L + '</span><span class="who">' + proc(l.id) + '</span><span class="' + l.cls + '">' + esc(l.text) + "</span></div>";
    }).join("");
    if (stick) box.scrollTop = box.scrollHeight;
  }

  // every send, receive and local event of every lot, sorted by (Lamport time, process): event_order.py
  function drawOrdered() {
    const box = $("#m4ordered");
    const stick = box.scrollTop + box.clientHeight >= box.scrollHeight - 30;
    const ev = EV.filter((e) => e.L !== null).slice(-260).sort((a, b) => a.L - b.L || (a.id < b.id ? -1 : a.id > b.id ? 1 : a.i - b.i));
    box.innerHTML = ev.map((e) => '<div class="' + e.kind + (e.fail ? " fail" : "") + '"><span class="t">L=' + e.L + '</span><span class="who">' +
      proc(e.id) + '</span><span class="k">' + (e.kind === "local" ? "event" : e.kind) + "</span>" + esc(e.text) + (e.fail ? "  (no answer)" : "") + "</div>").join("") ||
      '<div class="say note">Nothing yet. Every send and receive will show up here in Lamport order.</div>';
    if (stick) box.scrollTop = box.scrollHeight;
  }

  // space-time diagram: one lane per lot, time runs to the right
  const tl = document.getElementById("m4tl");
  const LANE = 34, TOP = 30, STEP = 30, LEFT = 22;
  function drawTimeline() {
    const wrap = tl.parentNode;
    const stick = wrap.scrollLeft + wrap.clientWidth >= wrap.scrollWidth - 40;
    const ev = EV.slice(-150);
    const xOf = new Map();
    ev.forEach((e, k) => xOf.set(e, LEFT + k * STEP));
    const w = Math.max(wrap.clientWidth - 2, LEFT + ev.length * STEP + 30), h = TOP + LANE * IDS.length - 6;
    tl.setAttribute("viewBox", "0 0 " + w + " " + h);
    tl.setAttribute("width", w); tl.setAttribute("height", h);
    const parts = [];
    IDS.forEach((id, k) => {
      const y = TOP + k * LANE;
      parts.push('<line class="tl-lane' + (N[id].status === "up" ? "" : " down") + '" x1="0" y1="' + y + '" x2="' + w + '" y2="' + y + '"/>');
    });
    const yOf = (id) => TOP + IDS.indexOf(id) * LANE;
    ev.forEach((e) => {
      if (e.kind !== "recv" || !e.link || !xOf.has(e.link)) return;
      parts.push('<line class="tl-msg ' + e.cls + (e.quiet ? " quiet" : "") + '" x1="' + xOf.get(e.link) + '" y1="' + yOf(e.link.id) +
        '" x2="' + xOf.get(e) + '" y2="' + yOf(e.id) + '"/>');
    });
    ev.forEach((e) => {
      const x = xOf.get(e), y = yOf(e.id);
      if (e.kind === "mark") {
        parts.push('<text class="tl-mark ' + e.cls + '" x="' + x + '" y="' + (y + 4) + '">' + (e.cls === "bad" ? "\u2715" : "\u25B6") + "</text>");
        return;
      }
      parts.push('<circle class="tl-ev ' + e.kind + " " + e.cls + (e.fail ? " fail" : "") + (e.quiet ? " quiet" : "") + '" cx="' + x + '" cy="' + y + '" r="' + (e.quiet ? 3 : 4.5) + '"><title>' +
        esc(proc(e.id) + "  L=" + e.L + "  " + e.kind + " " + e.text) + "</title></circle>");
      if (!e.quiet || e.kind === "recv") parts.push('<text class="tl-l" x="' + x + '" y="' + (y - 8) + '">' + e.L + "</text>");
    });
    tl.innerHTML = parts.join("");
    if (stick) wrap.scrollLeft = wrap.scrollWidth;
  }

  // ------------------------------------------------------------------ recorded output of the real prototype
  const TRACES = {
    crash: [
      "# lot D is the leader. kill -9 on lot D at 16:01:35. Lot C notices with its next LEADER_PING.",
      "[16:01:36 L=691] [lot_server_C] LEADER D did not answer the ping within 1s -> it failed",
      "[16:01:36 L=691] [lot_server_C] LEADER D -> none (leader D unreachable)",
      "[16:01:36 L=691] [lot_server_C] ELECTION #2 started: leader D failed",
      "[16:01:36 L=691] [lot_server_C] ELECTION -> D, E (every node with a higher id)",
      "[16:01:36 L=693] [lot_server_C] LEADER none -> C (no higher node answered, I am the coordinator)",
      "[16:01:36 L=693] [lot_server_C] COORDINATOR -> A, B, D, E",
      "[16:01:36 L=696] [lot_server_A] LEADER D -> C (COORDINATOR from C)",
      "[16:01:36 L=697] [lot_server_B] LEADER D -> C (COORDINATOR from C)",
      "[16:01:36 L=700] [lot_server_C] COORDINATOR acknowledged by A, B",
      "[16:01:36 L=710] [lot_server_C] MUTEX[coord] I coordinate now; state from 3 node(s): holders {}, queued {}",
      "",
      "# lot D is started again five seconds later. It has the highest id, so it takes the leadership back.",
      "[16:01:41 L=758] [lot_server_D] ELECTION #1 started: starting up",
      "[16:01:41 L=758] [lot_server_D] ELECTION -> E (every node with a higher id)",
      "[16:01:41 L=760] [lot_server_D] LEADER none -> D (no higher node answered, I am the coordinator)",
      "[16:01:41 L=760] [lot_server_D] COORDINATOR -> A, B, C, E",
      "[16:01:41 L=765] [lot_server_C] LEADER C -> D (COORDINATOR from D)",
      "[16:01:41 L=765] [lot_server_C] MUTEX[coord] stepping down, D coordinates now",
      "[16:01:41 L=767] [lot_server_A] LEADER C -> D (COORDINATOR from D)",
      "[16:01:41 L=767] [lot_server_B] LEADER C -> D (COORDINATOR from D)",
      "[16:01:41 L=770] [lot_server_D] COORDINATOR acknowledged by A, B, C",
      "[16:01:41 L=781] [lot_server_D] MUTEX[coord] I coordinate now; state from 4 node(s): holders {}, queued {}"
    ],
    nolock: [
      "$ python3 mutex_race.py --no-lock",
      "table reset through lot A (inside the lock)",
      "6 drivers ask lots A,B,C for an overflow space at the same time, WITHOUT the lock",
      "  ... all 6 replies in after 0.3s",
      "",
      "  driver     via  told        waited  version",
      "  driver1    A    O-01         0.30s  1",
      "  driver2    B    O-01         0.30s  1",
      "  driver3    C    O-01         0.30s  1",
      "  driver4    A    O-01         0.30s  1",
      "  driver5    B    O-01         0.30s  1",
      "  driver6    C    O-01         0.30s  1",
      "",
      "  shared table now (version 1): {'O-01': 'driver2'}",
      "",
      "  CONFLICT: O-01 was given to 6 drivers: driver1 (via A), driver2 (via B), driver3 (via C), driver4 (via A), driver5 (via B), driver6 (via C)",
      "  LOST UPDATE: driver1 was told O-01, but the table says driver2 has it",
      "  LOST UPDATE: driver3 was told O-01, but the table says driver2 has it",
      "  LOST UPDATE: driver4 was told O-01, but the table says driver2 has it",
      "  LOST UPDATE: driver5 was told O-01, but the table says driver2 has it",
      "  LOST UPDATE: driver6 was told O-01, but the table says driver2 has it",
      "  several writes produced the same table version [1]: they overwrote each other"
    ],
    lock: [
      "$ python3 mutex_race.py",
      "table reset through lot A (inside the lock)",
      "6 drivers ask lots A,B,C for an overflow space at the same time, with the distributed lock",
      "  ... all 6 replies in after 1.5s",
      "",
      "  driver     via  told        waited  version",
      "  driver1    A    O-03         0.92s  3",
      "  driver2    B    O-01         0.31s  1",
      "  driver3    C    O-02         0.61s  2",
      "  driver4    A    (overflow garage full)  1.53s  5",
      "  driver5    B    O-04         1.22s  4",
      "  driver6    C    O-05         1.52s  5",
      "",
      "  shared table now (version 5): {'O-01': 'driver2', 'O-02': 'driver3', 'O-03': 'driver1', 'O-04': 'driver5', 'O-05': 'driver6'}",
      "",
      "  no conflict: 5 driver(s) got 5 different space(s), the table agrees with every reply, 1 told the garage is full",
      "",
      "# the same race on the coordinator's console (lot D). The queue is ordered by request timestamp.",
      "[16:01:34 L=605] [lot_server_D] MUTEX[coord] overflow: REQUEST from B (ts=581) -> GRANT (free)",
      "[16:01:34 L=606] [lot_server_D] MUTEX[coord] overflow: REQUEST from C (ts=583) -> queued behind B, queue ['C(ts=583)']",
      "[16:01:34 L=607] [lot_server_D] MUTEX[coord] overflow: REQUEST from A (ts=601) -> queued behind B, queue ['C(ts=583)', 'A(ts=601)']",
      "[16:01:34 L=620] [lot_server_D] MUTEX[coord] overflow: RELEASE by B, next: C",
      "[16:01:34 L=620] [lot_server_D] MUTEX[coord] overflow: GRANT -> C (ts=583)",
      "[16:01:34 L=626] [lot_server_D] MUTEX[coord] overflow: REQUEST from B (ts=623) -> queued behind C, queue ['A(ts=601)', 'B(ts=623)']",
      "[16:01:34 L=628] [lot_server_D] MUTEX[coord] overflow: RELEASE by C, next: A",
      "[16:01:34 L=628] [lot_server_D] MUTEX[coord] overflow: GRANT -> A (ts=601)",
      "[16:01:35 L=644] [lot_server_D] MUTEX[coord] overflow: RELEASE by A, next: B",
      "[16:01:35 L=644] [lot_server_D] MUTEX[coord] overflow: GRANT -> B (ts=623)",
      "[16:01:35 L=665] [lot_server_D] MUTEX[coord] overflow: RELEASE by B, next: C",
      "[16:01:35 L=665] [lot_server_D] MUTEX[coord] overflow: GRANT -> C (ts=630)",
      "[16:01:35 L=671] [lot_server_D] MUTEX[coord] overflow: RELEASE by C, next: A",
      "[16:01:35 L=671] [lot_server_D] MUTEX[coord] overflow: GRANT -> A (ts=646)",
      "[16:01:35 L=677] [lot_server_D] MUTEX[coord] overflow: RELEASE by A, nobody waiting"
    ],
    kill: [
      "$ python3 mutex_race.py --drivers 8 --kill-leader 0.6",
      "the leader (lock coordinator) is lot D, pid 1879: it will be killed 0.6s into the race; requests go through lots A,B,C",
      "8 drivers ask lots A,B,C for an overflow space at the same time, with the distributed lock",
      "  ... 0.6s in: killed the leader, lot D (pid 1879)",
      "  ... all 8 replies in after 2.2s",
      "  ... the leader is now lot C",
      "",
      "  driver     via  told        waited  version",
      "  driver1    A    O-05         2.16s  5",
      "  driver2    B    O-02         1.61s  2",
      "  driver3    C    O-01         0.31s  1",
      "  driver4    A    O-03         1.55s  3",
      "  driver5    B    (overflow garage full)  2.16s  5",
      "  driver6    C    O-04         1.85s  4",
      "  driver7    A    (overflow garage full)  2.16s  5",
      "  driver8    B    (overflow garage full)  2.17s  5",
      "",
      "  no conflict: 5 driver(s) got 5 different space(s), the table agrees with every reply, 3 told the garage is full",
      "",
      "# lot C's console. D died while lot A held the lock and lot C was waiting. C wins the election,",
      "# asks every lot what it holds, and continues from exactly that state.",
      "[16:01:47 L=1009] [lot_server_C] ELECTION from A: answered OK, taking over the election",
      "[16:01:47 L=1009] [lot_server_C] ELECTION -> D, E (every node with a higher id)",
      "[16:01:47 L=1011] [lot_server_C] LEADER D -> C (no higher node answered, I am the coordinator)",
      "[16:01:47 L=1011] [lot_server_C] COORDINATOR -> A, B, D, E",
      "[16:01:47 L=1019] [lot_server_C] COORDINATOR acknowledged by A, B",
      "[16:01:47 L=1029] [lot_server_C] MUTEX[coord] I coordinate now; state from 3 node(s): holders {'overflow': 'A'}, queued {'overflow': ['C(ts=995)']}",
      "[16:01:47 L=1043] [lot_server_C] MUTEX[coord] overflow: RELEASE by A, next: C",
      "[16:01:47 L=1043] [lot_server_C] MUTEX[coord] overflow: GRANT -> C (ts=995)",
      "[16:01:47 L=1049] [lot_server_C] MUTEX GRANTED overflow by C after 1.24s in the queue (ts=995)",
      "[16:01:47 L=1067] [lot_server_C] CS exit  overflow: assign driver6 -> O-04 (table version 4)"
    ],
    order: [
      "$ python3 event_order.py --types REQUEST,REPLY --grep A-03",
      "L=1     16:01:33.883  client_alice     send REQUEST      -> 127.0.0.1:5001        reserve client=alice space=A-03",
      "L=547   16:01:33.884  lot_server_A     recv REQUEST      <- 127.0.0.1:55462       reserve client=alice space=A-03 (sent at 1)",
      "L=548   16:01:33.885  lot_server_A     send REPLY        -> 127.0.0.1:55462       OK op=reserve space=A-03 granted=True",
      "L=549   16:01:33.885  client_alice     recv REPLY        <- 127.0.0.1:5001        OK op=reserve space=A-03 granted=True (sent at 548)",
      "",
      "$ python3 event_order.py --types MUTEX_REQUEST,MUTEX_GRANT,MUTEX_RELEASE",
      "L=582   16:01:34.350  lot_server_B     send MUTEX_REQUEST -> 127.0.0.1:5004        {\"resource\": \"overflow\", \"node\": \"B\", ..",
      "L=584   16:01:34.351  lot_server_C     send MUTEX_REQUEST -> 127.0.0.1:5004        {\"resource\": \"overflow\", \"node\": \"C\", ..",
      "L=590   16:01:34.342  lot_server_A     send MUTEX_REQUEST -> 127.0.0.1:5004        {\"resource\": \"overflow\", \"node\": \"A\", ..",
      "L=591   16:01:34.342  lot_server_D     recv MUTEX_REQUEST <- 127.0.0.1:55472       {\"resource\": \"overflow\", \"node\": \"A\", ..",
      "L=594   16:01:34.344  lot_server_A     send MUTEX_RELEASE -> 127.0.0.1:5004        {\"resource\": \"overflow\", \"node\": \"A\", ..",
      "L=595   16:01:34.344  lot_server_D     recv MUTEX_RELEASE <- 127.0.0.1:55478       {\"resource\": \"overflow\", \"node\": \"A\", ..",
      "L=597   16:01:34.351  lot_server_D     recv MUTEX_REQUEST <- 127.0.0.1:55502       {\"resource\": \"overflow\", \"node\": \"B\", ..",
      "L=605   16:01:34.351  lot_server_D     recv MUTEX_REQUEST <- 127.0.0.1:55504       {\"resource\": \"overflow\", \"node\": \"C\", ..",
      "L=615   16:01:34.653  lot_server_B     send MUTEX_RELEASE -> 127.0.0.1:5004        {\"resource\": \"overflow\", \"node\": \"B\", ..",
      "L=619   16:01:34.654  lot_server_D     recv MUTEX_RELEASE <- 127.0.0.1:55522       {\"resource\": \"overflow\", \"node\": \"B\", ..",
      "L=621   16:01:34.655  lot_server_D     send MUTEX_GRANT  -> 127.0.0.1:5003        {\"resource\": \"overflow\", \"req_id\": \"48c8..",
      "L=622   16:01:34.656  lot_server_C     recv MUTEX_GRANT  <- 127.0.0.1:35504       {\"resource\": \"overflow\", \"req_id\": \"48c8..",
      "",
      "$ python3 event_order.py --check",
      "2063 log lines from 20 processes",
      "receives after their send (C_recv > C_msg): 1019 ok, 0 violated",
      "replies after their request (C_reply > C_request): 326 ok, 0 violated",
      "clock condition HOLDS"
    ]
  };

  function paint(line) {
    if (!line) return "";
    if (line[0] === "#") return '<span class="gap">' + esc(line.slice(2)) + "</span>";
    if (line[0] === "$") return '<span class="cmd">' + esc(line) + "</span>";
    let m = /^\[(\d\d:\d\d:\d\d L=\d+)\] \[(\w+)\] (.*)$/.exec(line);
    if (m) return '<span class="t">' + m[1] + '</span> <span class="who">' + m[2] + "</span> " + esc(m[3]);
    m = /^(L=\d+\s+)(\S+\s+)(\S+)(\s+)(send|recv)(.*)$/.exec(line);
    if (m) return '<span class="lam">' + m[1] + '</span><span class="t">' + m[2] + '</span><span class="who">' + m[3] + "</span>" + m[4] + m[5] + esc(m[6]);
    if (/CONFLICT|LOST UPDATE|overwrote|killed the leader/.test(line)) return '<span class="bad">' + esc(line) + "</span>";
    if (/no conflict|HOLDS|0 violated/.test(line)) return '<span class="ok">' + esc(line) + "</span>";
    return esc(line);
  }
  Object.keys(TRACES).forEach((k) => {
    const pre = document.getElementById("m4trace-" + k);
    if (pre) pre.innerHTML = TRACES[k].map(paint).join("\n");
  });

  // ------------------------------------------------------------------ wiring
  build();
  init();
  ["m4from", "m4to"].forEach((sid, k) => {
    const s = document.getElementById(sid);
    IDS.forEach((id) => s.appendChild(new Option("lot " + id, id)));
    s.value = k === 0 ? "A" : "C";
  });
  document.querySelectorAll("[data-m4]").forEach((b) => {
    b.addEventListener("click", () => {
      const op = b.dataset.m4;
      if (op === "reset") return reset();
      if (op === "send") return run(() => OPS.send(false));
      if (op === "sendfar") return run(() => OPS.send(true));
      run(OPS[op]);
    });
  });
  document.querySelectorAll("#m4speed .chip").forEach((c) => {
    c.addEventListener("click", () => {
      speed = +c.dataset.s;
      document.querySelectorAll("#m4speed .chip").forEach((x) => x.classList.toggle("on", x === c));
    });
  });
  document.querySelectorAll("#m4logtabs button").forEach((b) => {
    b.addEventListener("click", () => {
      document.querySelectorAll("#m4logtabs button").forEach((x) => x.classList.toggle("on", x === b));
      document.querySelectorAll(".m4pane").forEach((p) => p.classList.toggle("on", p.dataset.v === b.dataset.v));
      dirty();
    });
  });
  document.querySelectorAll("#m4tabs button").forEach((b) => {
    b.addEventListener("click", () => {
      document.querySelectorAll("#m4tabs button").forEach((x) => x.classList.toggle("on", x === b));
      document.querySelectorAll("#milestone-4 .vpane4, #milestone-4 .vtext4").forEach((p) => p.classList.toggle("on", p.dataset.v === b.dataset.v));
    });
  });
  window.addEventListener("resize", dirty);
  note("Lots A to D are running and D is the leader. E is a fifth lot that has not been started.");
  render();

  // for the page tests
  window.__m4 = {
    state: () => ({ busy: busy, leader: leaderNow(), lock: LOCK, table: TABLE, version: VERSION, results: RESULTS, verdict: VERDICT,
      nodes: JSON.parse(JSON.stringify(N)), events: EV.length, inflight: inflight }),
    events: () => EV.map((e) => ({ id: e.id, kind: e.kind, L: e.L, text: e.text, link: e.link ? e.link.L : null, fail: !!e.fail })),
    speed: (s) => { speed = s; }
  };
})();
