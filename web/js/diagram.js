// Draws the architecture diagram as SVG. Nodes are clickable (crash / restart),
// links are clickable (cut / restore), and a dot travels along an edge whenever
// a message goes between two processes.

const NODES = {
  sensor_node_A: { x: 20,  y: 18,  w: 178, h: 60 },
  sensor_node_B: { x: 362, y: 18,  w: 178, h: 60 },
  lot_server_A:  { x: 20,  y: 128, w: 178, h: 76 },
  lot_server_B:  { x: 362, y: 128, w: 178, h: 76 },
  client:        { x: 191, y: 266, w: 178, h: 60 },
};

// key -> [point near first process, point near second, label, label x, label y, text-anchor]
const EDGES = {
  "lot_server_A|sensor_node_A": [[109, 78], [109, 128], "events + heartbeat", 116, 106, "start"],
  "lot_server_B|sensor_node_B": [[451, 78], [451, 128], "events + heartbeat", 444, 106, "end"],
  "lot_server_A|lot_server_B":  [[198, 152], [362, 152], "lot summaries (peers)", 280, 145, "middle"],
  "client|lot_server_A":        [[215, 266], [122, 204], "find / reserve / release", 60, 250, "start"],
  "client|lot_server_B":        [[345, 266], [438, 204], "request ↔ reply", 500, 250, "end"],
};

function drawDiagram() {
  const svg = $("#diagram");
  let h = "";

  for (const k in EDGES) {
    const e = EDGES[k];
    const coords = 'x1="' + e[0][0] + '" y1="' + e[0][1] + '" x2="' + e[1][0] + '" y2="' + e[1][1] + '"';
    h += '<line class="edge-hit" data-l="' + k + '" ' + coords + '><title>' + LINKS[k] + ': click to cut / restore</title></line>';
    h += '<line id="edge-' + k + '" class="edge" ' + coords + "/>";
    h += '<text class="elabel" x="' + e[3] + '" y="' + e[4] + '" text-anchor="' + e[5] + '">' + e[2] + "</text>";
  }

  for (const n in PROCS) {
    const p = PROCS[n];
    const g = NODES[n];
    const roles = p.role.split(" · ");
    h += '<g id="node-' + n + '" class="node" data-p="' + n + '">';
    h += '<rect x="' + g.x + '" y="' + g.y + '" width="' + g.w + '" height="' + g.h + '"/>';
    h += '<text x="' + (g.x + 10) + '" y="' + (g.y + 19) + '">' + p.label + "</text>";
    roles.forEach(function (r, i) {
      h += '<text class="role" x="' + (g.x + 10) + '" y="' + (g.y + 33 + i * 12) + '">' + r + "</text>";
    });
    h += '<text class="st" x="' + (g.x + 10) + '" y="' + (g.y + g.h - 8) + '">● running</text>';
    h += '<text class="hint" x="' + (g.x + g.w - 10) + '" y="' + (g.y + g.h - 8) + '" text-anchor="end">click to crash</text>';
    h += "</g>";
  }

  h += '<g class="disk"><rect x="30" y="212" width="158" height="16"/><text x="38" y="223">log A on disk (append-only)</text></g>';
  h += '<g class="disk"><rect x="372" y="212" width="158" height="16"/><text x="380" y="223">log B on disk (append-only)</text></g>';
  h += '<text class="elabel" x="280" y="340" text-anchor="middle">federated: each lot server owns its lot, no global coordinator</text>';

  svg.innerHTML = h;

  svg.querySelectorAll(".node").forEach(function (g) {
    g.onclick = function () { toggleProcess(g.dataset.p); };
  });
  svg.querySelectorAll(".edge-hit").forEach(function (l) {
    const edge = document.getElementById("edge-" + l.dataset.l);
    l.onmouseenter = function () { edge.classList.add("hover"); };
    l.onmouseleave = function () { edge.classList.remove("hover"); };
    l.onclick = function () {
      const up = !linkUp[l.dataset.l];
      setLink(l.dataset.l, up);
      toast(LINKS[l.dataset.l] + (up ? " restored" : " cut"), up ? "ok" : "err");
    };
  });
}

// which end of the edge is closer to the sending process?
function edgeEndpoints(from, to) {
  const e = EDGES[linkKey(from, to)];
  if (!e) return null;
  const nf = NODES[from];
  const cx = nf.x + nf.w / 2;
  const cy = nf.y + nf.h / 2;
  const dA = Math.hypot(e[0][0] - cx, e[0][1] - cy);
  const dB = Math.hypot(e[1][0] - cx, e[1][1] - cy);
  return dA < dB ? [e[0], e[1]] : [e[1], e[0]];
}

function animateDot(from, to, kind) {
  const ends = edgeEndpoints(from, to);
  if (!ends) return;
  const s = ends[0];
  const d = ends[1];

  const svg = $("#diagram");
  const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  c.setAttribute("r", 3.5);
  c.setAttribute("class", "dot " + kind);
  svg.appendChild(c);

  const edge = document.getElementById("edge-" + linkKey(from, to));
  edge.classList.add("flash");
  setTimeout(function () { edge.classList.remove("flash"); }, 250);

  // a dropped message only makes it part of the way
  const dur = kind === "dropped" ? 260 : Math.max(220, Math.min(900, latency));
  const t0 = performance.now();
  function step(t) {
    const k = Math.min(1, (t - t0) / dur);
    const kk = kind === "dropped" ? k * 0.45 : k;
    c.setAttribute("cx", s[0] + (d[0] - s[0]) * kk);
    c.setAttribute("cy", s[1] + (d[1] - s[1]) * kk);
    if (k < 1) requestAnimationFrame(step);
    else c.remove();
  }
  requestAnimationFrame(step);
}
