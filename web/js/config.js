// Shared settings for the browser demo. Same numbers as prototype/common.py.

const CFG = {
  spaces: 20,
  heartbeat: 2,       // sensor heartbeat interval (s)
  delta: 6,           // staleness timeout = 3 heartbeats
  peerInterval: 3,    // lot server summary exchange (s)
  peerStale: 9,       // other lot's summary is stale after 3 missed rounds
  lease: 30,          // reservation lease (s)
  flipRate: 0.12,     // chance per second that a car arrives/leaves in a lot
  timeout: 2,         // client timeout (s)
  retries: 3,
};

const PROCS = {
  sensor_node_A: { file: "workers/sensor_node.js", lot: "A", label: "Sensor node A", role: "pushes events + heartbeats", kind: "sensor" },
  sensor_node_B: { file: "workers/sensor_node.js", lot: "B", label: "Sensor node B", role: "pushes events + heartbeats", kind: "sensor" },
  lot_server_A:  { file: "workers/lot_server.js",  lot: "A", label: "Lot server A", role: "authoritative for Lot A · peer of B", kind: "server" },
  lot_server_B:  { file: "workers/lot_server.js",  lot: "B", label: "Lot server B", role: "authoritative for Lot B · peer of A", kind: "server" },
  client:        { file: "workers/client.js",      lot: null, label: "Client (driver app)", role: "find / reserve / release", kind: "client" },
};

// links between processes that can be cut from the UI
const LINKS = {
  "lot_server_A|sensor_node_A": "Sensor A ↔ Lot A",
  "lot_server_B|sensor_node_B": "Sensor B ↔ Lot B",
  "lot_server_A|lot_server_B": "Lot A ↔ Lot B (peer)",
  "client|lot_server_A": "Client ↔ Lot A",
  "client|lot_server_B": "Client ↔ Lot B",
};

function linkKey(a, b) {
  return [a, b].sort().join("|");
}

function $(sel) {
  return document.querySelector(sel);
}
