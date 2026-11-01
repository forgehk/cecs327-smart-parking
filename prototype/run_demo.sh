#!/usr/bin/env bash
# Starts the whole prototype in the background: registry, broker, 4 lot servers
# (the P2P overlay), 4 sensor nodes. Then leaves you at a prompt to run client commands.
#
#   ./run_demo.sh           # fresh start (deletes old logs)
#   ./run_demo.sh keep      # keep logs -> lot servers RECOVER their state from the log
#   ./run_demo.sh stop      # stop everything (lot servers leave the overlay gracefully)
set -e
cd "$(dirname "$0")"
PY=${PYTHON:-python3}
mkdir -p logs run

stop_all() {
  for f in run/*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
}
if [ "$1" = "stop" ]; then stop_all; sleep 0.5; echo "stopped"; exit 0; fi
stop_all
[ "$1" = "keep" ] || rm -f logs/*.log

start() { # name, cmd...
  local name=$1; shift
  nohup "$PY" -u "$@" > "run/$name.out" 2>&1 &
  echo $! > "run/$name.pid"
  echo "started $name (pid $!)  -> run/$name.out"
}
start registry     registry.py
start broker       broker.py
sleep 0.5
# the overlay: A is the first peer, everyone else joins through one bootstrap peer.
# the mesh that comes out of this: A-B, A-C, A-D, B-C, C-D (degree A=3 B=2 C=3 D=2, diameter 2)
start lot_server_A lot_server.py --lot A
sleep 0.6
start lot_server_B lot_server.py --lot B --bootstrap 127.0.0.1:5001
sleep 0.6
start lot_server_C lot_server.py --lot C --bootstrap 127.0.0.1:5002
sleep 0.6
start lot_server_D lot_server.py --lot D --bootstrap 127.0.0.1:5001
sleep 0.8
start sensor_node_A sensor_node.py --lot A --seed 1
start sensor_node_B sensor_node.py --lot B --seed 2
start sensor_node_C sensor_node.py --lot C --seed 3
start sensor_node_D sensor_node.py --lot D --seed 4
sleep 3
echo
echo "All processes running. Try:"
echo "  $PY client.py find                          # lot A plus what it holds about B, C, D"
echo "  $PY client.py peers                         # lot A's neighbors, replicas, counters"
echo "  $PY client.py --lot C peers"
echo "  $PY client.py lookup D                      # pull: lot A floods a lookup if D is not cached"
echo "  $PY client.py total                         # convergecast: free spaces over all lots"
echo "  $PY client.py --id alice reserve A-03"
echo "  $PY client.py watch A                       # live status events via the broker"
echo "  $PY lot_server.py --lot E --bootstrap 127.0.0.1:5003   # a 5th lot joins through C (Ctrl-C = graceful leave)"
echo "  kill \$(cat run/lot_server_C.pid)           # graceful leave: neighbors drop C"
echo "  kill -9 \$(cat run/lot_server_D.pid)        # crash: neighbors detect the dead pointer within 5 s"
echo "  $PY p2p_metrics.py                          # degree, edges, diameter, lookup hops, traffic"
echo "  tail -f run/lot_server_A.out                # a server's console"
echo "  ./run_demo.sh stop"
