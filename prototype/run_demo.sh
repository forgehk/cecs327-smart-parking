#!/usr/bin/env bash
# Starts the whole prototype (2 sensor nodes, 2 lot servers) in the background,
# then leaves you at a prompt to run client commands. Ctrl-C / `./run_demo.sh stop` to stop.
#
#   ./run_demo.sh           # fresh start (deletes old logs)
#   ./run_demo.sh keep      # keep logs -> lot servers RECOVER their state from the log
#   ./run_demo.sh stop      # stop everything
set -e
cd "$(dirname "$0")"
PY=${PYTHON:-python3}
mkdir -p logs run

stop_all() {
  for f in run/*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
}
if [ "$1" = "stop" ]; then stop_all; echo "stopped"; exit 0; fi
stop_all
[ "$1" = "keep" ] || rm -f logs/*.log

start() { # name, cmd...
  local name=$1; shift
  nohup "$PY" -u "$@" > "run/$name.out" 2>&1 &
  echo $! > "run/$name.pid"
  echo "started $name (pid $!)  -> run/$name.out"
}
start lot_server_A lot_server.py --lot A
start lot_server_B lot_server.py --lot B
sleep 0.5
start sensor_node_A sensor_node.py --lot A --seed 1
start sensor_node_B sensor_node.py --lot B --seed 2
sleep 2.5
echo
echo "All processes running. Try:"
echo "  $PY client.py find"
echo "  $PY client.py --id alice reserve A-03"
echo "  $PY client.py --id bob   reserve A-03      # DENIED reserved"
echo "  $PY client.py --id alice release A-03"
echo "  kill \$(cat run/sensor_node_A.pid)          # spaces in Lot A -> unknown after 6 s"
echo "  kill \$(cat run/lot_server_A.pid); $PY lot_server.py --lot A   # crash + recovery from log"
echo "  tail -f run/lot_server_A.out                # watch a server's console"
echo "  tail -f logs/lot_server_A.log               # the JSON-lines message log"
echo "  ./run_demo.sh stop"
