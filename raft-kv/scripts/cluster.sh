#!/usr/bin/env bash
# Start/stop a local 3-replica cluster: scripts/cluster.sh start|stop|restart <id>
set -euo pipefail
cd "$(dirname "$0")/.."
PEERS=http://127.0.0.1:7000,http://127.0.0.1:7001,http://127.0.0.1:7002
mkdir -p data
start_one() { ./bin/kvserver -id "$1" -peers "$PEERS" -data ./data -store "${STORE:-memory}" >"data/node-$1.log" 2>&1 & echo $! >"data/node-$1.pid"; }
stop_one()  { [ -f "data/node-$1.pid" ] && kill "$(cat "data/node-$1.pid")" 2>/dev/null || true; rm -f "data/node-$1.pid"; }
case "${1:-}" in
  start)   for i in 0 1 2; do start_one $i; done; sleep 1; echo "cluster up: $PEERS" ;;
  stop)    for i in 0 1 2; do stop_one $i; done ;;
  kill)    stop_one "$2" ;;
  restart) stop_one "$2"; start_one "$2" ;;
  *) echo "usage: $0 start|stop|kill <id>|restart <id>"; exit 2 ;;
esac
