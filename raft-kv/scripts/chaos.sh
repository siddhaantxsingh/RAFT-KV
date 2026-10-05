#!/usr/bin/env bash
# Real-process chaos test: 3 kvserver processes, concurrent clients on
# shared keys, SIGKILL (random and leader-targeted) + restart and
# SIGSTOP/SIGCONT pauses; then a linearizability check of every observed
# operation and a replica convergence check. See cmd/kvchaos.
#   DURATION=60s SEED=42 scripts/chaos.sh
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/ ./cmd/...
D="${DURATION:-30s}"; [[ "$D" =~ ^[0-9]+$ ]] && D="${D}s"   # accept bare seconds
exec ./bin/kvchaos -duration "$D" ${SEED:+-seed "$SEED"} -json data/chaos-summary.json
