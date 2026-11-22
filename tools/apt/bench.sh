#!/usr/bin/env bash
# Run on the client node: one independent ordering or release transport trial.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KIND="${1:-}"
DEV="${IB_DEV:-mlx4_0}"
export MUCQL_IB_PORT="${IB_PORT:-1}"
ORDER_PORT="${CONTROL_PORT:-29660}"
RELEASE_PORT="${RELEASE_PORT:-29670}"
N="${COUNT:-1000}"
BALLOT="${BALLOT:-1001}"
mkdir -p "$ROOT/results"
TS="$(date -u +%Y%m%dT%H%M%S%NZ)"
case "$KIND" in
  ordering)
    : "${R1:?Set R1 experiment-network IP}" "${R2:?Set R2 experiment-network IP}" "${R3:?Set R3 experiment-network IP}"
    if ((N < 1 || N > 4096)); then echo 'COUNT must be 1..4096' >&2; exit 2; fi
    LOG="$ROOT/results/${TS}_ordering.log"
    "$ROOT/mucql/build-apt/mucql_rdma_paxos" client "$BALLOT" "$N" \
      "$R1:$ORDER_PORT" "$R2:$ORDER_PORT" "$R3:$ORDER_PORT" "$DEV" 2>&1 | tee "$LOG"
    python3 "$ROOT/tools/apt/record_run.py" ordering "$LOG" --csv "$ROOT/results/smoke.csv"
    ;;
  release)
    : "${RELEASE_SERVER_IP:?Set RELEASE_SERVER_IP to experiment-network address}"
    local_count="${COUNT:-100000}"
    mode="${MODE:-cas}"
    if [[ "$mode" != cas && "$mode" != write ]]; then echo 'MODE must be cas or write' >&2; exit 2; fi
    if ((local_count < 1 || local_count > 200000)); then echo 'COUNT must be 1..200000' >&2; exit 2; fi
    LOG="$ROOT/results/${TS}_release_${mode}.log"
    "$ROOT/mucql/build-apt/mucql_verbs_release" client \
      "$RELEASE_SERVER_IP" "$RELEASE_PORT" "$local_count" "$DEV" "$mode" 2>&1 | tee "$LOG"
    python3 "$ROOT/tools/apt/record_run.py" release "$LOG" --csv "$ROOT/results/smoke.csv"
    ;;
  *) echo 'Usage: tools/apt/bench.sh ordering|release' >&2; exit 2;;
esac
