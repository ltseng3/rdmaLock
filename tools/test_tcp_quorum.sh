#!/usr/bin/env bash
# Three local independent acceptor processes; protocol regression smoke test.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${MUCQL_BUILD_DIR:-$ROOT/mucql/build}"
BASE="$((20000 + ($$ % 10000)))"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT
for i in 0 1 2; do
  "$BIN/mucql_acceptor" "$((BASE+i))" >/dev/null 2>&1 &
  PIDS+=("$!")
done
sleep 0.15
HOSTS=("127.0.0.1:$BASE" "127.0.0.1:$((BASE+1))" "127.0.0.1:$((BASE+2))")
"$BIN/mucql_paxos_demo" 100 32 "${HOSTS[@]}"
"$BIN/mucql_paxos_demo" 200 32 "${HOSTS[@]}"
kill "${PIDS[2]}"
"$BIN/mucql_paxos_demo" 300 16 "${HOSTS[@]}"
echo 'PASS: three TCP acceptors, takeover, one crashed replica'
