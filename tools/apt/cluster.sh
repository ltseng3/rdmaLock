#!/usr/bin/env bash
# Deploy/build and run repeatable Apt transport smoke tests over SSH.
# Run on your laptop/control machine; no GitHub or SSH private key on Apt nodes.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [[ $# -lt 1 ]]; then echo "Usage: $0 sync|check|ordering|release|all|stop [trials=3] [hosts.env]" >&2; exit 2; fi
ACTION="$1"; TRIALS="${2:-3}"; CONF="${3:-$ROOT/hosts.env}"
if [[ ! -f "$CONF" ]]; then echo "Missing $CONF; copy tools/apt/hosts.example.env to hosts.env and edit it" >&2; exit 2; fi
# shellcheck disable=SC1090
source "$CONF"
: "${R1:?}" "${R2:?}" "${R3:?}" "${R1_SSH:?}" "${R2_SSH:?}" "${R3_SSH:?}" "${CLIENT_SSH:?}"
IB_DEV="${IB_DEV:-mlx4_0}"; IB_PORT="${IB_PORT:-1}"
CONTROL_PORT="${CONTROL_PORT:-29660}"; RELEASE_PORT="${RELEASE_PORT:-29670}"
REMOTE_DIR="${REMOTE_DIR:-rdma-queue}"
GITHUB_REPO="${GITHUB_REPO:-}"
if [[ ! "$REMOTE_DIR" =~ ^[a-zA-Z0-9_.-]+$ ]]; then echo "REMOTE_DIR must be a simple directory name" >&2; exit 2; fi
if [[ ! "$TRIALS" =~ ^[0-9]+$ ]] || (( TRIALS<1 || TRIALS>100 )); then echo "trials must be 1..100" >&2; exit 2; fi
for x in "$R1" "$R2" "$R3"; do
  if [[ ! "$x" =~ ^[a-zA-Z0-9_.-]+$ ]]; then echo 'IP/host must not contain spaces or shell metacharacters' >&2; exit 2; fi
done
SSH_FLAGS=(-o BatchMode=yes -o ConnectTimeout=10)
remote() { local h="$1"; shift; ssh "${SSH_FLAGS[@]}" "$h" "$@"; }
replicas=("$R1_SSH" "$R2_SSH" "$R3_SSH")
nodes=("$R1_SSH" "$R2_SSH" "$R3_SSH" "$CLIENT_SSH")
server() { local host="$1" kind="$2" action="$3"; remote "$host" "cd '$REMOTE_DIR' && IB_DEV='$IB_DEV' IB_PORT='$IB_PORT' CONTROL_PORT='$CONTROL_PORT' RELEASE_PORT='$RELEASE_PORT' bash tools/apt/server.sh '$kind' '$action'"; }
stop_servers() {
  for h in "${replicas[@]}"; do server "$h" ordering stop || true; done
  server "${replicas[0]}" release stop || true
}
local_output="$ROOT/results/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$local_output"
case "$ACTION" in
  sync)
    if [[ ! "$GITHUB_REPO" =~ ^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+$ ]]; then echo "Set GITHUB_REPO=OWNER/REPO in hosts.env" >&2; exit 2; fi
    for h in "${nodes[@]}"; do
      echo "=== sync + compile: $h ==="
      remote "$h" "set -e; if [ -d '$REMOTE_DIR/.git' ]; then git -C '$REMOTE_DIR' pull --ff-only; else git clone 'https://github.com/$GITHUB_REPO.git' '$REMOTE_DIR'; fi; cd '$REMOTE_DIR'; bash tools/apt/build.sh"
    done
    echo "Done. All four nodes updated from public GitHub and compiled.";;
  check)
    for h in "${nodes[@]}"; do
      echo "=== RDMA preflight $h ==="
      remote "$h" "cd '$REMOTE_DIR' && bash tools/apt/check.sh '$IB_DEV' '$IB_PORT' && test -x mucql/build-apt/mucql_rdma_paxos && test -x mucql/build-apt/mucql_verbs_release"
    done;;
  ordering|all)
    for ((trial=1;trial<=TRIALS;trial++)); do
      echo "=== ordering trial $trial/$TRIALS ==="
      for h in "${replicas[@]}"; do server "$h" ordering restart; done
      LOG="$local_output/ordering_${trial}.log"
      remote "$CLIENT_SSH" "cd '$REMOTE_DIR' && R1='$R1' R2='$R2' R3='$R3' IB_DEV='$IB_DEV' IB_PORT='$IB_PORT' CONTROL_PORT='$CONTROL_PORT' COUNT=1000 BALLOT='$((1000+trial))' bash tools/apt/bench.sh ordering" | tee "$LOG"
      python3 "$ROOT/tools/apt/record_run.py" ordering "$LOG" --csv "$local_output/summary.csv"
    done
    for h in "${replicas[@]}"; do server "$h" ordering stop; done
    if [[ "$ACTION" == ordering ]]; then echo "CSV: $local_output/summary.csv";
      python3 "$ROOT/tools/apt/summarize.py" "$local_output/summary.csv"; exit 0; fi
    ;;&
  release|all)
    for ((trial=1;trial<=TRIALS;trial++)); do
      echo "=== release CAS trial $trial/$TRIALS ==="
      server "${replicas[0]}" release restart
      LOG="$local_output/release_${trial}.log"
      remote "$CLIENT_SSH" "cd '$REMOTE_DIR' && RELEASE_SERVER_IP='$R1' IB_DEV='$IB_DEV' IB_PORT='$IB_PORT' RELEASE_PORT='$RELEASE_PORT' COUNT=100000 MODE=cas bash tools/apt/bench.sh release" | tee "$LOG"
      python3 "$ROOT/tools/apt/record_run.py" release "$LOG" --csv "$local_output/summary.csv"
    done
    server "${replicas[0]}" release stop
    echo "CSV: $local_output/summary.csv"
    python3 "$ROOT/tools/apt/summarize.py" "$local_output/summary.csv";;
  stop) stop_servers;;
  *) echo "Usage: $0 sync|check|ordering|release|all|stop [trials=3] [hosts.env]" >&2; exit 2;;
esac
