#!/usr/bin/env bash
# Run on the client node, after servers were started on three other nodes.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CONF=${1:-./hosts.env}
if [[ ! -f $CONF ]]; then echo "missing $CONF" >&2;exit 2;fi
# shellcheck source=/dev/null
source "$CONF"
: "${R1:?}" "${R2:?}" "${R3:?}" "${IB_DEV:?}"
export MUCQL_IB_PORT=${IB_PORT:-1}
N=${2:-1000}
BALLOT=${3:-1001}
if ((N<1 || N>4096)); then echo 'N must be in 1..4096 for default accepted region' >&2;exit 2;fi
"$ROOT/mucql/build-apt/mucql_rdma_paxos" client "$BALLOT" "$N" \
  "$R1:${CONTROL_PORT:-29660}" "$R2:${CONTROL_PORT:-29660}" \
  "$R3:${CONTROL_PORT:-29660}" "$IB_DEV"
