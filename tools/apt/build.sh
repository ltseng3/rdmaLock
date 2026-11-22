#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cmake -S "$ROOT/mucql" -B "$ROOT/mucql/build-apt" \
  -DCMAKE_BUILD_TYPE=Release -DMUCQL_MOCK_VERBS_TEST=OFF
cmake --build "$ROOT/mucql/build-apt" -j "$(nproc)"
for f in mucql_rdma_paxos mucql_verbs_release mucql_lock_e2e mucql_lock_live; do
  if [[ ! -x "$ROOT/mucql/build-apt/$f" ]]; then
    echo "Missing $f: check libibverbs-dev and CMake output" >&2
    exit 1
  fi
done
printf 'Native RDMA binaries built: %s\n' "$ROOT/mucql/build-apt"
