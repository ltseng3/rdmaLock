#!/usr/bin/env bash
# Saves explicit host private addresses; do not automatically use SSH/public IPs.
set -euo pipefail
if [[ $# -ne 4 ]]; then
  echo 'Usage: make_host_file.sh <replica1-private-ip> <replica2-private-ip> <replica3-private-ip> <client-private-ip>' >&2
  exit 2
fi
cat > hosts.env <<ENV
# Connections must be over reserved Apt experiment/IB network, NOT public/control network.
R1=$1
R2=$2
R3=$3
CLIENT=$4
IB_DEV=mlx4_0
IB_PORT=1
CONTROL_PORT=29660
RELEASE_PORT=29670
ENV
printf 'Saved hosts.env; inspect and source it only after checking interfaces.\n'
