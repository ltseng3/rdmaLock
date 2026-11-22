#!/usr/bin/env bash
# Local hardware preflight, read-only. Defaults to mlx4_0 port 1.
set -euo pipefail
D=${1:-mlx4_0}
P=${2:-1}
printf '\nHost: %s | Device: %s | Port: %s\n' "$(hostname)" "$D" "$P"
printf '\n-- PCI devices --\n'
lspci | grep -Ei 'Mellanox|InfiniBand' || true
printf '\n-- IB devices --\n'
ibv_devices
printf '\n-- IB port --\n'
ibv_devinfo -d "$D" -i "$P"
printf '\n-- IB link --\n'
ibstat "$D" || true
printf '\n-- IP interfaces --\n'
ip -brief address
printf '\n-- memory lock limit --\n'
ulimit -l
printf '\n-- clock sync status (informational; not a formal safety bound) --\n'
command -v chronyc >/dev/null && chronyc tracking || true
if ! ibv_devinfo -d "$D" -i "$P" | grep -q 'PORT_ACTIVE'; then
  echo 'ERROR: selected port is not active InfiniBand. Fix Apt flexible fabric/profile first.' >&2
  exit 1
fi
echo 'Preflight: selected IB port reports ACTIVE.'
