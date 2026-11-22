#!/usr/bin/env bash
# Run on each reserved CloudLab Apt node. Uses distro mlx4 / rdma-core stack.
set -euo pipefail
if [[ $EUID -ne 0 ]]; then SUDO=sudo; else SUDO=; fi
$SUDO apt-get update
$SUDO env DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential cmake pkg-config git unzip python3 \
  rdma-core libibverbs-dev ibverbs-utils infiniband-diags perftest pciutils \
  libnuma-dev numactl
$SUDO modprobe mlx4_core || true
$SUDO modprobe mlx4_ib || true
$SUDO modprobe ib_uverbs || true
printf '\n-- RDMA devices --\n'
ibv_devices || true
printf '\n-- RDMA ports --\n'
ibstat || true
printf '\nMake sure the flexible fabric is configured for InfiniBand,\n'
printf 'and select an ACTIVE mlx4 port on all nodes.\n'
