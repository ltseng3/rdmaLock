#!/usr/bin/env bash
# Install only missing CloudLab build and RNIC diagnostic prerequisites.
# Runs during controller bootstrap, BEFORE CMake, outside benchmark timing.
set -euo pipefail

mode=${1:-auto}
if [[ "$mode" != auto && "$mode" != check ]]; then
  echo "Usage: bash tools/apt/ensure_deps.sh [auto|check]" >&2
  exit 2
fi

missing=()
packages=()
require_command() {
  local executable=$1 package=$2
  if ! command -v "$executable" >/dev/null 2>&1; then
    missing+=("$executable")
    packages+=("$package")
  fi
}
require_command cmake cmake
require_command c++ build-essential
require_command make build-essential
require_command git git
require_command python3 python3
require_command pkg-config pkg-config
require_command ibv_devices ibverbs-utils
require_command ibv_devinfo ibverbs-utils
require_command ibstat infiniband-diags
require_command lspci pciutils
require_command ip iproute2

# Catch missing native verbs headers before the CMake build.
if [[ ! -r /usr/include/infiniband/verbs.h ]] && \
   ! { command -v pkg-config >/dev/null 2>&1 && pkg-config --exists libibverbs; }; then
  missing+=("libibverbs development headers")
  packages+=(libibverbs-dev)
fi

if (( ${#missing[@]} == 0 )); then
  echo 'Bootstrap dependencies: OK (nothing to install).'
  exit 0
fi
printf 'Bootstrap missing prerequisites: %s\n' "${missing[*]}"
if [[ "$mode" == check ]]; then
  echo 'Auto-install disabled. Install missing packages or rerun without --no-auto-deps.' >&2
  exit 2
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo 'Cannot auto-install: apt-get unavailable (requires a Debian/Ubuntu CloudLab image).' >&2
  exit 2
fi

if (( EUID != 0 )); then
  if ! command -v sudo >/dev/null 2>&1 || ! sudo -n true >/dev/null 2>&1; then
    echo 'Cannot auto-install: passwordless sudo is unavailable on this node.' >&2
    echo 'Ask CloudLab to enable sudo -n, or install the reported packages yourself.' >&2
    exit 2
  fi
  apt=(sudo -n env DEBIAN_FRONTEND=noninteractive apt-get)
else
  apt=(env DEBIAN_FRONTEND=noninteractive apt-get)
fi
# Drop duplicate packages: multiple missing tools can map to one package.
mapfile -t unique_packages < <(printf '%s\n' "${packages[@]}" | sort -u)
printf 'Installing missing packages: %s\n' "${unique_packages[*]}"
"${apt[@]}" update -qq
"${apt[@]}" install -y -qq --no-install-recommends "${unique_packages[@]}"

# Fail before building if the package manager did not provide all tools.
exec bash "$0" check
