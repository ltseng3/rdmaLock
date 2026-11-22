# Dedicated memory nodes and scalable client nodes (Apt)

This dashboard-driven runner uses **zero GitHub credentials on nodes**, **zero
SSH between nodes**, and **no client processes on any memory node**. It is
compatible with ConnectX-3 `mlx4_0` on the native IB fabric.

## Nodes

- `mn1`, `mn2`, `mn3`: three RDMA quorum acceptors, one per machine.
- `mn4`: release/data memory region and one ordering coordinator.
- `cn1`, `cn2`, ...: **separate physical machines** running configurable lock
  worker processes. Do not colocate them with MNs.

**Minimum:** 6 physical nodes (4 MNs, 2 CNs). The 4-node validation layout
used earlier *colocated* client and memory services and is NOT the preferred
performance topology. More CNs can be added by modifying one JSON file.

## Configure once

1. Reserve dedicated CN nodes on the same Apt native InfiniBand fabric.
2. Edit `tools/apt/topology.json` **in GitHub's web editor**. Change `cn1` and
   `cn2` IPs to the *actual* experiment-network IPs. `10.10.1.5` and `.6` are
   EXAMPLES, not discovered addresses. Add other CNs under `clients`, each
   with an `id`, `ip`, `workers`, and physical `cores` count.
3. On every node, use the CloudLab dashboard to run:

```bash
cd ~/rdmaLock
git pull --ff-only
bash tools/apt/build.sh
python3 tools/apt/isolated_cluster.py show
```

The configuration refuses duplicate physical IPs and refuses worker counts
larger than the configured available cores. There can be up to **64 worker
processes** across CN nodes. Cores specified in the JSON are a guard against
oversubscription, not a CPU-pinning implementation; record actual CPU affinity.

## Run a trial (one dashboard terminal on each role)

First, **stop all old server processes** and restart all four MN servers for
**every independent trial**: the journal and release slots are write-once.
Do not run an earlier trial on the same server state. Start:

On MN1, MN2, MN3, one command each:

```bash
python3 tools/apt/isolated_cluster.py run mn1   # MN1 terminal
python3 tools/apt/isolated_cluster.py run mn2   # MN2 terminal
python3 tools/apt/isolated_cluster.py run mn3   # MN3 terminal
```

On MN4 memory server terminal:

```bash
python3 tools/apt/isolated_cluster.py run mn4
```

On MN1 (another terminal), run the hardware atomic/READ preflight once after
MN4 starts:

```bash
./mucql/build-apt/mucql_verbs_release check 10.10.1.4 29670 mlx4_0
```

On MN4, **another terminal**, start the coordinator *before* the CN workers:

```bash
python3 tools/apt/isolated_cluster.py run coordinator \
    --ballot 4101 --positions 2048 --batch 16 --probe 1
```

Each CN machine, in its own dashboard terminal (all CNs should use the same
`tools/apt/topology.json` pulled from GitHub):

```bash
python3 tools/apt/isolated_cluster.py run cn1 --log-dir results/cn1_b4101
python3 tools/apt/isolated_cluster.py run cn2 --log-dir results/cn2_b4101
```

The launcher assigns globally unique worker indices and distinct TCP listener
ports automatically. It validates that the local machine owns the configured
private IP, preventing accidental execution on an MN. It saves a log for each
local worker and fails if any worker does not exit successfully. To inspect
commands without starting anything, use `print` instead of `run`.

Look for `LOCK_E2E ... status=PASS` on MN4, with all positions completed,
`violations=0` and all release markers verified. Then restart MN servers and
rerun with **a new ballot** and `--probe 0`. Test `--batch 1,8,16,32` to compare
phase-2 pipelining. Keep all other parameters identical within a sweep.

For an ordering-only preflight (not client-generated request load), run on MN4
with acceptors restarted and the memory server not needed:

```bash
python3 tools/apt/isolated_cluster.py run ordering --ballot 5101 \
    --positions 2048 --batch 16
```

## Performance interpretation

**Existing Stage 4 capability:** phase-2 RDMA batching (up to 32 contiguous
entries), more lock workers on dedicated CN nodes, and independent handoff
verification. This test still precommits a finite FIFO batch before the GO
signal. `goodput_ops_per_sec` excludes ordering and preparation; the
`including_ordering_ops_per_sec` number includes serial admission followed by
handoff, but is not a continuous mixed-load result.

**Not yet implemented:** CN-generated continuous JOIN traffic, dynamic
replenishment of PREPARE ranges, journal/lease-safe reclamation, verified
leader-failover fences, and a common duration-based workload interface with
DecLock. Until those exist, use these results to check the transport and batch
mechanism, not as a fair sustained cross-system throughput comparison.
