# Direct-handoff lock test on Apt (dashboard terminals)

This exercises a **finite, precommitted, exclusive-lock FIFO batch** with native
one-sided RDMA for the protected data WRITE, direct handoff, and release marker.
The control plane orders JOIN/PREPARE records using a three-replica RDMA quorum.
Clients do **not** ask the coordinator to transfer ownership on the normal
handoff path.

**Important limits:** This is a batch-preparation benchmark, not a dynamic
closed-loop workload or a drop-in DecLock backend. The lease-expiry recovery
path is disabled: no trustworthy cross-host clock bounds have been established.
A crashed holder can therefore stop progress (fail closed). RNIC fencing and
reconfiguration are not exercised. Do not interpret a passing run as a proof of
failover behavior.

## Four-dashboard-terminal shortcut

Only one dashboard terminal per node is required if the acceptors/release
region run in the background. After the one-time build, run the following on
nodes **1, 2, and 3** respectively:

```bash
cd ~/rdmaLock
nohup ./mucql/build-apt/mucql_rdma_paxos server 29660 mlx4_0 \
    > paxos_acceptor.log 2>&1 & echo $! > paxos_acceptor.pid
```

On **node 4**, start the release region in the background:

```bash
cd ~/rdmaLock
nohup ./mucql/build-apt/mucql_verbs_release server 29670 mlx4_0 4096 \
    > release_region.log 2>&1 & echo $! > release_region.pid
```

Then execute the coordinator command from step 3 on node 4, and the two
worker commands from step 4 on nodes 1 and 2. Node 3 only hosts an acceptor.
Check `paxos_acceptor.log` or `release_region.log` if a server fails to start.
Restart these background processes for every new trial with:

```bash
# Nodes 1, 2, 3:
kill "$(cat paxos_acceptor.pid)"
# Node 4:
kill "$(cat release_region.pid)"
```

Reissue the corresponding `nohup` startup commands after stopping them.
The detailed steps below also work if you prefer separate foreground terminals.

## 0. One-time build on all four nodes

In your CloudLab dashboard terminal on **each** node:

```bash
cd ~/rdmaLock
git pull --ff-only
bash tools/apt/build.sh
ls -l mucql/build-apt/mucql_lock_e2e
```

The code above must already be committed to your public GitHub repository.
You can also upload the provided ZIP to a checkout if you haven't pushed yet.
As with the previous transport tests, use the **active** `mlx4_0` device; set
`MUCQL_IB_PORT=2` in each terminal if port 2 is the active InfiniBand port.

Determine the private experiment-network IP on **node 4**:

```bash
ip -br -4 addr
```

Use its actual experiment-network IP in place of `NODE4_IP` below. The
example node 1/2/3 addresses are the ones from your earlier working test:
`10.10.1.1`, `10.10.1.2`, `10.10.1.3`. Recheck them if the experiment was
re-provisioned.

## 1. Start three acceptors (nodes 1, 2, 3)

Stop any old `mucql_rdma_paxos` process with Ctrl+C; these acceptors do not
recycle the journal or enable leader takeover. On **each** of nodes 1, 2, 3:

```bash
cd ~/rdmaLock
./mucql/build-apt/mucql_rdma_paxos server 29660 mlx4_0
```

Leave them running. You can open another CloudLab dashboard shell on node 1
and node 2 for the worker process in step 4.

## 2. Start shared RDMA memory region (node 4, terminal A)

```bash
cd ~/rdmaLock
./mucql/build-apt/mucql_verbs_release server 29670 mlx4_0 4096
```

Leave it running. This region contains one write-once CAS release marker per
position, plus a protected 8-byte data slot and an optional owner-check slot.
Restart it before each independent benchmark to clear all write-once markers.

**Strongly recommended preflight (run once on node 1, after node 4's
release server starts):**

```bash
cd ~/rdmaLock
NODE4_IP="REPLACE_WITH_NODE4_PRIVATE_IP"
./mucql/build-apt/mucql_verbs_release check "$NODE4_IP" 29670 mlx4_0
```

Expect `RDMA release check CAS_nonzero=PASS READ_WRITE=PASS`. This checks the
nonzero CAS return value and RDMA READ permissions that the earlier microbench
(with only initially zero CAS slots) did not exercise. A failure means **do
not run the lock benchmark** until corrected.

## 3. Start the coordinator (node 4, terminal B)

First replace `REPLACE_WITH_NODE4_PRIVATE_IP` with the actual private IP
shown by `ip -br -4 addr` on node 4. Do not include angle brackets or a
placeholder prefix in the final command:

```bash
cd ~/rdmaLock
NODE4_IP="REPLACE_WITH_NODE4_PRIVATE_IP"
./mucql/build-apt/mucql_lock_e2e coordinator \
    29700 3001 1000 2 \
    "$NODE4_IP:29670" \
    10.10.1.1:29660 10.10.1.2:29660 10.10.1.3:29660 \
    mlx4_0 1 0 2>&1 | tee lock_e2e_probe_run1.log
```

Argument meanings: `29700` coordinator TCP port, `3001` unique ballot,
`1000` FIFO positions, `2` workers, `1` activates **owner-CAS verification**,
`0` adds no synthetic work. The coordinator waits for two worker connections.

## 4. Start two workers (node 1 and node 2, separate dashboard shells)

**Node 1 worker**:

```bash
cd ~/rdmaLock
NODE4_IP="REPLACE_WITH_NODE4_PRIVATE_IP"
./mucql/build-apt/mucql_lock_e2e worker \
    "$NODE4_IP:29700" 0 10.10.1.1 29711 mlx4_0
```

**Node 2 worker**:

```bash
cd ~/rdmaLock
NODE4_IP="REPLACE_WITH_NODE4_PRIVATE_IP"
./mucql/build-apt/mucql_lock_e2e worker \
    "$NODE4_IP:29700" 1 10.10.1.2 29712 mlx4_0
```

The coordinator commits all positions before issuing `GO`. Clients then take
turns via **direct peer-to-peer RDMA CAS**, with the receiver polling its local
registered memory. The previous holder has already retired and completed its
protected RDMA data access. A separate CAS release marker is posted without
waiting for the ordering leader's CPU.

## 5. Interpret the results

Look for a line of this form in **node 4's coordinator terminal**:

```text
LOCK_E2E positions=1000 workers=2 completed=1000 violations=0 verified_markers=1000 ordered_entries=1001 probe=1 work_ns=0 ordering_seconds=... handoff_seconds=... goodput_ops_per_sec=... including_ordering_ops_per_sec=... status=PASS
```

The numbers represented by `...` are measured on your nodes; none are
pre-filled. The coordinator refuses to print `status=PASS` if any committed
release marker is missing, the final protected data value is wrong, the
instrumented owner slot is nonzero, or completions are missing.

- `goodput_ops_per_sec` measures the **GO-to-DONE** execution window after
  batch ordering and preparation (includes worker completion TCP notifications).
- `including_ordering_ops_per_sec` also accounts for time spent committing
  the finite ordered batch. It excludes process startup/QP establishment.
- `ordering_seconds` measures queue ordering and preparation separately.
- `probe=1` adds **two extra RDMA CAS operations per critical section** to
  catch conflicting owners. It is a safety instrumentation run, not the
  fastest execution path.

## 6. Measure the direct-handoff fast path

**Restart all three Paxos acceptors, the release-region server, and the
coordinator/workers** so the log and marker memory are empty. Repeat steps
1–4 with `3002` as the ballot and `0` instead of `1` after `mlx4_0`:

```bash
# Node 4 coordinator command, after restarting services:
NODE4_IP="REPLACE_WITH_NODE4_PRIVATE_IP"
./mucql/build-apt/mucql_lock_e2e coordinator \
    29700 3002 1000 2 \
    "$NODE4_IP:29670" \
    10.10.1.1:29660 10.10.1.2:29660 10.10.1.3:29660 \
    mlx4_0 0 0 2>&1 | tee lock_e2e_fast_run2.log
```

Launch the same workers (step 4) after the coordinator starts. Try a third
run with `3003` and `work_ns=1000` (last CLI argument) to emulate a one
microsecond local critical section; optionally increase positions to 2000.

**Important:** Do not compare `probe=1` and `probe=0` as if they perform the
same amount of work. The fastest run still includes one completed protected
RDMA WRITE per position, a release-marker CAS, and a peer handoff CAS.

## 7. Export machine-readable numbers

From the coordinator node:

```bash
python3 tools/apt/e2e_results.py lock_e2e_*.log > lock_results.csv
cat lock_results.csv
```

It rejects incomplete or failed runs. Keep both the raw logs and CSV.

## Troubleshooting

- **TCP connect failure:** confirm listener and port with `ss -lntp`, and
  `nc -vz <host> <port>` from the relevant node. Ports: replicas `29660`,
  release region `29670`, coordinator `29700`, worker 0 `29711`, worker 1 `29712`.
- **Phase-1 quorum unavailable:** restart all three acceptors to clear the
  previous non-recyclable log/ballot, then rerun the entire trial.
- **Release marker mismatch / stale handoff:** restart every process from
  step 1; an independent run requires empty marker memory.
- **Worker times out waiting for handoff:** capture both worker logs. It is
  intentionally fail-closed—there is no unsafe timeout-stealing mechanism.
- **Only one worker reports PASS:** the coordinator does not report PASS until
  all workers finish and the release markers/data are independently checked.

### Experimental scope

The four-node setup shares RNICs between acceptors and workers on nodes 1 and
2, which can introduce hardware contention. It is useful for functional and
preliminary performance validation, not a controlled comparison with a
separate multi-node lock baseline. The client fast path does not involve the
leader CPU, but **all ticket commits and batch preparations precede GO** in
this version. Steady-state dynamic queue replenishment, safe lease expiry,
client/leader failover, and a native DecLock `SYSTEM=mucql` backend are still
separate work items.
