# RDMA queue and transport tools

C++17 utilities for ordered queue entries, one-sided RDMA transport, and repeatable throughput smoke tests.

## Dedicated CN nodes and batched ordering

The Apt launcher accepts a JSON topology with separate replica, memory, and client nodes, and can start multiple independent worker processes on each CN. The ordering fast path supports up to 32 in-flight contiguous log entries per replica. [Dashboard commands](tools/apt/ISOLATED.md). The current lock test remains finite-batch; continuous request admission is not yet implemented.

## End-to-end exclusive locking

The `mucql_lock_e2e` binary exercises a precommitted FIFO of exclusive lock
positions on CloudLab Apt using three RDMA ordering acceptors, peer-to-peer
one-sided handoff CAS, one-sided release markers, and an RDMA WRITE to shared
protected data under the lock. An optional owner-CAS probe detects overlapping
critical sections. The test is fail-closed and does not enable clock-based
lease recovery.

**Dashboard instructions:** [End-to-end locking on Apt](tools/apt/E2E.md).
This is a fixed-batch lock test, not a dynamic DecLock benchmark replacement.

## Automated checks

GitHub Actions runs the C++ unit tests, mock-verbs tests, a TCP quorum smoke test, and a native `libibverbs` compilation check on every push. The hosted runner has no RDMA device, so hardware tests run separately.

## Run on CloudLab Apt

The quickest path is four nodes with active InfiniBand ports: three acceptor nodes and one test client. Use the experiment fabric for all node-to-node test traffic.

**On every node:**

```bash
git clone https://github.com/OWNER/REPO.git
cd REPO
bash tools/apt/setup.sh
bash tools/apt/check.sh mlx4_0 1
bash tools/apt/build.sh
```

**On each of the three acceptor nodes:**

```bash
./mucql/build-apt/mucql_rdma_paxos server 29660 mlx4_0
```

**On the client node:**

```bash
./mucql/build-apt/mucql_rdma_paxos client 1001 1000 \
  REPLICA1_EXPERIMENT_IP:29660 REPLICA2_EXPERIMENT_IP:29660 \
  REPLICA3_EXPERIMENT_IP:29660 mlx4_0
```

Expected output contains `One-sided RDMA phase2 committed=1000` and `ordered_entries_per_sec=...`.

**Release marker test** (restart the release server between independent trials):

```bash
# One node:
./mucql/build-apt/mucql_verbs_release server 29670 mlx4_0 200000
# Client node:
./mucql/build-apt/mucql_verbs_release client RELEASE_SERVER_EXPERIMENT_IP 29670 100000 mlx4_0 cas
```

Expected output contains `RDMA release mode=CAS operations=100000` and `ops_per_sec=...`.

For a single-command-per-node setup and CSV capture, see [Apt quickstart](tools/apt/README.md).

## Updating

On each node, from the checkout:

```bash
git pull --ff-only
bash tools/apt/build.sh
```

No GitHub credentials are needed to clone or pull this public repository. Do not commit host configuration, private keys, or benchmark output; `.gitignore` excludes the usual local files.

## Scope

The RDMA ordering and release benchmarks exercise transports separately. They are not an application-level acquire/release throughput benchmark. The RDMA leader-takeover mode is experimental and disabled by default; real NIC fencing and clock-bound behavior must be validated separately.
