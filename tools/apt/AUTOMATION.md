# One-command CloudLab benchmark automation

Use **one dashboard terminal on any configured CloudLab node**, preferably
MN4 (`10.10.1.4`), to control the entire deployment and collect results.
The controller uses SSH into the experiment IPs; neither clients nor MNs need
a GitHub token or access to a private repository. The code comes from the
public repository pinned to the exact same commit on every host.

## One-time configuration

Copy the tracked example to a local override on the **controller node**:

```bash
cp tools/apt/topology.json tools/apt/topology.local.json
nano tools/apt/topology.local.json
```

Edit `topology.local.json` (git-ignored, automatically preferred if present):
keep the three `replicas` and `data_mn`
on distinct hosts, set the real experiment IPs for `clients`, and specify
physical cores per CN. The supplied `.5`/`.6` CN addresses are **examples**,
not verified reservations. You can add CNs to the list; suite expansion will
choose available node counts. The runner refuses to colocate CNs on MNs.

From MN4, passwordless SSH must work over the experiment fabric. For example:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 "${USER}@10.10.1.5" hostname
```

CloudLab dashboard shell access **does not automatically guarantee** node-to-node
SSH authentication. If this fails, configure your CloudLab user key or agent
forwarding through normal CloudLab facilities. The automation will not disable
SSH authentication, copy SSH private keys, or require GitHub credentials.

The controller sends each trial configuration to all nodes automatically; only
the controller needs `topology.local.json`. The nodes need git, cmake, libibverbs
and an active mlx4 InfiniBand port.
If dependencies aren't installed, run once with `--install-deps` (requires
passwordless sudo on the target hosts). Run from the same user account on all
hosts, or pass `--ssh-user NAME`.

## One command to build, test, gather logs and summarize

```bash
cd ~/rdmaLock && git pull --ff-only && python3 tools/apt/run_suite.py
```

This runs `suites/smoke.json` (seven independent trial configurations):
three single-driver ordering batch sizes and four checked/uninstrumented
fixed-batch direct-handoff trials. It automatically:

1. Validates the host/role topology and expands the JSON test matrix.
2. Logs in to every needed node with noninteractive SSH (no password prompts).
3. Pins each node to the **same commit**, builds native RDMA binaries and checks the IB port.
4. Synchronizes trial-specific settings and starts fresh acceptor/release processes.
5. Performs the remote CAS/READ/WRITE hardware preflight before E2E tests.
6. Runs ordering or handoff on the selected CNs, with worker IDs assigned automatically.
7. Collects per-host raw logs, validates full completion, and shuts down only its own processes.
8. Writes `runs.csv`, `summary.csv`, per-trial `config.json`, logs, `failure.txt`, and `manifest.json`.

Results are saved under `results/automation/<timestamp>/` on the controller.
`summary.csv` groups runs with the same settings and reports median, minimum
and maximum ops/sec; `runs.csv` retains every trial, including failures. A
failed run is **never** silently treated as zero throughput or included in a
median. The runner exits nonzero if any configuration fails.

## Live CN-admitted smoke and MuCQL ablations

These new suites exercise **ongoing CN-generated JOIN and bounded online
PREPARE**, not an offline-prepared queue:

```bash
cd ~/rdmaLock
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_smoke.json
# Then, after smoke and owner-probe checks pass on actual hardware:
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_ablations.json
# Additional CN and lock-distribution scaling:
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_scaling.json
```

The live suites produce an additional `ablations.csv` with matched paired
speedups and gains; old fixed-batch suites remain unchanged. The full ablation
suite includes **320 independent trials** at its default two CNs and five
repetitions per case. Control/treatment order is shuffled with a fixed seed.
See [Live semantics, limitations and fair-comparison requirements](LIVE.md).

**Note:** Live MuCQL throughput is measured (not a precommit handoff-only
metric), but DecLock baseline execution is not yet integrated. Every result
remains labeled `comparable_to_declock=NO` until the shared DecLock benchmark
is implemented and validated.

## Run the larger ablation suite

```bash
cd ~/rdmaLock && python3 tools/apt/run_suite.py \
  --suite tools/apt/suites/ablations.json
```

This sweeps JOIN RDMA batch sizes (1/8/16/32), CN process counts, safety probe,
critical-section work, and repetitions, subject to the current implementation's
maxima (3,000 positions; 64 worker processes). The `cn_counts` field can be
`"powers_of_two"`: if you add 4 or 8 CNs to the topology, the same suite
expands automatically to 2/4/8 CN configurations.

To inspect the matrix without contacting any host:

```bash
python3 tools/apt/run_suite.py --suite tools/apt/suites/ablations.json --dry-run
```

To deploy/build/verify the machines but not run tests:

```bash
python3 tools/apt/run_suite.py --bootstrap-only
```

The automation uses TCP ports **39660, 39670, 39700, and 39711–39774** to
avoid interfering with manually started prototype tests on the original ports.
It refuses concurrent runs from the same controller via a lock file.

## Important measurement scope

The available modes are strictly labeled:

- `ordering_micro`: a **single coordinator** issuing fixed batches to the quorum.
  It does **not** demonstrate concurrent CN-generated JOIN throughput.
- `prepared_e2e`: a **precommitted queue** followed by direct handoff. The
  `goodput_ops_per_sec` field excludes ordering; the
  `including_ordering_ops_per_sec` field includes sequential batch preparation.

The two older modes are not a steady-state DecLock comparison. The new
`live_e2e` mode measures online client-generated JOIN, PREPARE and handoff, but
DecLock integration and workload parity are still outstanding. Every CSV row includes
`comparable_to_declock=NO`. The runner rejects an unavailable `declock`
backend rather than inventing results. Named `raw`, `full` and per-optimization
profiles are selected through the `live_e2e` suite; these are not separate
runner test kinds. A native DecLock execution adapter and common validation
remain outstanding.

Both the protected operation (one completed 8-byte RDMA WRITE) and the
independent owner probe remain explicit. Never mix probe=1 and probe=0 results
as equivalent workloads. No lease-clock or failover safety claim is implied by
passing this hardware benchmark.

## Failure and troubleshooting

When a trial fails, inspect `results/automation/<timestamp>/<case>/failure.txt`
and the collected `stdout.log` files. A missing CN/MN address, failed SSH
key exchange, inactive IB port, busy test port, or nonzero worker exit is a
hard error. A prior manually started server can keep running; automation
will not indiscriminately kill unknown processes. No GitHub authentication or
push of benchmark data is performed.
