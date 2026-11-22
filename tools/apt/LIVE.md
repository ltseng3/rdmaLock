# Live MuCQL: bounded CN-admitted benchmark and optimization ablations

This benchmark is an **online, failure-free exclusive-lock** experiment. Dedicated
compute-node (CN) processes issue actual JOIN requests over TCP. The ordering
leader commits JOIN and PREPARE records to three RDMA acceptors while clients
execute protected RDMA WRITEs and hand off ownership directly using one-sided
CAS. A successfully completed CN request includes acquisition, one completed
8-byte RDMA WRITE, retirement, a release-marker CAS, and client completion.

**One command from MN4** after the one-time [CloudLab topology/SSH setup](AUTOMATION.md):

```bash
cd ~/rdmaLock && git pull --ff-only
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_smoke.json
```

Results appear in `results/automation/<run>/runs.csv`, `summary.csv`,
`ablations.csv`, and per-node raw logs. No manual process startup is necessary.
For a larger experiment, use `--suite tools/apt/suites/live_ablations.json` or
`--suite tools/apt/suites/live_scaling.json`. Always inspect the matrix first:

```bash
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_ablations.json --dry-run
```

The full ablation suite contains hundreds of independent trials, five repeated
measurements per matched configuration, and two seconds of warmup plus ten
seconds of measurement per trial. Each trial restarts services so write-once
slot state cannot contaminate subsequent trials. Case order is shuffled with
an explicit seed, and results are indexed by the same seed and trial number.

## Experimental levels: never mix the numbers

| Test kind | Work issued | Throughput meaning | DecLock comparable? |
|---|---|---|---|
| `ordering_micro` | One leader synthesizes a fixed batch | Committed consensus entries/s | No |
| `prepared_e2e` | Queue fully committed before GO | Preprepared handoff operations/s | No |
| `live_e2e` | Actual CN requests, ongoing JOIN and PREPARE | Completed live lock cycles/s in timed interval | Not yet: DecLock adapter and measurement parity missing |

`live_e2e` **does not precommit the timed queue**. Its warmup requests use the
same live path, and PREPARE is limited by the configured lookahead window.
Workers generate another request only after finishing their previous operation
(closed loop, one outstanding request per worker process), matching the useful
shape of DecLock's coroutine microbenchmark. With multiple workers per CN,
concurrency is the number of active worker processes, not a synthetic queue.
The transport **from CN to coordinator is TCP**, while replica ordering,
protected data operations, and client handoffs use RDMA. TCP admission and the
leader's release-marker RDMA READ are part of the measured cost. Never describe
this as a pure one-sided CN admission path or subtract these costs.

## Focused transport and preparation diagnostic (before the 320-case suite)

The live CN/coordinator control sockets now set `TCP_NODELAY` on both ends.
This is a **common admission transport correction** (not a MuCQL algorithmic
optimization). Earlier runs may have suffered Nagle/delayed-ACK latency and
must not be merged with post-fix results as though the transport were unchanged.

Run the following **28-trial diagnostic suite** before attempting the full sweep:

```bash
cd ~/rdmaLock && git pull --ff-only
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_diagnostic.json --fail-fast
```

It executes paired two-worker and eight-worker cases with two repetitions
per profile: raw, lookahead, reactive/proactive replenishment, full without JOIN
aggregation, full without RNIC phase-2 pipelining, and full. All cases use
one hot lock, a 200 ms warmup, a 1,000 ms measurement, and a fixed
131,072-slot log; the maximum 90,000 JOINs is an **upper bound**, not a
throughput target. If the rate exceeds the finite log, the test fails rather
than reporting a truncated rate. The roughly 16-MiB accepted-log allocation
per replica is substantially smaller than the 1,000,000-slot full sweep,
but still needs RNIC registration. These diagnostic durations and two
repetitions are for debugging and attribution hypotheses, **not**
publication-quality confidence intervals.

With `--fail-fast`, the first rejected trial stops the matrix, writes
`failure.txt`, retains the per-node logs, and includes `error` plus the
full intended configuration in `runs.csv`. In a clean run, use
`summary.csv` and `ablations.csv` to compare matched variants. Do not
start the 320-trial sweep if any diagnostic case fails.

## Named optimization profiles

Each profile resolves to an immutable set of six knobs in
[`profiles.py`](profiles.py):

| Knob | Raw | Full | Interpretation |
|---|---:|---:|---|
| `join_aggregate` | 1 | 16 | Real outstanding CN JOINs per logical batch |
| `phase2_window` | 1 | 16 | Phase-2 slots posted before draining RDMA completions |
| `prepare_range` | 1 | 16 | Successors represented by one PREPARE log record |
| `lookahead` | 2 | 64 | Maximum *unretired* prepared slots per lock |
| `replenish_threshold` | 1 | 16 | When to refill prepared successors |
| `batch_wait_us` | 0 | 25 | Explicit bounded request-aggregation wait |

The automation includes paired control/treatment cases for every optimization,
including variants in the context of the fully optimized protocol. **Controls
for JOIN aggregation hold `batch_wait_us` fixed at 25**; when aggregation=1,
waiting for additional requests is a no-op. For PREPARE range compression,
lookahead is held at 64 (range=1 vs 16). For lookahead, range and other knobs
are held fixed, because a range cannot safely exceed the active window.

Ablation effects are **contextual and not additive**. `ablations.csv` records
trial-paired control/treatment goodput, median speedup and signed percentage
change. Negative changes stay visible. The optional deterministic bootstrap
interval describes variability between trials but is not an independent
statistical proof. Use a controlled hardware schedule and several repetitions.
Do not compare `probe=1` runs with `probe=0` as if they carry equal work.

## Experimental constraints and why they matter

* **No reliable cross-host lease clock.** `EndlessClock` disables expiry and
  takeover, so the experiment fails closed on a crashed holder. This does NOT
  establish leader/client fault tolerance. It avoids introducing unvalidated
  clock uncertainty, but omits lease/recovery overhead and can alter normal
  production behavior. All performance claims must be qualified as failure-free.
* **Nonrecycled log/inbox slots.** Each JOIN uses its unique committed log index
  as a write-once release marker and inbox slot. `paxos_slots` determines
  registered-memory capacity. We do **not** reclaim old entries, since reuse
  requires generation fencing and a crash-consistent checkpoint. The run
  aborts rather than silently finishing early when the budget is exhausted.
  Memory-registration size may affect RNIC caching and memory pressure; record
  `paxos_slots` and hold it constant across paired tests. A 10-second sweep
  requires enough slots to sustain the entire warmup+timed interval.
* **Exclusive writes only.** One completed 8-byte RDMA WRITE per lock cycle.
  DecLock shared-reader and locality configurations are not comparable until
  MuCQL implements corresponding semantics. Workload lock IDs are per-worker
  reproducible by seeded uniform or Zipf(0.99) generators. Different rates
  consume different trace prefixes; seeds control distributions rather than
  guaranteeing identical ordered traces.
* **Bounded lookahead, online PREPARE.** A successor cannot activate before
  its JOIN and PREPARE records commit and the previous holder retires.
  Prepared-window progress is driven by the leader observing a positive
  write-once CAS release marker. The leader's marker READ is conservatively
  charged to throughput. No speculative lease transfer or timer-driven takeover.
* **Timing.** Each worker opens its timed interval on receipt of GO, after a
  readiness barrier. Clocks are local; there is no cross-host clock assumption.
  The coordinator sums completions tagged within each worker's local window
  and divides by the nominal interval. For long (10s) intervals the launch
  skew should be measured separately and reported if material. The startup,
  QP setup, initial Paxos phase 1, and post-run marker validation are outside
  the measured interval. Warmup is run through the same continuous live path.
* **Resources.** MN1–MN3 hold replicated consensus memory; MN4 holds protected
  data, release-marker memory, and the coordinator. CNs are physically separate
  by enforced topology validation. A DecLock run with fewer MNs must disclose
  its resource difference explicitly; identical clients do not imply identical
  server cost. Heavy CN process counts can still stress per-node RNIC/CPU.
* **Scope of verification.** On a fully completed run the coordinator checks
  every release marker, positive owner-CAS when `probe=1`, the per-lock final
  protected write, exactly-once client completions, and the committed JOIN
  schedule. Mock RNIC tests do not validate actual ConnectX-3 timing, ordering
  beyond mock semantics, or RNIC fencing under failure. Only CloudLab hardware
  runs establish hardware goodput.

## Before the DecLock comparison

The live MuCQL path is ready for **MuCQL-only** optimization ablations after
hardware validation, not yet a DecLock comparison. The automation explicitly
rejects the unavailable `declock` backend and marks every CSV row
`comparable_to_declock=NO`. For a valid comparison, integrate native DecLock
CQL-only and full DecLock execution under the same 2s/10s clocking and the
same CN nodes, number of closed-loop contexts, lock distribution, completed
exclusive RDMA WRITEs, result validation and selected NUMA/CPU placement.
Count **successful** operations (the upstream DecLock microbenchmark currently
increments its request count even when an acquire returns an abort indication).
Run `mucql_raw`, `mucql_full`, `declock_cql`, and `declock_full` on the same
external workload, preserving and disclosing differences in MN provisioning.
Do not invent DecLock results or mark an unfinished adapter as supported.

## Failure investigation and reruns

A failing trial emits `failure.txt` and raw logs and is excluded from positive
throughput summaries, never silently treated as zero ops/s. The one-command
runner exits nonzero if *any* requested trial fails. Check logs, capacity and
network configuration before repeating; do not discard slow passing trials.
For smoke tests, use `probe=1` before uninstrumented ablations. If the
nonrecycling cap trips, increase the reserved slot count (within the
implementation's 1,000,000-slot bound), shorten the requested interval, or
record that the configured workload is unsupported at that rate.
