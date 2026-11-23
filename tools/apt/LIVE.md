# Live MuCQL: bounded CN-admitted benchmark and optimization ablations

This benchmark is an **online, failure-free exclusive-lock** experiment. Dedicated
compute-node (CN) processes issue actual JOIN requests over two-sided RDMA
SEND/RECV by default, with TCP available as a matched transport control. The ordering
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
The default steady-state client-to-leader transport is **two-sided RC RDMA
SEND/RECV**. TCP is retained **only for QP/configuration bootstrap before GO**,
and can be selected for a fully identical performance control using
`"transports": ["tcp"]` in a live suite. Every timed JOIN, PREPARE ticket,
handoff notification, DONE, GO/STOP, and FINISH message uses the selected
control transport. CN-to-CN handoff and protected data access remain one-sided
RDMA. Results include a mandatory `transport` column and never merge TCP
with RDMA observations. Any live suite without a transport axis defaults to
`"rdma"`.

The former CN-to-coordinator TCP path remains an explicitly selectable
baseline. Replica ordering, protected data operations and direct ownership
handoffs use one-sided RDMA in either mode. The leader's release-marker RDMA
READ is charged to measured throughput. The selected client admission and
notification transport is charged to request latency; do not subtract it.

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

## RDMA control transport hardware validation

The two-sided control path has a dedicated, bounded QP with 32 preposted
receives and registered send/receive buffers **per CN worker**. The leader
has one QP per worker. Send and receive CQs are separate to permit a
background RECV thread with a synchronous SEND producer. Each received
frame is copied then its receive buffer is reposted before the next poll.
Pending recv threads can be cancelled and joined on failure. The TCP
socket remains only for initialization, startup, and cancellation.

Run `live_transport_smoke.json` before performance comparisons:

```bash
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_transport_smoke.json --fail-fast
```

It runs 16 distinct safety-probed cases: tcp and rdma, raw and full, two
and eight workers (two repetitions). All are the same exclusive hot-lock
workload with a bounded live queue. Inspect `runs.csv` for per-transport
latency, throughput, and `violations=0`. If a single RDMA run fails, keep
its logs and do not interpret TCP results as proof that the RDMA path works.
The test mock models registered buffer ownership and CQ completions but
**cannot validate real mlx4 RNR, CQ ordering, MR registration, or RNIC timing**.

## Intermittent release-server TCP port collisions

If a smoke trial fails with
`ibverbs release: TCP bind 39670: Address already in use`, the release
server on MN4 never started and that trial **did not measure MuCQL**.
It is not an RDMA CAS/consensus safety failure. Previous successful trial
rows can still be inspected, but a multi-repetition result is incomplete.

Lifecycle checks now verify whether the next server can **actually bind and
listen** on the configured wildcard TCP port, rather than merely requiring
that a `LISTEN` line disappear from `/proc/net/tcp`. The release-server
bootstrap bind also retries only transient `EADDRINUSE` for at most four
seconds; a persistent conflict is a hard error. Failed startup reports
include `TCP_DIAGNOSTIC` from all TCP socket states (when `ss` exists).
No process is killed by port number or command name. A job only stops its
own tagged process group.

On **MN4**, inspect an unresolved collision **without killing anything**:

```bash
sudo ss -tanp | grep ':39670' || true
pgrep -af mucql_verbs_release || true
```

Inspect the owner and `results/automation/<session>/<failed-case>/mn4/stdout.log`
and `failure.txt` before deciding whether an orphaned prior-run process
needs manual intervention. Do not run multiple controllers concurrently or
run DecLock at the same time. If no owner appears, the conflict may have
been transient; a missing socket after the failure does not establish its
original cause.

After updating all nodes to the patched commit, **rerun the 24-case
`live_declock_hot_smoke.json` with `--fail-fast`**. Run the 60-case
`live_declock_hot_match.json` only after every smoke trial passes. Do not
combine incomplete pairs with a new commit's results.

## Paxos accepted-log preflight and long-session lifetime

The 2-second DecLock-style hot-match suite initially used 524288 Paxos
slots, which maps to **64 MiB of accepted-log MR per Paxos replica**:
524288 × 128-byte slots. On real hardware, all three replicas appeared
ready but the coordinator received TCP EOFs during phase 1. This is
consistent with failure or delay while registering the large MR, but
server-side MN1–MN3 logs are required to establish the actual cause.

Each Paxos server now **allocates and registers its initial accepted log
before printing the ready token**, then uses the already registered QP and
MR for the first ballot. Insufficient pinned memory is therefore a
startup failure, not an ambiguous coordinator quorum failure.
The failure explicitly logs the slot count, registration bytes,
errno, and RLIMIT_MEMLOCK soft limit. The phase-1 TCP handshake uses
a ten-second timeout, then removes its receive timeout for the lifetime
of the established RC QP; long measurements must not cause an idle
bootstrap socket to expire.

For the first matched comparison, the hot and Zipf suites reserve
**131072 slots (16 MiB per Paxos replica)** and **90000 maximum operations**,
the footprint already demonstrated by the passing 24-case hardware smoke.
The two-second measured interval, 0% reads, exclusive-lock workload,
8-byte write count, and client concurrency are unchanged. Every run
still fails rather than silently truncating if its fixed nonrecycling log
or operation budget is exhausted; a failure is not a throughput sample.
If that occurs, record the specific failure and test an explicitly
validated larger MR configuration. Do not lower the benchmark duration or
accept a truncated run as an apples-to-apples result.

For a phase-1 failure on a replica, inspect its server log:

\`\`\`bash
# On the CloudLab experiment controller, inside the failed case directory:
grep -R -E 'accepted MR registration|RDMA acceptor session|RDMA Paxos provisioning' \
    mn1 mn2 mn3
\`\`\`

## Key-count scaling BEFORE tuning

**Run this before the new idle-poll / early-connection tuning suites.**
The one-key, all-exclusive workload is a useful stress test, but its
serialized ownership transfer does not characterize multi-key scalability.
The live benchmark now supports **1–100,000 distinct lock IDs**, deterministic
uniform keys, and Zipf(alpha=0.99) keys. This matches DecLock's default key
population, although MuCQL still implements only exclusive locks. A key
population is **not** a committed-operation count: per-key protected data
and owner-marker regions allocate 16 bytes per key, while the consensus
history keeps its own fixed write-once budget. No log reclamation is enabled.

This experiment changes **only the number of keys and the distribution**.
To avoid an optimization confound, it holds the baseline at raw, RDMA
control, one completed 8-byte RDMA data WRITE, 0% reads, no CPU busy-work,
no idle polling sleeps, and **handoff_preconnect=0** (the legacy raw path).
It uses 3 Paxos replicas plus the data MN. Each result retains release
marker checking, and the smoke trials additionally probe exclusive owners.

For reproducibility, all cases fix Paxos slots to 131072 and max_ops to
90000 (the previously hardware-tested 16 MiB-per-replica registration).
These logs are nonrecycling; if a case exceeds the fixed operation/log
budget, it must FAIL and be reported, not silently truncate its elapsed time.
The prior reservation of one extra Paxos log slot **per configured key**
has been replaced by a fixed 64-slot PREPARE headroom check: keys that
receive no JOINs must not consume log history. The coordinator still
checks every JOIN and PREPARE against actual remaining log capacity.
For 100000 keys with 131072 Paxos slots, the MN4 data + owner MR
has 331104 8-byte slots (~2.53 MiB); each Paxos replica remains at 16 MiB.
Longer tests still need proper log reuse, which is separate future work.

### Stage 1: 22 safety smoke trials (8 total workers)

On the experiment controller (Node 4):

~~~bash
cd ~/rdmaLock && git pull --ff-only
python3 tools/apt/run_suite.py \
  --suite tools/apt/suites/live_key_scaling_smoke.json --fail-fast
~~~

These trials cover one lock and 16/256/4096/16384/100000 keys
(both uniform and Zipf on multi-key populations), with two repetitions,
100 ms warmup, 500 ms measurement and ownership probes enabled. Before a larger sweep, verify
all trials PASS with zero ownership violations.

### Stage 2: 51-case 8-worker key-count curve

~~~bash
python3 tools/apt/run_suite.py \
  --suite tools/apt/suites/live_key_scaling_8.json --fail-fast
~~~

Lock counts are **1, 4, 16, 64, 256, 1000, 4096, 16384, and 100000**;
for all counts greater than one, compare uniform with Zipf(alpha=0.99).
Each has three repetitions and a 1000 ms measured interval (zero
warmup), giving 17 key-count/distribution configurations times three trials. Eight
total CN workers = 4 workers per CN.

This is a **scaling survey**, not the prior 2-second matched DecLock
measurement. Do not plot its throughput alongside the 2-second historical
samples without marking the differing measurement duration.

### Stage 3: optional 51-case 16-worker curve

~~~bash
python3 tools/apt/run_suite.py \
  --suite tools/apt/suites/live_key_scaling_16.json --fail-fast
~~~

Exactly the same workload factors, but eight workers per CN / 16 total,
to identify whether the previously observed scaling collapse persists as
the key count increases. These configurations can be paired with Stage 2.

**Read the curves before touching other tunables.** Look for:
(i) the key-count knee at which throughput stops improving, (ii) P99
latency and P99 handoff-wait changes, (iii) residual Zipf hot-key
contention relative to uniform, (iv) whether 16 workers finally scale
beyond eight. If the raw multi-key curve remains near one-key throughput,
investigate the single-threaded coordinator's synchronous decisions and
per-key scanning; do not randomly vary JOIN, pipelining or polling knobs.

All three suites explicitly set handoff_preconnect=0 and both polling
backoffs to 0, so they reproduce the original hot-path algorithm even
though the new implementation defaults to early connection. Later,
select a representative high-contention and low-contention key count
for focused A/B optimization tests.

## CPU polling and early handoff connection experiment

This is **MuCQL-only tuning**, not a DecLock benchmark or a demonstrated
hardware performance improvement. The completed 60-case 2-second hardware
baseline on revision 8479550 reported these median successful operations/s:

| Active writers | RDMA writes | raw | full_no_join | full |
| --- | ---: | ---: | ---: | ---: |
| 8 | 1 | 19379.5 | 18883 | 17884.5 |
| 8 | 32 | 8814.5 | 9019 | 9047 |
| 16 | 1 | 15857 | 14983.5 | 11295 |
| 16 | 32 | 7153 | 6793 | 6487 |

Two independently testable changes preserve the existing replicated
JOIN/PREPARE decisions and handoff certificate.

* **Early RC handoff connection:** on HandoffReady (which requires a
  committed PREPARE), the predecessor opens the successor's RDMA RC
  connection before waiting for its own release. The one-sided CAS into
  the successor inbox still waits for locally confirmed retirement.
  This behavior is controlled by handoff_preconnect (default 1, old 0).
* **CPU backoff on idle polling:** optional microsecond sleep every 32
  unsuccessful CQ RECV polls (rx_idle_us 0..100) and optional sleep
  every 32 unsuccessful predecessor-inbox polls (handoff_idle_us
  0..100). Both default to 0, the original yield-only behavior.
  SEND and Paxos phase-2 CQ paths are unaffected.

Per-trial configuration is propagated as MUCQL_RX_IDLE_US,
MUCQL_HANDOFF_IDLE_US and MUCQL_HANDOFF_PRECONNECT. PASS output
and CSV grouping include all three parameters, so distinct polling
strategies cannot be silently pooled. All release-marker validation
and optional mutual-exclusion probes are retained.

Run the **28-case safety smoke** first on Node 4:

~~~bash
cd ~/rdmaLock && git pull --ff-only
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_hot_poll_smoke.json --fail-fast
~~~

Only if every case passes, run the 57-case, two-second tuning suite:

~~~bash
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_hot_poll_tuning.json --fail-fast
~~~

Measure changes in median throughput, P99 latency and P99 handoff wait for
16 writers / one RDMA WRITE and 8 writers / 32 RDMA WRITEs. Do not assume
that a nonzero sleep setting helps: Linux timer granularity might
increase admission latency. Preserve slow PASS trials, and do not
combine rows from differently tuned configurations. If the backoff
policy reduces throughput, the 0 setting restores original behavior.

Numerical comparison caveat: DecLock Figure 12 uses up to 256 clients
across eight CNs with 100 Gbps ConnectX-4 RNICs, with default
100,000 locks and 50% reads. Our setup has two CNs, mlx4 RNICs,
one hot exclusively written lock and a replicated Paxos log.
Absolute throughput numbers are **not directly comparable**.

## DecLock-style exclusive critical-section comparison

The live worker now supports **\`data_ops\` = 1..32**, exactly that many
sequential **8-byte one-sided RDMA WRITEs**, each waited to successful CQ
completion, while it holds an exclusive lock. Every operation targets the
per-key protected data slot on MN4 and writes the unique grant ID; the final
value is verified after completion. The existing \`work_ns\` option is CPU
busy-work and **is not** a DecLock substitute. Zero read ratio is intentional:
MuCQL currently implements exclusive locks, not shared-reader semantics.

This matches DecLock's \`MICROBENCH_SHOT_NUM\` loop in
\`tests/workload/micro.h\`, with \`MICROBENCH_DATA_SIZE=8\`,
\`read_ratio=0\`, \`locality=0\`, one coroutine per core, and the number
of RDMA completions matching \`data_ops\`. Both workloads access one 8-byte
location per lock. The lock algorithms and resource footprint differ: MuCQL
uses 3 Paxos MNs + MN4, while DecLock uses a single MN. This difference must
be reported and **must not** be hidden by a shared "apples-to-apples" label.

Each LIVE trial now reports and validates
\`data_ops\`, \`data_size_bytes=8\`, \`read_ratio=0\`, and
\`data_writes_measured=measured*data_ops\`. Run summaries and ablations group
by these fields, so 1-write and 32-write trials cannot be merged. The counter
validates **posted-and-completed** write-loop semantics, not RNIC hardware
counters. Unlike DecLock's \`request throughput\` attempt counter, compare
only **successful completed lock cycles** and refuse the baseline unless its
goodput instrumentation is installed.

### Run these one at a time on Node 4

\`\`\`bash
cd ~/rdmaLock && git pull --ff-only
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_declock_hot_smoke.json --fail-fast
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_declock_hot_match.json --fail-fast
python3 tools/apt/run_suite.py --suite tools/apt/suites/live_declock_skew_match.json --fail-fast
\`\`\`

**Start with the 24-trial smoke suite** (2 CN × 4/8 workers each, 1 hot lock,
data_ops=1/32, raw/full_no_join/full, 2 trials, probe enabled).
If it succeeds, the 60-trial matched suite measures the same conditions with
probe disabled, **0 ms warmup and 2,000 ms measurement** (five repetitions),
matching DecLock's unmodified \`TEST_TIME_MS=2000\`. The optional 36-trial
skew suite uses 16 writers and 8, 64, or 1000 keys at Zipf alpha 0.99, with
the same two durations and 1/32 writes. All runs use RDMA SEND/RECV for
client admission and one-sided RDMA for the data writes.

**Do not invoke the 320-case suite** or treat these MuCQL-only numbers as
a measured DecLock comparison. Run the upstream \`dracit7/declock\` benchmark
on the same MN4 and CNs, with one coroutine/core, 0% reads, the same lock
count and writes, on the same RNICs. Pin exact git revisions. Our helper
\`tools/apt/declock_match_plan.py\` can generate an exact DecLock command
manifest and per-host \`set-env.sh\` template without modifying the baseline.
DecLock's \`dmlock\` variant is PF; \`dmlock_tf\` is TF. Before comparing
goodput, apply \`tools/patch_declock_benchmark.py\` to the external DecLock
checkout, rebuild both variants, and parse its **successful lock throughput**
rather than its legacy attempt counter. DecLock's `LLOG` writes this metric
to **stderr**. Capture the full `run-on-all.sh` stderr/stdout output (as
upstream `getresults.sh` does in `tmp`), verify both CNs exited, then run:

```bash
python3 tools/apt/parse_declock_goodput.py --cores-per-cn 8 /path/to/combined-run.stderr.log
```

A log containing only `request throughput` is rejected, as is a missing,
duplicate, or partially collected core. Do **not** parse the remote CN
stdout latency files as goodput. Example planning command (does NOT run jobs):

```bash
python3 tools/apt/declock_match_plan.py --declock-dir ~/declock \
    --workers-per-cn 8 --locks 1 --data-ops 1 --data-ops 32
```

This writes `results/declock_match_plan/cases.json`,
`commands.txt`, and `set-env.sh` for review. Install the environment
on MN4/CN1/CN2 only after checking hostnames/SSH and verifying that
the same upstream pinned source and rebuilt binary are present on all
three nodes. Run one baseline at a time. Validate SSH, dependencies, pinning,
and benchmark launch on hardware; no DecLock hardware run has been performed
by GitHub CI. Do not run MuCQL and DecLock concurrently.

### Interpretation caveats

* One lock is maximally contended and uniform/Zipf are indistinguishable.
* With multiple keys, both systems use Zipf alpha 0.99 but different seeded
  PRNG implementations; therefore a statistical workload match, not an
  identical request trace. Key heat and client affinity should be measured.
* MuCQL uses one independent worker process per outstanding request; DecLock
  uses one coroutine/core. Align **active requests**, actual CPU allocations,
  one-way network links, and measured read ratio before calling results
  performance-comparable.
* The data write loop is included in MuCQL's timed end-to-end latency;
  the release marker and replicated JOIN/PREPARE decisions are not subtracted.
* The 2,000 ms, zero-warmup matching suite is a direct artifact duration
  match; do not mix it with earlier 1,000 ms/500 ms smoke trials.
* Software mock verbs never demonstrate actual mlx4 RDMA completion behavior.
  First prove correctness with the safety-probed hardware suite.

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
