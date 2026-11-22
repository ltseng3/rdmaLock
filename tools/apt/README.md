# CloudLab Apt quickstart

The examples use four reserved nodes: `r1`, `r2`, `r3` (RDMA acceptors) and `c1` (client). Use the **experiment network** for TCP setup and the active InfiniBand port for RDMA. GitHub is used only to distribute source and run build/tests.

## 1. Before the first run

1. Create an **empty public GitHub repository**. Do not add an initial README, .gitignore, or license file in the GitHub UI; this checkout supplies the README and .gitignore.
2. From your **laptop**, push this checkout using the commands below. A personal GitHub credential may be required **on your laptop**, never on Apt nodes.

```bash
# Run from the unpacked repository directory on your laptop.
git init -b main
git add .
git commit -m 'Initial version'
git remote add origin https://github.com/OWNER/REPO.git
git push -u origin main
```

3. In GitHub, open the **Actions** tab. The `Build and tests` workflow runs automatically on pushes to `main` and can also be started manually via **Run workflow**. It compiles the native RDMA targets and runs tests against mock verbs, without any hardware. A green workflow does **not** verify NIC operation.

## 2. First-time setup on each of the four Apt nodes

SSH to each node using the CloudLab-provided login and run:

```bash
git clone https://github.com/OWNER/REPO.git
cd REPO
bash tools/apt/setup.sh
bash tools/apt/check.sh mlx4_0 1
bash tools/apt/build.sh
```

If `mlx4_0` is not the active IB device, use `ibv_devices`, `ibv_devinfo -v`, and `ibstat` to find the right name/port. If your port is 2, use `MUCQL_IB_PORT=2` for every process. The setup uses `sudo` for system packages only; cloning and pulling a public repo need no GitHub credentials.

If the link is not `PORT_ACTIVE`, do not run the benchmark. Check the CloudLab flexible-fabric settings, IB subnet, and port choice first. Do not substitute the public control-network IPs for experiment-network IPs.

## 3. Easiest manual ordering test

Start one acceptor on each of `r1`, `r2`, and `r3`:

```bash
cd REPO
./mucql/build-apt/mucql_rdma_paxos server 29660 mlx4_0
```

On `c1`, use the private experiment-network IP addresses of the **three acceptors**:

```bash
cd REPO
./mucql/build-apt/mucql_rdma_paxos client 1001 1000 \
  R1_EXPERIMENT_IP:29660 R2_EXPERIMENT_IP:29660 \
  R3_EXPERIMENT_IP:29660 mlx4_0
```

A successful run prints output similar in form (not value) to:

```text
One-sided RDMA phase2 committed=1000 seconds=... ordered_entries_per_sec=...
```

This is a single-leader, one-ballot run; **restart all three acceptor processes between trials**. The default memory region holds a limited number of entries. Use 1,000 for the initial run; the supplied runner limits the count to 4,096.

## 4. Easiest manual release-marker test

On `r1`, stop the ordering server and start the release server:

```bash
./mucql/build-apt/mucql_verbs_release server 29670 mlx4_0 200000
```

On `c1`:

```bash
./mucql/build-apt/mucql_verbs_release client R1_EXPERIMENT_IP 29670 100000 mlx4_0 cas
```

A successful run prints `RDMA release mode=CAS operations=100000 ... ops_per_sec=...`. Each CAS slot is write-once; restart the server between runs. `write` mode is a transport experiment only, not certifying evidence for early lock recovery.

## 5. Single command to collect repeated measurements (optional)

Use this mode **from your laptop**, where your existing CloudLab SSH key is already available. It requires no SSH private keys or GitHub credentials on the nodes. All hosts must be reachable through `ssh USER@SSH_HOST` from your laptop; the private test IPs are separate from the SSH hostnames.

Copy and edit:

```bash
cp tools/apt/hosts.example.env hosts.env
$EDITOR hosts.env
```

Set `R1`, `R2`, `R3` to the experiment-network IPs; set `R1_SSH`, `R2_SSH`, `R3_SSH`, `CLIENT_SSH` to the accessible CloudLab SSH logins; set `GITHUB_REPO=OWNER/REPO`; and set `REMOTE_DIR=REPO` (the directory created by the clone above). Check that `IB_DEV` and `IB_PORT` are correct.

From your laptop's checkout:

```bash
bash tools/apt/cluster.sh sync           # Pull public GitHub and rebuild on all 4 nodes
bash tools/apt/cluster.sh check          # All four RDMA ports active?
bash tools/apt/cluster.sh all 3          # Three ordering + three CAS marker trials
```

This restarts servers for independent trials and saves raw logs plus `results/TIMESTAMP/summary.csv` **on your laptop**. Each client also retains its own `results/smoke.csv` (ignored by Git). To run only one test, use `cluster.sh ordering 3` or `cluster.sh release 3`; to stop tracked servers, use `cluster.sh stop`.

After pushing new code to GitHub, the update workflow is just:

```bash
bash tools/apt/cluster.sh sync
bash tools/apt/cluster.sh all 3
```

The script uses ordinary SSH, not unattended credentials on the nodes. If laptop-to-Apt SSH is unavailable, use the manual commands in sections 3 and 4 instead.

## 6. Interpret the numbers

- `ordered_entries_per_sec` is the single-leader RDMA ordering throughput for this binary; it is **not lock throughput**.
- `ops_per_sec` in the release test measures signaled, individually completed RDMA marker operations; it is **not a fully pipelined handoff throughput measurement**.
- GitHub Actions runs in a VM without an RDMA NIC. Hardware numbers come only from Apt, not from the CI workflow.
- The application-level lock backend for DecLock is not yet wired into these standalone tests, and the takeover/fencing path is experimental and disabled by default. Do not infer end-to-end mutual exclusion or failure recovery from an RDMA transport success.

## 7. Common failures

- **`PORT_ACTIVE` check fails:** wrong port, IB subnet not ready, or flexible fabric in Ethernet mode.
- **Connection refused:** server not started, wrong experiment IP, missing private-network route, or a prior server still owns the port.
- **Remote atomic error:** verify ConnectX-3 atomic support and memory registration; inspect server log, `ibv_devinfo -v`, and `/var/log/kern.log`/`dmesg` as appropriate.
- **`ulimit -l` too small:** increase the memlock limit and rerun; RDMA memory registration can fail with low limits.
- **SSH orchestration cannot connect:** check your local CloudLab SSH identity or use four interactive terminals; do not add credentials to the repo.
- **Second run fails:** restart the acceptors/release server between independent runs; the slots are not recycled.

For baseline comparison, use the original DecLock `SYSTEM=cqlock` benchmark separately and apply the included completed-operation counter patch to both baselines. These transport smoke tests are not a direct CQL comparison.
