#!/usr/bin/env python3
"""One-command CloudLab deployment, isolated benchmark sweeps and CSV collection.

Only currently implemented measurements are accepted: ordering_micro and
prepared_e2e. Live, fair DecLock comparisons are explicitly unsupported.
"""
from __future__ import annotations
import argparse
import copy
import csv
import datetime as dt
import fcntl
import ipaddress
import itertools
import json
import math
import os
from pathlib import Path
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
import isolated_cluster as cluster
from e2e_results import extract as extract_e2e

SSH_OPTS = ['-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=accept-new',
            '-o', 'ConnectTimeout=8', '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3']
WORKDIR = 'rdmaLock'
REPO_URL = 'https://github.com/ltseng3/rdmaLock.git'
SUPPORTED = {'ordering_micro', 'prepared_e2e'}
COLUMNS = ['case', 'trial', 'kind', 'measurement_scope', 'comparable_to_declock',
           'commit', 'cn_nodes', 'worker_processes', 'positions', 'batch', 'probe',
           'work_ns', 'completed', 'verified_markers', 'violations',
           'ordering_seconds', 'handoff_seconds', 'goodput_ops_per_sec',
           'including_ordering_ops_per_sec', 'ordered_entries_per_sec', 'status']


def require_int(value, lo, hi, name):
    if type(value) is not int or not lo <= value <= hi:
        raise ValueError(f'{name} must be integer {lo}..{hi}')
    return value


def expand_suite(suite, topology):
    if not isinstance(suite, dict) or not isinstance(suite.get('tests'), list) or not suite['tests']:
        raise ValueError('suite must contain nonempty tests array')
    result = []
    for spec in suite['tests']:
        name, kind = spec.get('name'), spec.get('kind')
        if not isinstance(name, str) or not re.fullmatch('[a-z][a-z0-9_-]{0,35}', name):
            raise ValueError('test name must be a simple lowercase identifier')
        if kind not in SUPPORTED:
            raise ValueError(f'{name}: unsupported kind {kind!r}; continuous/DecLock benchmarks are NOT implemented')
        repetitions = require_int(spec.get('repetitions', 1), 1, 20, 'repetitions')
        def axis(key, default, lo, hi):
            vals = spec.get(key, [default])
            if type(vals) is not list or not vals:
                raise ValueError(f'{name}.{key} must be a nonempty array')
            for v in vals:
                require_int(v, lo, hi, f'{name}.{key}')
            return vals
        batches = axis('batches', 1, 1, 32)
        counts = axis('positions', 1000, 2, 3000)
        if kind == 'ordering_micro':
            if any(key in spec for key in ('cn_counts', 'workers_per_cn', 'probes', 'work_ns')):
                raise ValueError('ordering_micro cannot claim CN concurrency or handoff workload')
            combinations = itertools.product(batches, counts, [0], [0], [0])
        else:
            if spec.get('cn_counts') == 'powers_of_two':
                cns = [2**n for n in range(1, 10) if 2**n <= len(topology['clients'])]
                if not cns:
                    raise ValueError('need at least 2 CNs')
            else:
                cns = axis('cn_counts', len(topology['clients']), 2, len(topology['clients']))
            workers = axis('workers_per_cn', 1, 1, 16)
            probes = axis('probes', 1, 0, 1)
            work = axis('work_ns', 0, 0, 1_000_000_000)
            combinations = itertools.product(batches, counts, cns, workers, probes, work)
        for combo in combinations:
            if kind == 'ordering_micro':
                batch, positions, _, _, _ = combo
                cn_count, workers_per_cn, probe, work_ns = 0, 0, 0, 0
            else:
                batch, positions, cn_count, workers_per_cn, probe, work_ns = combo
                if positions < cn_count * workers_per_cn:
                    raise ValueError(f'{name}: fewer positions than worker processes')
                for cn in topology['clients'][:cn_count]:
                    if workers_per_cn > cn.get('cores', 8):
                        raise ValueError(f'{name}: {workers_per_cn} workers oversubscribe {cn["id"]}')
                if cn_count * workers_per_cn > 64:
                    raise ValueError(f'{name}: >64 worker processes')
            for trial in range(1, repetitions + 1):
                case = dict(name=name, kind=kind, trial=trial, batch=batch,
                            positions=positions, cn_count=cn_count,
                            workers_per_cn=workers_per_cn, probe=probe, work_ns=work_ns)
                case['id'] = f'{len(result)+1:04d}-{name}-t{trial}'
                result.append(case)
    if len(result) > 1000:
        raise ValueError('suite has more than 1000 trials; split it into smaller suites')
    return result


def case_topology(original, case, ballot):
    doc = copy.deepcopy(original)
    if case['kind'] == 'prepared_e2e':
        doc['clients'] = doc['clients'][:case['cn_count']]
        for cn in doc['clients']:
            cn['workers'] = case['workers_per_cn']
    # Dedicated automation ports avoid older manually launched 29660/29670 services.
    doc['ports'] = {'ordering':39660,'data':39670,'coordinator':39700,'worker_base':39711}
    doc['benchmark'].update(positions=case['positions'], ballot=ballot,
                            order_batch=case['batch'], probe=case['probe'],
                            work_ns=case['work_ns'])
    cluster.validate(doc)
    return doc


def parse_summary(kind, text):
    if kind == 'prepared_e2e':
        d = extract_e2e(text)
        for f in ('goodput_ops_per_sec', 'including_ordering_ops_per_sec'):
            if not math.isfinite(float(d[f])) or float(d[f]) <= 0:
                raise ValueError('invalid throughput ' + f)
        return d
    lines = [x for x in text.splitlines() if x.startswith('One-sided RDMA phase2 committed=')]
    if len(lines) != 1:
        raise ValueError('expected exactly one RDMA ordering summary')
    d = dict(re.findall(r'([a-z_]+)=([^\s]+)', lines[0]))
    for key in ('committed', 'batch', 'seconds', 'ordered_entries_per_sec'):
        if key not in d:
            raise ValueError('ordering summary missing ' + key)
    if int(d['committed']) <= 0 or float(d['seconds']) <= 0 or float(d['ordered_entries_per_sec']) <= 0:
        raise ValueError('bad ordering summary')
    return d


class Executor:
    def __init__(self, *, username, checkout, local_ips, repo_dir=WORKDIR):
        if not re.fullmatch('[a-zA-Z0-9_-]{1,32}', username):
            raise ValueError('invalid SSH username')
        if not re.fullmatch('[a-zA-Z0-9_-]{1,48}', repo_dir):
            raise ValueError('remote checkout must be one simple directory name')
        self.username, self.checkout, self.local_ips, self.repo_dir = username, checkout, local_ips, repo_dir

    def is_local(self, node):
        return node['ip'] in self.local_ips

    def shell(self, node, script, *, input_text=None, timeout=180, check=True):
        if self.is_local(node):
            argv = ['bash', '-lc', script]
        else:
            dest = self.username + '@' + node['ip']
            argv = ['ssh', *SSH_OPTS, dest, 'bash -lc ' + shlex.quote(script)]
        p = subprocess.run(argv, input=input_text, text=True, capture_output=True, timeout=timeout)
        if check and p.returncode:
            raise RuntimeError(f'{node["id"]}: command failed ({p.returncode})\n{(p.stdout+p.stderr)[-2000:]}')
        return p.stdout

    def agent(self, node, *, cfg, job, role, action, timeout=20):
        command = ('cd "$HOME/' + self.repo_dir + '" && python3 tools/apt/node_agent.py'
                   ' --config ' + shlex.quote('results/automation/configs/' + cfg + '.json')
                   + ' --job ' + shlex.quote(job) + ' --role ' + shlex.quote(role)
                   + ' ' + shlex.quote(action))
        raw = self.shell(node, command, timeout=timeout)
        return json.loads(raw)

    def sync_config(self, node, cfg, json_text):
        remote = '$HOME/' + self.repo_dir + '/results/automation/configs/' + cfg + '.json'
        self.shell(node, 'mkdir -p "$HOME/' + self.repo_dir + '/results/automation/configs" && cat > "' + remote + '"',
                   input_text=json_text, timeout=15)

    def bootstrap(self, node, commit, ib_device, ib_port, install_deps=False):
        script = ('set -euo pipefail; '
                  'if [ ! -d "$HOME/' + self.repo_dir + '/.git" ]; then '
                  'git clone ' + shlex.quote(REPO_URL) + ' "$HOME/' + self.repo_dir + '"; fi; '
                  'cd "$HOME/' + self.repo_dir + '"; '
                  'git fetch --quiet origin main; '
                  'if [ "$(git rev-parse HEAD)" != ' + shlex.quote(commit) + ' ]; then git checkout --detach ' + shlex.quote(commit) + '; fi; '
                  'test "$(git rev-parse HEAD)" = ' + shlex.quote(commit) + '; '
                  + ('sudo -n bash tools/apt/setup.sh; ' if install_deps else '') +
                  'bash tools/apt/build.sh; bash tools/apt/check.sh '
                  + shlex.quote(ib_device) + ' ' + str(ib_port))
        return self.shell(node, script, timeout=360)


def local_ip_set():
    try:
        x = subprocess.run(['ip', '-o', '-4', 'addr', 'show'], capture_output=True, text=True, check=True)
        return set(re.findall(r'\binet\s+([0-9.]+)/', x.stdout))
    except (FileNotFoundError, subprocess.CalledProcessError):
        return set()


def git_commit():
    def cmd(*args):
        return subprocess.check_output(['git', '-C', str(ROOT), *args], text=True).strip()
    sha = cmd('rev-parse', 'HEAD')
    if cmd('status', '--porcelain', '--untracked-files=no'):
        raise RuntimeError('controller has tracked local edits; commit or discard them first')
    return sha


def wait_for(ex, node, cfg, job, role, *, ready=False, timeout=40):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        s = ex.agent(node, cfg=cfg, job=job, role=role, action='status')
        if ready and s['ready'] and s['alive']:
            return s
        if not ready and s['finished']:
            if s['returncode'] != 0:
                raise RuntimeError(f'{role} exited {s["returncode"]}: {s["log_tail"][-900:]}')
            return s
        if ready and (s['finished'] or not s['alive']):
            raise RuntimeError(f'{role} failed to become ready: {s["log_tail"][-900:]}')
        time.sleep(0.35)
    raise TimeoutError(f'timeout waiting for {role} ({job})')


def benchmark(ex, case, topology, commit, session, run_root, ballot, timeout):
    job = session + '-' + case['id']
    cfg_name = job
    doc = case_topology(topology, case, ballot)
    machines = doc['replicas'] + [doc['data_mn']]
    if case['kind'] == 'prepared_e2e':
        machines += doc['clients']
    conf = json.dumps(doc, indent=2) + '\n'
    for node in machines:
        ex.sync_config(node, cfg_name, conf)
    jobs = []
    case_dir = run_root / case['id']
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / 'config.json').write_text(conf)
    try:
        def start(node, role, ready):
            ex.agent(node, cfg=cfg_name, job=job + '-' + role, role=role, action='start')
            jobs.append((node, role, job + '-' + role))
            if ready:
                wait_for(ex, node, cfg_name, job + '-' + role, role, ready=True, timeout=25)
        for replica in doc['replicas']:
            start(replica, replica['id'], True)
        if case['kind'] == 'prepared_e2e':
            start(doc['data_mn'], doc['data_mn']['id'], True)
            # Check from first CN, never on the data MN itself.
            first = doc['clients'][0]
            ex.agent(first, cfg=cfg_name, job=job+'-probe', role=first['id'], action='probe')
            start(doc['data_mn'], 'coordinator', True)
            for cn in doc['clients']:
                start(cn, cn['id'], False)
            wait_for(ex, doc['data_mn'], cfg_name, job+'-coordinator', 'coordinator', timeout=timeout)
            for cn in doc['clients']:
                wait_for(ex, cn, cfg_name, job+'-'+cn['id'], cn['id'], timeout=timeout)
            primary = 'coordinator'
        else:
            start(doc['data_mn'], 'ordering', False)
            wait_for(ex, doc['data_mn'], cfg_name, job+'-ordering', 'ordering', timeout=timeout)
            primary = 'ordering'
        results = {}
        for node, role, jid in jobs:
            data = ex.agent(node, cfg=cfg_name, job=jid, role=role, action='logs', timeout=35)
            results[role] = data
            destination = case_dir / role
            destination.mkdir(exist_ok=True)
            for filename, content in data['files'].items():
                out = destination / filename
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text(content)
        summary = parse_summary(case['kind'], results[primary]['files']['stdout.log'])
        if case['kind'] == 'prepared_e2e':
            if int(summary['positions']) != case['positions'] or int(summary['workers']) != sum(x['workers'] for x in doc['clients']):
                raise ValueError('result does not match requested case')
        elif int(summary['committed']) != case['positions'] or int(summary['batch']) != case['batch']:
            raise ValueError('ordering result does not match requested case')
        row = {key: '' for key in COLUMNS}
        row.update(case=case['id'], trial=case['trial'], kind=case['kind'],
                   measurement_scope='precommitted_handoff' if case['kind']=='prepared_e2e' else 'single_driver_ordering',
                   comparable_to_declock='NO', commit=commit,
                   cn_nodes=case['cn_count'],worker_processes=case['cn_count']*case['workers_per_cn'],
                   positions=case['positions'],batch=case['batch'],probe=case['probe'],work_ns=case['work_ns'],status='PASS')
        if case['kind'] == 'prepared_e2e':
            for f in ('completed', 'verified_markers', 'violations', 'ordering_seconds',
                      'handoff_seconds', 'goodput_ops_per_sec', 'including_ordering_ops_per_sec'):
                row[f] = summary[f]
        else:
            row['completed'] = summary['committed']
            row['ordering_seconds'] = summary['seconds']
            row['ordered_entries_per_sec'] = summary['ordered_entries_per_sec']
        return row
    finally:
        # Never pkill by a program name; only stop jobs started by this session.
        # Collect logs even on error when available.
        for node, role, jid in reversed(jobs):
            try:
                data = ex.agent(node, cfg=cfg_name, job=jid, role=role, action='logs', timeout=25)
                destination = case_dir / role
                destination.mkdir(exist_ok=True)
                for filename, content in data['files'].items():
                    out = destination / filename
                    out.parent.mkdir(parents=True, exist_ok=True)
                    out.write_text(content)
            except Exception as exc:
                (case_dir / ('collect_error_'+role+'.txt')).write_text(str(exc))
            try:
                ex.agent(node, cfg=cfg_name, job=jid, role=role, action='stop', timeout=15)
            except Exception as exc:
                (case_dir / ('cleanup_error_'+role+'.txt')).write_text(str(exc))


def summarize(rows, path):
    groups = {}
    for row in rows:
        if row['status'] != 'PASS':
            continue
        field = 'goodput_ops_per_sec' if row['kind']=='prepared_e2e' else 'ordered_entries_per_sec'
        key = (row['kind'],row['cn_nodes'],row['worker_processes'],row['positions'],row['batch'],row['probe'],row['work_ns'])
        groups.setdefault(key, []).append(float(row[field]))
    with path.open('w',newline='') as fp:
        w = csv.writer(fp)
        w.writerow(['kind','cn_nodes','workers','positions','batch','probe','work_ns','trials','median_ops_per_sec','min_ops_per_sec','max_ops_per_sec','comparable_to_declock'])
        for key, values in sorted(groups.items()):
            w.writerow([*key,len(values),f'{statistics.median(values):.2f}',
                        f'{min(values):.2f}',f'{max(values):.2f}','NO'])


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--topology', type=Path, default=HERE/'topology.json')
    ap.add_argument('--suite', type=Path, default=HERE/'suites/smoke.json')
    ap.add_argument('--ssh-user', default=os.environ.get('USER', ''))
    ap.add_argument('--remote-dir', default=WORKDIR)
    ap.add_argument('--timeout', type=int, default=180)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--bootstrap-only', action='store_true')
    ap.add_argument('--install-deps', action='store_true', help='run Apt package setup on nodes using passwordless sudo')
    ap.add_argument('--max-cases', type=int, default=0)
    args = ap.parse_args()
    topo, _ = cluster.read(args.topology)
    cases = expand_suite(json.loads(args.suite.read_text()), topo)
    if args.max_cases:
        cases=cases[:args.max_cases]
    machines = topo['replicas']+[topo['data_mn']]+topo['clients']
    print(f'Configured {len(cases)} independent trials on {len(machines)} physical nodes; CNs never share MNs.',flush=True)
    for c in cases:
        print(f'  {c["id"]} {c["kind"]} positions={c["positions"]} batch={c["batch"]} '
              f'CN={c["cn_count"]} workers/CN={c["workers_per_cn"]} probe={c["probe"]}',flush=True)
    print('WARNING: fixed-batch prepared handoff and single-driver ordering only. NO apples-to-apples DecLock curves.',flush=True)
    if args.dry_run:
        return 0
    if args.timeout<30 or args.timeout>3600:
        raise ValueError('timeout must be 30..3600 seconds')
    sha = git_commit()
    ips = local_ip_set()
    if not ips.intersection(n['ip'] for n in machines):
        raise RuntimeError('run from a configured CloudLab node (experiment IPv4 not detected)')
    executor = Executor(username=args.ssh_user, checkout=ROOT, local_ips=ips,repo_dir=args.remote_dir)
    needed_ids = {x['id'] for x in topo['replicas']+[topo['data_mn']]}
    for c in cases:
        if c['kind']=='prepared_e2e':
            needed_ids.update(x['id'] for x in topo['clients'][:c['cn_count']])
    needed = [n for n in machines if n['id'] in needed_ids]
    lock_file = ROOT/'results'/'automation'/'runner.lock'
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = lock_file.open('w')
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError('another automation suite is already active on this controller')
    print('Checking SSH and building exact commit',sha,flush=True)
    for node in needed:
        executor.shell(node, 'hostname && id -un', timeout=18)
        out = executor.bootstrap(node, sha, topo['device'], topo['ib_port'], install_deps=args.install_deps)
        print(f'  {node["id"]}: build and verbs check OK ({len(out)} chars)',flush=True)
    if args.bootstrap_only:
        return 0
    session = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + f'-{os.getpid()}'
    root = ROOT/'results'/'automation'/session
    root.mkdir(parents=True)
    (root/'manifest.json').write_text(json.dumps({'commit':sha,'topology':topo,'suite':str(args.suite),
                                                  'cases':cases,'measurement_scope':'NOT_COMPARABLE_TO_DECLOCK'},indent=2))
    summary_csv = root/'runs.csv'
    rows = []
    with summary_csv.open('w',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=COLUMNS)
        writer.writeheader()
        for idx,case in enumerate(cases):
            print(f'[{idx+1}/{len(cases)}] {case["id"]} ...',flush=True)
            try:
                row=benchmark(executor,case,topo,sha,session,root,
                              topo['benchmark']['ballot']+idx+1,args.timeout)
                print('  PASS:',row['goodput_ops_per_sec'] or row['ordered_entries_per_sec'],'ops/s',flush=True)
            except Exception as exc:
                row={k:'' for k in COLUMNS}
                row.update(case=case['id'],trial=case['trial'],kind=case['kind'],
                           measurement_scope='failed',comparable_to_declock='NO',commit=sha,
                           positions=case['positions'],batch=case['batch'],status='FAIL')
                (root/case['id']/'failure.txt').parent.mkdir(parents=True,exist_ok=True)
                (root/case['id']/'failure.txt').write_text(repr(exc)+'\n')
                print('  FAILED:',exc,flush=True)
            rows.append(row)
            writer.writerow(row)
            f.flush()
    summarize(rows,root/'summary.csv')
    print('\nRaw logs:',root, '\nRun records:',summary_csv,'\nGrouped medians:',root/'summary.csv',flush=True)
    text = (root/'summary.csv').read_text()
    print('\nSummary (CSV):\n' + '\n'.join(text.splitlines()[:25]),flush=True)
    if len(text.splitlines())>25:
        print('... more rows in summary.csv',flush=True)
    if any(r['status']!='PASS' for r in rows):
        raise RuntimeError('one or more benchmark trials failed (see failure.txt and logs)')
    return 0


if __name__=='__main__':
    try:
        sys.exit(main())
    except (ValueError,RuntimeError,OSError,subprocess.CalledProcessError,TimeoutError) as exc:
        print('ERROR:',exc,file=sys.stderr)
        sys.exit(2)
