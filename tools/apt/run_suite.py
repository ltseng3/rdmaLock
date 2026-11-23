#!/usr/bin/env python3
"""One-command CloudLab deployment, isolated benchmark sweeps and CSV collection.

Supports single-driver ordering, precommitted handoff and live CN-admitted
MuCQL locking. Native DecLock execution is explicitly NOT implemented.
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
import random
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
from profiles import profile, ABLATIONS

SSH_OPTS = ['-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=accept-new',
            '-o', 'ConnectTimeout=8', '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3']
WORKDIR = 'rdmaLock'
REPO_URL = 'https://github.com/ltseng3/rdmaLock.git'
SUPPORTED = {'ordering_micro', 'prepared_e2e', 'live_e2e'}
COLUMNS = ['case', 'scenario', 'trial', 'kind', 'measurement_scope', 'comparable_to_declock',
           'commit', 'cn_nodes', 'worker_processes', 'positions', 'batch', 'probe',
           'work_ns', 'data_ops', 'rx_idle_us', 'handoff_idle_us', 'handoff_preconnect',
           'data_size_bytes', 'read_ratio', 'data_writes_measured',
           'completed', 'verified_markers', 'violations',
           'ordering_seconds', 'handoff_seconds', 'goodput_ops_per_sec',
           'including_ordering_ops_per_sec', 'ordered_entries_per_sec', 'status',
           'profile', 'transport', 'lock_count', 'distribution', 'warmup_ms', 'measure_ms', 'seed',
           'paxos_slots', 'max_ops', 'join_aggregate', 'phase2_window', 'prepare_range',
           'lookahead', 'replenish_threshold', 'batch_wait_us', 'measured', 'join_batches',
           'actual_join_batch_mean', 'prepare_records', 'latency_p50_ns', 'latency_p95_ns',
           'latency_p99_ns', 'admission_p50_ns', 'admission_p99_ns', 'handoff_wait_p99_ns', 'error']


def require_int(value, lo, hi, name):
    if type(value) is not int or not lo <= value <= hi:
        raise ValueError(f'{name} must be integer {lo}..{hi}')
    return value


def expand_live(spec, topology, suite_seed, start_id):
    """Expand a bounded LIVE factorial design; never synthesize CN joins."""
    name = spec['name']
    repetitions = require_int(spec.get('repetitions', 1), 1, 20, 'repetitions')
    def axis(key, default, lo, hi):
        values = spec.get(key, [default])
        if not isinstance(values, list) or not values:
            raise ValueError(f'{name}.{key} must be a nonempty list')
        return [require_int(x, lo, hi, f'{name}.{key}') for x in values]
    profiles = spec.get('profiles', ['raw'])
    if not isinstance(profiles, list) or not profiles:
        raise ValueError('LIVE profiles must be a nonempty array')
    for p in profiles:
        profile(p)
    transports=spec.get('transports',['rdma'])
    if not isinstance(transports,list) or not transports or any(t not in ('rdma','tcp') for t in transports):
        raise ValueError('LIVE transports must contain rdma or tcp')
    cn_values = spec.get('cn_counts', [len(topology['clients'])])
    if cn_values == 'powers_of_two':
        cn_values = [2**i for i in range(1, 8) if 2**i <= len(topology['clients'])]
    if not cn_values:
        raise ValueError('LIVE needs at least two dedicated CNs')
    cn_counts = [require_int(x, 2, len(topology['clients']), 'cn_counts') for x in cn_values]
    workers = axis('workers_per_cn', 1, 1, 16)
    locks = axis('locks', 1, 1, 4096)
    probes = axis('probes', 1, 0, 1)
    work = axis('work_ns', 0, 0, 1000000000)
    data_ops = axis('data_ops', 1, 1, 32)
    rx_idle = axis('rx_idle_us', 0, 0, 100)
    handoff_idle = axis('handoff_idle_us', 0, 0, 100)
    preconnect = axis('handoff_preconnect', 1, 0, 1)
    distributions = spec.get('distributions', ['uniform'])
    if not isinstance(distributions, list) or not distributions or any(x not in ('uniform', 'zipf_0.99') for x in distributions):
        raise ValueError('LIVE distributions must contain uniform or zipf_0.99')
    warmup = require_int(spec.get('warmup_ms', 2000), 0, 60000, 'warmup_ms')
    measurement = require_int(spec.get('measure_ms', 10000), 50, 120000, 'measure_ms')
    slots = require_int(spec.get('paxos_slots', 1000000), 4096, 1000000, 'paxos_slots')
    max_ops = require_int(spec.get('max_ops', 900000), 2, 1000000, 'max_ops')
    seed = require_int(spec.get('seed', suite_seed), 1, 2**63-1, 'seed')
    if max_ops > slots - slots//64:
        raise ValueError('LIVE max_ops must leave safety headroom in consensus log')
    cases=[]
    combinations = itertools.product(profiles, transports, cn_counts, workers, locks,
                                     distributions, probes, work, data_ops,
                                     rx_idle, handoff_idle, preconnect)
    for pname, transport, cn, wpc, lock_count, distribution, probe, ns, shots, rx, hi, pc in combinations:
        if cn*wpc>64 or any(wpc>n.get('cores', 8) for n in topology['clients'][:cn]):
            raise ValueError('LIVE workers exceed core budget or 64 worker processes')
        for trial in range(1, repetitions+1):
            case=dict(name=name,kind='live_e2e',trial=trial,profile=pname,transport=transport,
                cn_count=cn,workers_per_cn=wpc,locks=lock_count,distribution=distribution,
                probe=probe,work_ns=ns,data_ops=shots,rx_idle_us=rx,
                handoff_idle_us=hi,handoff_preconnect=pc,
                seed=seed,warmup_ms=warmup,measure_ms=measurement,
                paxos_slots=slots,max_ops=max_ops,positions=topology['benchmark']['positions'],batch=1)
            case['id']=f'{start_id+len(cases):04d}-{name}-{pname}-{transport}-d{shots}-rx{rx}-hi{hi}-pc{pc}-t{trial}'
            cases.append(case)
    return cases


def expand_suite(suite, topology):
    if not isinstance(suite, dict) or not isinstance(suite.get('tests'), list) or not suite['tests']:
        raise ValueError('suite must contain nonempty tests array')
    result = []
    for spec in suite['tests']:
        name, kind = spec.get('name'), spec.get('kind')
        if not isinstance(name, str) or not re.fullmatch('[a-z][a-z0-9_-]{0,35}', name):
            raise ValueError('test name must be a simple lowercase identifier')
        if kind not in SUPPORTED:
            raise ValueError(f'{name}: unsupported kind {kind!r}; DecLock backend is NOT implemented')
        if kind == 'live_e2e':
            result.extend(expand_live(spec, topology, suite.get('seed', 2026), len(result)+1))
            continue
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
    if suite.get('shuffle', False):
        random.Random(suite.get('seed', 2026)).shuffle(result)
    return result


def case_topology(original, case, ballot):
    doc = copy.deepcopy(original)
    if case['kind'] in ('prepared_e2e', 'live_e2e'):
        doc['clients'] = doc['clients'][:case['cn_count']]
        for cn in doc['clients']:
            cn['workers'] = case['workers_per_cn']
    # Dedicated automation ports avoid older manually launched 29660/29670 services.
    doc['ports'] = {'ordering':39660,'data':39670,'coordinator':39700,'worker_base':39711}
    doc['benchmark'].update(positions=case['positions'], ballot=ballot,
                            order_batch=case['batch'], probe=case['probe'],
                            work_ns=case['work_ns'])
    if case['kind'] == 'live_e2e':
        p = profile(case['profile'])
        doc['benchmark'].update(mode='live', cn_transport=case['transport'], **p, paxos_slots=case['paxos_slots'],
            max_ops=case['max_ops'], warmup_ms=case['warmup_ms'],
            measure_ms=case['measure_ms'], locks=case['locks'], data_ops=case['data_ops'],
            rx_idle_us=case['rx_idle_us'],handoff_idle_us=case['handoff_idle_us'],
            handoff_preconnect=case['handoff_preconnect'],
            zipf=int(case['distribution']=='zipf_0.99'), seed=case['seed'])
    cluster.validate(doc)
    return doc


def extract_live(text):
    lines=[line for line in text.splitlines() if line.startswith('LIVE_E2E ')]
    if len(lines)!=1:
        raise ValueError('expected exactly one LIVE_E2E line')
    result=dict(re.findall(r'([a-z0-9_]+)=([^\s]+)',lines[0]))
    needed=('status','scope','workers','locks','warmup_ms','measure_ms','completed','measured',
            'joins','verified_markers','violations','join_aggregate','phase2_window',
            'prepare_range','lookahead','replenish_threshold','batch_wait_us',
            'actual_join_batch_mean','prepare_records','goodput_ops_per_sec',
            'latency_p50_ns','latency_p95_ns','latency_p99_ns',
            'admission_p50_ns','admission_p99_ns','handoff_wait_p99_ns','transport',
            'data_ops','data_size_bytes','read_ratio','data_writes_measured',
            'rx_idle_us','handoff_idle_us','handoff_preconnect')
    if any(key not in result for key in needed):
        raise ValueError('incomplete LIVE_E2E output')
    if result['status']!='PASS' or result['scope']!='live_steady_state':
        raise ValueError('LIVE result is not a PASS steady-state sample')
    if (int(result['completed'])!=int(result['joins']) or
        int(result['verified_markers'])!=int(result['joins']) or
        int(result['measured'])<1 or int(result['violations'])!=0 or
        int(result['data_ops']) not in range(1,33) or
        int(result['data_size_bytes'])!=8 or int(result['read_ratio'])!=0 or
        int(result['rx_idle_us']) not in range(101) or
        int(result['handoff_idle_us']) not in range(101) or
        int(result['handoff_preconnect']) not in (0,1) or
        int(result['data_writes_measured'])!=int(result['measured'])*int(result['data_ops'])):
        raise ValueError('LIVE completion/marker/ownership mismatch')
    for key in ('goodput_ops_per_sec','actual_join_batch_mean'):
        if not math.isfinite(float(result[key])) or float(result[key])<=0:
            raise ValueError('invalid LIVE metric '+key)
    return result


def parse_summary(kind, text):
    if kind == 'live_e2e':
        return extract_live(text)
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

    def bootstrap(self, node, commit, ib_device, ib_port, install_deps=False, auto_deps=True):
        script = ('set -euo pipefail; '
                  'if [ ! -d "$HOME/' + self.repo_dir + '/.git" ]; then '
                  'git clone ' + shlex.quote(REPO_URL) + ' "$HOME/' + self.repo_dir + '"; fi; '
                  'cd "$HOME/' + self.repo_dir + '"; '
                  'git fetch --quiet origin main; '
                  'if [ "$(git rev-parse HEAD)" != ' + shlex.quote(commit) + ' ]; then git checkout --detach ' + shlex.quote(commit) + '; fi; '
                  'test "$(git rev-parse HEAD)" = ' + shlex.quote(commit) + '; '
                  + ('sudo -n bash tools/apt/setup.sh; ' if install_deps else '') +
                  'bash tools/apt/ensure_deps.sh ' + ('auto' if auto_deps else 'check') + '; ' +
                  'bash tools/apt/build.sh; bash tools/apt/check.sh '
                  + shlex.quote(ib_device) + ' ' + str(ib_port))
        return self.shell(node, script, timeout=900 if (install_deps or auto_deps) else 360)


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
    if case['kind'] in ('prepared_e2e', 'live_e2e'):
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
        if case['kind'] in ('prepared_e2e', 'live_e2e'):
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
        elif case['kind'] == 'live_e2e':
            if (int(summary['workers']) != sum(x['workers'] for x in doc['clients']) or
                int(summary['locks']) != case['locks'] or
                int(summary['measure_ms']) != case['measure_ms'] or
                int(summary['warmup_ms']) != case['warmup_ms'] or
                summary['transport'] != case['transport'] or
                int(summary['data_ops']) != case['data_ops'] or
                int(summary['rx_idle_us']) != case['rx_idle_us'] or
                int(summary['handoff_idle_us']) != case['handoff_idle_us'] or
                int(summary['handoff_preconnect']) != case['handoff_preconnect']):
                raise ValueError('LIVE result does not match request')
            for k in ('join_aggregate','phase2_window','prepare_range','lookahead','replenish_threshold','batch_wait_us'):
                if int(summary[k]) != doc['benchmark'][k]:
                    raise ValueError('LIVE reported knob differs: '+k)
        elif int(summary['committed']) != case['positions'] or int(summary['batch']) != case['batch']:
            raise ValueError('ordering result does not match requested case')
        row = {key: '' for key in COLUMNS}
        row.update(case=case['id'], scenario=case['name'],trial=case['trial'], kind=case['kind'],
                   measurement_scope=('precommitted_handoff' if case['kind']=='prepared_e2e' else
                                      'cn_admitted_bounded_live' if case['kind']=='live_e2e' else
                                      'single_driver_ordering'),
                   comparable_to_declock='NO', commit=commit,
                   cn_nodes=case['cn_count'],worker_processes=case['cn_count']*case['workers_per_cn'],
                   positions=case['positions'],batch=case['batch'],probe=case['probe'],work_ns=case['work_ns'],status='PASS')
        if case['kind'] == 'live_e2e':
            row.update(profile=case['profile'],transport=case['transport'],lock_count=case['locks'],distribution=case['distribution'],
                data_ops=case['data_ops'],rx_idle_us=case['rx_idle_us'],
                handoff_idle_us=case['handoff_idle_us'],
                handoff_preconnect=case['handoff_preconnect'],
                data_size_bytes=8, read_ratio=0,
                warmup_ms=case['warmup_ms'],measure_ms=case['measure_ms'],seed=case['seed'],
                paxos_slots=case['paxos_slots'],max_ops=case['max_ops'])
            for k in ('join_aggregate','phase2_window','prepare_range','lookahead','replenish_threshold',
                      'batch_wait_us','measured','join_batches','actual_join_batch_mean','prepare_records',
                      'latency_p50_ns','latency_p95_ns','latency_p99_ns','admission_p50_ns','admission_p99_ns',
                      'handoff_wait_p99_ns','goodput_ops_per_sec','completed','verified_markers','violations',
                       'data_writes_measured'):
                row[k]=summary[k]
        elif case['kind'] == 'prepared_e2e':
            for f in ('completed', 'verified_markers', 'violations', 'ordering_seconds',
                      'handoff_seconds', 'goodput_ops_per_sec', 'including_ordering_ops_per_sec'):
                row[f] = summary[f]
        else:
            row['completed'] = summary['committed']
            row['ordering_seconds'] = summary['seconds']
            row['ordered_entries_per_sec'] = summary['ordered_entries_per_sec']
        return row
    finally:
        # A PASS is invalid if an old listener will collide with the next case.
        # Preserve the original error when a trial already failed.
        failed_before_cleanup = sys.exc_info()[0] is not None
        cleanup_failures = []
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
                cleanup_failures.append(role+': '+str(exc))
        if cleanup_failures and not failed_before_cleanup:
            raise RuntimeError('trial cleanup incomplete; refusing PASS: '+'; '.join(cleanup_failures))


def summarize(rows, path):
    """Never merge prepared handoff, ordering-only and LIVE measurements."""
    groups = {}
    for row in rows:
        if row['status'] != 'PASS':
            continue
        field = 'ordered_entries_per_sec' if row['kind']=='ordering_micro' else 'goodput_ops_per_sec'
        key = (row['kind'], row.get('scenario',''), row.get('profile',''), row.get('transport',''), row['cn_nodes'],
               row['worker_processes'], row['positions'], row['batch'], row['probe'], row['work_ns'],
               row.get('lock_count',''), row.get('distribution',''), row.get('data_ops',''),
                row.get('rx_idle_us',''),row.get('handoff_idle_us',''),
                row.get('handoff_preconnect',''),row.get('data_size_bytes',''), row.get('read_ratio',''), row.get('warmup_ms',''),
               row.get('measure_ms',''), row.get('seed',''))
        groups.setdefault(key, []).append(float(row[field]))
    with path.open('w', newline='') as fp:
        w=csv.writer(fp)
        w.writerow(['kind','scenario','profile','transport','cn_nodes','workers','positions','batch','probe',
                    'work_ns','locks','distribution','data_ops','rx_idle_us','handoff_idle_us',
                    'handoff_preconnect','data_size_bytes','read_ratio','warmup_ms','measure_ms','seed',
                    'trials','median_ops_per_sec','min_ops_per_sec','max_ops_per_sec','comparable_to_declock'])
        for key,values in sorted(groups.items(),key=lambda p:str(p[0])):
            w.writerow([*key,len(values),f'{statistics.median(values):.2f}',
                        f'{min(values):.2f}',f'{max(values):.2f}','NO'])


def write_ablations(rows, path):
    """Paired trial ratios, deterministic bootstrap median CI (not a p-value).

    Controls and treatments must share the SAME measured external workload.
    Missing/failed pairs are reported, never assumed to have zero throughput.
    """
    groups={}
    for row in rows:
        if row['kind']!='live_e2e':
            continue
        key=(row.get('scenario'),row.get('transport'),row.get('cn_nodes'),row.get('worker_processes'),
             row.get('lock_count'),row.get('distribution'),row.get('work_ns'),row.get('data_ops'),
             row.get('rx_idle_us'),row.get('handoff_idle_us'),row.get('handoff_preconnect'),
             row.get('data_size_bytes'),row.get('read_ratio'),row.get('probe'),row.get('warmup_ms'),row.get('measure_ms'),row.get('seed'))
        groups.setdefault(key,{})[(row.get('profile'),row['trial'])]=row
    with path.open('w',newline='') as f:
        writer=csv.writer(f)
        writer.writerow(['scenario','transport','cn_nodes','workers','locks','distribution','work_ns',
            'data_ops','rx_idle_us','handoff_idle_us','handoff_preconnect',
            'data_size_bytes','read_ratio','probe',
            'warmup_ms','measure_ms','seed','effect','control','treatment',
            'matched_trials','control_median_ops','treatment_median_ops',
            'median_speedup','median_gain_pct','bootstrap95_low','bootstrap95_high','status'])
        for key,items in sorted(groups.items(),key=lambda x:str(x[0])):
            for effect,a,b in ABLATIONS:
                trials=sorted({trial for profile_name,trial in items if profile_name in (a,b)})
                if not any(p==a for p,t in items) or not any(p==b for p,t in items):
                    continue
                pairs=[]
                for t in trials:
                    left,right=items.get((a,t)),items.get((b,t))
                    if left and right and left['status']=='PASS' and right['status']=='PASS':
                        ca,cb=float(left['goodput_ops_per_sec']),float(right['goodput_ops_per_sec'])
                        if ca>0 and cb>0:pairs.append((ca,cb))
                status='PASS' if len(pairs)==len(trials) else 'INCOMPLETE'
                if pairs:
                    ratios=[b/a for a,b in pairs]
                    median=statistics.median(ratios)
                    ca=statistics.median(a for a,b in pairs)
                    cb=statistics.median(b for a,b in pairs)
                    low=high=''
                    if len(pairs)>=3:
                        rng=random.Random(2026)
                        sample=[statistics.median(rng.choices(ratios,k=len(ratios))) for _ in range(2000)]
                        sample.sort();low=f'{sample[49]:.4f}';high=f'{sample[1949]:.4f}'
                    values=[f'{ca:.2f}',f'{cb:.2f}',f'{median:.4f}',f'{(median-1)*100:.2f}',low,high]
                else:
                    values=['']*6
                writer.writerow([*key,effect,a,b,len(pairs),*values,status])



def failure_row(case, commit, exc):
    """Preserve the requested configuration and exception for rejected trials."""
    row={key:'' for key in COLUMNS}
    row.update(case=case['id'],scenario=case['name'],trial=case['trial'],
               kind=case['kind'],measurement_scope='failed',
               comparable_to_declock='NO',commit=commit,
               cn_nodes=case['cn_count'],
               worker_processes=case['cn_count']*case['workers_per_cn'],
               positions=case['positions'],batch=case['batch'],
               probe=case['probe'],work_ns=case['work_ns'],status='FAIL',
               error=str(exc).replace('\n',' | ')[:2000])
    if case['kind']=='live_e2e':
        row.update(profile=case['profile'],transport=case['transport'],lock_count=case['locks'],
                   distribution=case['distribution'],data_ops=case['data_ops'],
                   rx_idle_us=case['rx_idle_us'],handoff_idle_us=case['handoff_idle_us'],
                   handoff_preconnect=case['handoff_preconnect'],data_size_bytes=8,read_ratio=0,warmup_ms=case['warmup_ms'],
                   measure_ms=case['measure_ms'],seed=case['seed'],
                   paxos_slots=case['paxos_slots'],max_ops=case['max_ops'],
                   **profile(case['profile']))
    return row


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--topology', type=Path, default=(HERE/'topology.local.json' if (HERE/'topology.local.json').exists() else HERE/'topology.json'))
    ap.add_argument('--suite', type=Path, default=HERE/'suites/smoke.json')
    ap.add_argument('--ssh-user', default=os.environ.get('USER', ''))
    ap.add_argument('--remote-dir', default=WORKDIR)
    ap.add_argument('--timeout', type=int, default=180)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--bootstrap-only', action='store_true')
    deps = ap.add_mutually_exclusive_group()
    deps.add_argument('--install-deps', action='store_true', help='force full Apt/RDMA tools setup on nodes (passwordless sudo)')
    deps.add_argument('--no-auto-deps', action='store_true', help='check prerequisites only; never install packages')
    ap.add_argument('--max-cases', type=int, default=0)
    ap.add_argument('--fail-fast', action='store_true', help='stop at first failed trial; retain collected logs and CSV')
    args = ap.parse_args()
    topo, _ = cluster.read(args.topology)
    cases = expand_suite(json.loads(args.suite.read_text()), topo)
    if args.max_cases:
        cases=cases[:args.max_cases]
    machines = topo['replicas']+[topo['data_mn']]+topo['clients']
    print(f'Configured {len(cases)} independent trials on {len(machines)} physical nodes; CNs never share MNs.',flush=True)
    for c in cases:
        print(f'  {c["id"]} {c["kind"]} positions={c["positions"]} batch={c["batch"]} '
              f'CN={c["cn_count"]} workers/CN={c["workers_per_cn"]} probe={c["probe"]}'
              f' profile={c.get("profile", "-")} transport={c.get("transport", "-")}'
              f' locks={c.get("locks", "-")} data_ops={c.get("data_ops","-")}'
              f' rx_idle_us={c.get("rx_idle_us","-")}'
              f' handoff_idle_us={c.get("handoff_idle_us","-")}'
              f' preconnect={c.get("handoff_preconnect","-")}',flush=True)
    print('Measurement families: live CN-admitted MuCQL, prepared-handoff, or ordering-only. '
          'DecLock adapter not yet available; do not present these as head-to-head curves.',flush=True)
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
        if c['kind'] in ('prepared_e2e','live_e2e'):
            needed_ids.update(x['id'] for x in topo['clients'][:c['cn_count']])
    needed = [n for n in machines if n['id'] in needed_ids]
    lock_file = ROOT/'results'/'automation'/'runner.lock'
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = lock_file.open('w')
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError('another automation suite is already active on this controller')
    session = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + f'-{os.getpid()}'
    root = ROOT/'results'/'automation'/session
    (root/'bootstrap').mkdir(parents=True)
    print('Checking SSH and building exact commit',sha,flush=True)
    for node in needed:
        print(f'  {node["id"]}: checking prerequisites and building ...', flush=True)
        try:
            executor.shell(node, 'hostname && id -un', timeout=18)
            out = executor.bootstrap(node, sha, topo['device'], topo['ib_port'],
                                     install_deps=args.install_deps, auto_deps=not args.no_auto_deps)
        except Exception as exc:
            (root/'bootstrap'/f'{node["id"]}.error.txt').write_text(str(exc)+'\n')
            raise
        (root/'bootstrap'/f'{node["id"]}.log').write_text(out)
        print(f'  {node["id"]}: build and verbs check OK ({len(out)} chars)',flush=True)
    if args.bootstrap_only:
        return 0
    (root/'manifest.json').write_text(json.dumps({'commit':sha,'topology':topo,'suite':str(args.suite),
                                                  'cases':cases,'measurement_scope':'NO_DECLOCK_BASELINE_YET'},indent=2))
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
                row=failure_row(case,sha,exc)
                (root/case['id']/'failure.txt').parent.mkdir(parents=True,exist_ok=True)
                (root/case['id']/'failure.txt').write_text(repr(exc)+'\n')
                print('  FAILED:',exc,flush=True)
            rows.append(row)
            writer.writerow(row)
            f.flush()
            if args.fail_fast and row['status']!='PASS':
                print('Stopping after first failure (--fail-fast). '
                      'Inspect the case failure.txt and node logs before a larger sweep.', flush=True)
                break
    summarize(rows,root/'summary.csv')
    write_ablations(rows,root/'ablations.csv')
    print('\nRaw logs:',root, '\nRun records:',summary_csv,'\nGrouped medians:',root/'summary.csv',
          '\nPaired ablations:',root/'ablations.csv',flush=True)
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
