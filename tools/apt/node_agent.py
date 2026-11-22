#!/usr/bin/env python3
"""Node-local, topology-checked benchmark process supervisor (stdlib only)."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
import isolated_cluster as cluster
import process_lifecycle as lifecycle


def location(job):
    if not job or any(ch not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.' for ch in job) or '..' in job:
        raise ValueError('invalid job ID')
    return ROOT / 'results' / 'automation' / 'jobs' / job


def running(pid):
    try:
        info = (Path('/proc') / str(pid) / 'stat').read_text()
        state = info.rsplit(')', 1)[1].strip().split()[0]
        return state not in ('Z', 'X')
    except (FileNotFoundError, ProcessLookupError):
        return False


def pid_start(pid):
    try:
        return (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].strip().split()[19]
    except (FileNotFoundError, ProcessLookupError):
        return None


def load_state(path):
    p = path / 'state.json'
    return json.loads(p.read_text()) if p.exists() else {}


def active(state):
    pid = state.get('pid')
    return bool(pid and running(pid) and pid_start(pid) == state.get('start_ticks'))


def snapshot(path):
    state = load_state(path)
    finished = path / 'exit.json'
    exit_info = json.loads(finished.read_text()) if finished.exists() else None
    content = (path / 'stdout.log').read_text(errors='replace') if (path / 'stdout.log').exists() else ''
    role = state.get('role')
    token = state.get('ready_token')
    ready = (token in content if token else active(state))
    return {'role': role, 'alive': active(state), 'ready': ready,
            'finished': exit_info is not None, 'returncode': exit_info.get('returncode') if exit_info else None,
            'log_tail': content[-2000:]}


def check_role(doc, role):
    if role in ('coordinator', 'ordering'):
        cluster.check_host(doc['data_mn']['ip'])
    else:
        nodes = cluster.role_nodes(doc)
        if role not in nodes:
            raise ValueError('invalid role')
        cluster.check_host(nodes[role]['ip'])


def supervise(path, config, role):
    doc, _ = cluster.read(config)
    check_role(doc, role)
    command = [sys.executable, str(HERE / 'isolated_cluster.py'), '--config', str(config),
               'run', role, '--log-dir', str(path / 'workers')]
    env = dict(os.environ)
    env['MUCQL_IB_PORT'] = str(doc['ib_port'])
    try:
        rc = subprocess.run(command, cwd=ROOT, env=env, check=False).returncode
    except Exception as exc:
        print('AGENT ERROR:', repr(exc), flush=True)
        rc = 125
    (path / 'exit.json').write_text(json.dumps({'returncode': rc, 'completed_unix': time.time()}))
    return rc


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', required=True, type=Path)
    p.add_argument('--job', required=True)
    p.add_argument('--role', required=True)
    p.add_argument('action', choices=('start', 'supervise', 'status', 'stop', 'logs', 'probe'))
    args = p.parse_args()
    path = location(args.job)
    if args.action == 'supervise':
        return supervise(path, args.config.resolve(), args.role)
    if args.action == 'probe':
        doc, _ = cluster.read(args.config)
        if args.role not in [x['id'] for x in doc['clients']]:
            raise ValueError('RDMA probe must originate from a CN')
        check_role(doc, args.role)
        target = doc['data_mn']['ip']
        exe = ROOT / 'mucql/build-apt/mucql_verbs_release'
        env = dict(os.environ, MUCQL_IB_PORT=str(doc['ib_port']))
        data_port = doc.get('ports', {}).get('data', 29670)
        result = subprocess.run([str(exe), 'check', target, str(data_port), doc['device']],
                                check=True, env=env, capture_output=True, text=True)
        if 'CAS_nonzero=PASS READ_WRITE=PASS' not in result.stdout:
            raise RuntimeError('RDMA preflight did not pass: ' + result.stdout)
        print(json.dumps({'probe': 'PASS', 'output': result.stdout.strip()}))
        return 0
    if args.action == 'start':
        doc, _ = cluster.read(args.config)
        check_role(doc, args.role)
        existing = load_state(path)
        if active(existing) or lifecycle.owned_group_members(existing.get('pid'),args.job):
            raise RuntimeError('job still running: ' + args.job)
        # A prior job must release its listeners before we launch this one.
        lifecycle.wait_ports_free(lifecycle.role_ports(doc,args.role,cluster))
        path.mkdir(parents=True, exist_ok=True)
        for old in ('exit.json', 'state.json'):
            (path / old).unlink(missing_ok=True)
        env=dict(os.environ)
        env[lifecycle.JOB_ENV]=args.job
        with (path / 'stdout.log').open('w') as log:
            proc = subprocess.Popen([sys.executable, str(__file__), '--config', str(args.config.resolve()),
                                     '--job', args.job, '--role', args.role, 'supervise'],
                                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                    start_new_session=True, cwd=ROOT, env=env)
        if args.role in [r['id'] for r in doc['replicas']]:
            ready_token = 'RDMA Paxos acceptor listening TCP'
        elif args.role == doc['data_mn']['id']:
            ready_token = 'RDMA release server IB device active'
        elif args.role == 'coordinator':
            ready_token = ('LIVE coordinator listening tcp=' if doc["benchmark"].get("mode") == "live" else 'lock coordinator listening tcp=')
        else:
            ready_token = None
        (path / 'state.json').write_text(json.dumps({'pid': proc.pid, 'start_ticks': pid_start(proc.pid),
                                                     'role': args.role, 'job': args.job, 'ready_token': ready_token}))
        print(json.dumps({'started': True, 'pid': proc.pid, 'role': args.role}))
        return 0
    if args.action == 'status':
        state = snapshot(path)
        if (state['finished'] and state['returncode'] not in (None,0)
                and ('Address already in use' in state['log_tail']
                     or 'TCP bind/listen' in state['log_tail'])):
            # Startup may race port shutdown after the initial preflight.
            # Preserve the original bind failure while showing the current
            # TCP socket owner/state. Diagnostic only: never kill by port.
            doc, _ = cluster.read(args.config)
            ports = lifecycle.role_ports(doc,args.role,cluster)
            if ports:
                state['log_tail'] = (
                    state['log_tail'][-1300:] + '\nTCP_DIAGNOSTIC: ' +
                    lifecycle.describe_ports(ports)[:400])
        print(json.dumps(state))
        return 0
    if args.action == 'stop':
        doc, _ = cluster.read(args.config)
        check_role(doc,args.role)
        state = load_state(path)
        if state and (state.get('job')!=args.job or state.get('role')!=args.role):
            raise RuntimeError('refusing to stop a mismatched job/role')
        info = lifecycle.stop_owned_group(state.get('pid'),args.job)
        # Do not equate supervisor exit with RDMA listener exit.
        lifecycle.wait_ports_free(lifecycle.role_ports(doc,args.role,cluster))
        print(json.dumps({'stopped': True, **info}))
        return 0
    if args.action == 'logs':
        files = {}
        if path.exists():
            for candidate in sorted(path.rglob('*.log')):
                if candidate.is_file() and candidate.stat().st_size <= 8_000_000:
                    files[str(candidate.relative_to(path))] = candidate.read_text(errors='replace')
        print(json.dumps({'files': files, 'status': snapshot(path)}))
        return 0
    return 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, OSError, subprocess.CalledProcessError) as exc:
        print('ERROR:', exc, file=sys.stderr)
        sys.exit(2)
