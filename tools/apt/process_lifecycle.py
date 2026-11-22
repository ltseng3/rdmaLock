"""Safe Linux job-owned process-group shutdown and TCP listener checks.

We signal only session members tagged with the exact per-trial environment
marker; no port-based killing or wildcard pkill is permitted.
"""
from __future__ import annotations
import os
from pathlib import Path
import signal
import subprocess
import time

JOB_ENV = 'MUCQL_AUTOMATION_JOB'


def listeners():
    """Listen ports for this network namespace (not TIME_WAIT sockets)."""
    result = set()
    for name in ('/proc/net/tcp', '/proc/net/tcp6'):
        try:
            with open(name, encoding='ascii') as f:
                next(f, None)
                for line in f:
                    fields = line.split()
                    if len(fields) >= 4 and fields[3] == '0A':
                        result.add(int(fields[1].rsplit(':', 1)[1], 16))
        except FileNotFoundError:
            continue
    return result


def describe_ports(ports):
    """Diagnostic only; ss is not required for any correctness check."""
    try:
        r = subprocess.run(['ss', '-ltnp'], capture_output=True, text=True,
                           timeout=3, check=False)
        lines = [s for s in r.stdout.splitlines()
                 if any(f':{p} ' in s or s.rstrip().endswith(f':{p}') for p in ports)]
        return '; '.join(lines)[:1200] if lines else 'ss did not identify a listener PID'
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return 'run ss -ltnp on the affected host to identify the listener'


def wait_ports_free(ports, timeout=4.0):
    ports = set(ports)
    if not ports:
        return
    deadline = time.monotonic() + timeout
    while True:
        busy = ports & listeners()
        if not busy:
            return
        if time.monotonic() >= deadline:
            values = ', '.join(str(x) for x in sorted(busy))
            raise RuntimeError(f'TCP LISTEN port(s) still occupied: {values}; '
                               f'{describe_ports(busy)}. '
                               'Refusing to terminate an unowned listener.')
        time.sleep(0.05)


def owned_group_members(pgid, job):
    """Find live children of the exact tagged session even if its leader died."""
    if not isinstance(pgid, int) or pgid < 2 or not isinstance(job, str) or not job:
        return []
    tag = (JOB_ENV + '=' + job).encode()
    found = []
    for entry in os.scandir('/proc'):
        if not entry.name.isdecimal():
            continue
        try:
            stat = (Path(entry.path) / 'stat').read_text()
            fields = stat.rsplit(')', 1)[1].split()
            if len(fields) < 3 or fields[0] in ('Z', 'X') or int(fields[2]) != pgid:
                continue
            env = (Path(entry.path) / 'environ').read_bytes()
            if tag in env.split(b'\0'):
                found.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError, IndexError):
            continue
    return found


def stop_owned_group(pgid, job, grace=2.0, kill_grace=2.0):
    """TERM, then KILL if needed, and wait for every tagged group member."""
    members = owned_group_members(pgid, job)
    if not members:
        return {'members': 0, 'escalated': False}
    for sig, timeout in ((signal.SIGTERM, grace), (signal.SIGKILL, kill_grace)):
        if not owned_group_members(pgid, job):
            return {'members': len(members), 'escalated': sig == signal.SIGKILL}
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not owned_group_members(pgid, job):
                return {'members': len(members), 'escalated': sig == signal.SIGKILL}
            time.sleep(0.05)
    remaining = owned_group_members(pgid, job)
    if remaining:
        raise RuntimeError(f'job {job}: process group {pgid} still alive after SIGKILL: {remaining}')
    return {'members': len(members), 'escalated': True}


def role_ports(doc, role, cluster):
    ports = doc.get('ports', {'ordering': 29660, 'data': 29670,
                              'coordinator': 29700, 'worker_base': 29711})
    if role in [n['id'] for n in doc['replicas']]:
        return [ports['ordering']]
    if role == doc['data_mn']['id']:
        return [ports['data']]
    if role == 'coordinator':
        return [ports['coordinator']]
    if role == 'ordering':
        return []
    return [ports['worker_base'] + i for i in cluster.workers_for(doc, role)]
