#!/usr/bin/env python3
"""Generate an auditable DecLock workload manifest, not fabricated measurements.

Only generates files. DecLock's native build, SSH deployment, 2-second run,
per-CN log collection, and health checks must succeed independently on CloudLab.
Run one command at a time; never overlap benchmark services.
"""
import argparse
import getpass
import itertools
import json
import re
import shlex
from pathlib import Path

import isolated_cluster as cluster

PINNED_DECLOCK_SHA = 'f9d06bf99cbfd8e8fc258bb985545e6d1d15ad7f'
SYSTEMS = ('dmlock', 'dmlock_tf')
HERE = Path(__file__).resolve().parent


def command(system, workers_per_cn, locks, data_ops):
    """Native upstream scripts/run-on-all.sh microbench argument contract."""
    if system not in SYSTEMS:
        raise ValueError('unknown DecLock system')
    for label, x, lower, upper in (
        ('workers_per_cn', workers_per_cn, 1, 16),
        ('locks', locks, 1, 10000000),
        ('data_ops', data_ops, 1, 32)):
        if type(x) is not int or x < lower or x > upper:
            raise ValueError(f'{label} must be {lower}..{upper}')
    # From upstream: bench, zipf, read_ratio, CN count, core count,
    # coroutines/core, locks, locality, data bytes, ops/critical-section,
    # grant-count, queue capacity, MN count. This is 0%-read exclusive-only.
    return [
        'bash', 'scripts/run-on-all.sh', system+'_microbench',
        '0.99', '0', '2', str(workers_per_cn), '1', str(locks),
        '0', '8', str(data_ops), '1', '0', '1'
    ]


def build_plan(topology, declock_dir, systems, workers, locks, data_ops, repetitions):
    cluster.validate(topology)
    if type(repetitions) is not int or not 1 <= repetitions <= 20:
        raise ValueError('repetitions must be 1..20')
    if len(topology['clients']) < 2:
        raise ValueError('two dedicated CNs required')
    if any(w > min(topology['clients'][i].get('cores',8) for i in range(2))
           for w in workers):
        raise ValueError('worker count exceeds CloudLab CN core budget')
    if len({topology['data_mn']['ip'], topology['clients'][0]['ip'],
            topology['clients'][1]['ip']}) != 3:
        raise ValueError('MN4 and CN nodes must be physically distinct')
    plan=[]
    for system,w,n,shot in itertools.product(systems,workers,locks,data_ops):
        argv=command(system,w,n,shot)
        for trial in range(1,repetitions+1):
            plan.append({
                'id':f'{system}-cn2-w{w}-locks{n}-dataops{shot}-t{trial}',
                'artifact_commit':PINNED_DECLOCK_SHA,
                'system':system,
                'cn_nodes':2,'workers_per_cn':w,'active_clients':2*w,
                'mn_nodes':1,'data_mn':topology['data_mn']['id'],
                'data_mn_ip':topology['data_mn']['ip'],
                'locks':n,'distribution':'zipf_0.99',
                'read_ratio':0,'locality':0,'data_size_bytes':8,
                'data_ops':shot,'coroutines_per_core':1,
                'test_time_ms':2000,'warmup_ms':0,
                'trial':trial,'argv':argv,'cwd':str(declock_dir),
                'measurement_status':'NOT_RUN',
            })
    return plan


def host_environment(topology, declock_dir, username):
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,32}',username):
        raise ValueError('invalid SSH username')
    # run-on-all.sh uses HOSTS in MN-first, CN-second order. The baseline
    # runs on MN4 only; MuCQL's quorum MNs are not falsely counted as DecLock MNs.
    hosts=[topology['data_mn'],*topology['clients'][:2]]
    listing=' '.join(f'{node["ip"]}-{node["ip"]}-0' for node in hosts)
    variables={
        'USERNAME':username,
        'DMLOCK_PATH':str(declock_dir),
        'DMLOCK_LOG_PATH':str(declock_dir/'log'),
        'SHERMAN_PATH':str(declock_dir/'app'/'sherman'),
        'RESULT_PATH':str(declock_dir/'results'),
        'HOSTS':listing,
        'MASTER_NODE':topology['data_mn']['ip'],
        'REPORT_PATH':str(declock_dir/'results'),
    }
    return '# Generated baseline configuration; copy to ~/set-env.sh on MN4 and both CNs.\n' + ''.join(
        f'export {name}={shlex.quote(value)}\n' for name,value in variables.items())


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--topology',type=Path,default=HERE/'topology.json')
    p.add_argument('--declock-dir',type=Path,default=Path.home()/'declock')
    p.add_argument('--out',type=Path,default=Path('results/declock_match_plan'))
    p.add_argument('--username',default=getpass.getuser())
    p.add_argument('--system',action='append',choices=SYSTEMS,default=None)
    p.add_argument('--workers-per-cn',action='append',type=int,default=None)
    p.add_argument('--locks',action='append',type=int,default=None)
    p.add_argument('--data-ops',action='append',type=int,default=None)
    p.add_argument('--repetitions',type=int,default=5)
    a=p.parse_args()
    topo,_=cluster.read(a.topology)
    directory=a.declock_dir.expanduser().resolve()
    cases=build_plan(topo,directory,a.system or list(SYSTEMS),
                     a.workers_per_cn or [8],a.locks or [1],
                     a.data_ops or [1,32],a.repetitions)
    out=a.out.expanduser().resolve()
    out.mkdir(parents=True,exist_ok=True)
    env=host_environment(topo,directory,a.username)
    (out/'set-env.sh').write_text(env)
    (out/'cases.json').write_text(json.dumps({
        'decLock_repository':'https://github.com/dracit7/declock',
        'pin':PINNED_DECLOCK_SHA,
        'notes':['Requires external DecLock repo and prerequisites installed on MN4/CN1/CN2',
                 'No RDMA benchmark is launched by this generator',
                 'Do not compare request throughput (attempt count); use successful completed cycles',
                 'DecLock requires separate per-CN logs and checked 2-second completion',
                 'MuCQL uses three additional Paxos MNs, DecLock uses one MN',
                 'DecLock and MuCQL Zipf generators differ despite alpha 0.99'],
        'cases':cases,
    },indent=2)+'\n')
    (out/'commands.txt').write_text(''.join(
        f'# {c["id"]} | assert git HEAD = {c["artifact_commit"]}; '
        f'collect and validate CN logs before next trial\n'+
        'cd '+shlex.quote(c['cwd'])+' && '+
        shlex.join(c['argv'])+'\n' for c in cases))
    print(f'Generated {len(cases)} DecLock run commands in {out}')
    print('NO DecLock jobs were executed or measured.')
    print(f'Install {out/"set-env.sh"} as ~/set-env.sh on MN4 and both CNs')
    print('Use the matching content as DecLock scripts/set-env.sh on MN4.')
    print('Pin upstream SHA and apply tools/patch_declock_benchmark.py before rebuilding.')
    print('Inspect commands.txt; execute one baseline at a time, retaining raw CN logs.')
    print('Never label this manifest as observed DecLock throughput.')


if __name__=='__main__':
    main()
