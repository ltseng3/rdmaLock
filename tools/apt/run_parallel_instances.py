#!/usr/bin/env python3
"""Compare one MuCQL leader with five independent concurrent MuCQL leaders.

This is a capacity microexperiment, NOT one sharded lock service.  Each instance
owns a disjoint logical lock namespace, an independent Paxos log/QP and data MR.
All instances share the original physical CNs, data MN, replicas and RNICs.

A one-vs-five comparison holds total closed-loop client workers constant:
by default 8 workers/CN (16 total) in the single-leader case, distributed
as [2,2,2,1,1] workers/CN across five independent instances. The 100,000
total keys are partitioned *in proportion to client count*, so that
the global key-request distribution remains uniform in both configurations.
Coordinators on MN4 wait for an absolute Unix-nanosecond GO gate so their
measurement intervals overlap.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import shlex
import statistics
import sys
import time

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[1]
sys.path.insert(0,str(HERE))
import isolated_cluster as cluster
import run_suite as runner

INSTANCE_COUNT=5
TOTAL_KEYS=100000
DEFAULT_PER_CN_WORKERS=8
PORT_STRIDE=200
# Stay below the default Linux ephemeral source-port range (32768+).
# Five shards + one baseline reserve six disjoint 200-port blocks per
# repetition; no port is reused before the whole experiment completes.
PORT_BASE=16560
ROUND_STRIDE=6*PORT_STRIDE
GO_SKEW_LIMIT_NS=100_000_000


def worker_allocation(count,workers_per_cn):
    """Balance physical processes across leaders without oversubscribing CNs."""
    if count not in (1,INSTANCE_COUNT):
        raise ValueError("expected one or five leaders")
    if type(workers_per_cn) is not int or not 5<=workers_per_cn<=16:
        raise ValueError("workers-per-cn must be an integer 5..16")
    q,r=divmod(workers_per_cn,count)
    result=[q+(i<r) for i in range(count)]
    assert min(result)>=1 and sum(result)==workers_per_cn
    return result


def key_allocation(workers):
    """Proportional keys give a globally uniform workload with unequal shards."""
    total=sum(workers)
    if total<=0:
        raise ValueError("no workers")
    allocations=[]
    prefix=0
    for w in workers:
        low=TOTAL_KEYS*prefix//total
        prefix+=w
        allocations.append(TOTAL_KEYS*prefix//total-low)
    assert sum(allocations)==TOTAL_KEYS and min(allocations)>=1
    return allocations


def make_cases(topo, count, repetition, gate,
               workers_per_cn=DEFAULT_PER_CN_WORKERS):
    """One or five independent groups with constant client/key totals."""
    workers=worker_allocation(count,workers_per_cn)
    keys=key_allocation(workers)
    if type(repetition) is not int or not 1<=repetition<=10:
        raise ValueError("repetition must be 1..10")
    if not isinstance(gate,str) or not 1<=len(gate)<=96 or not all(
            x.isascii() and (x.isalnum() or x in "_-") for x in gate):
        raise ValueError("invalid gate token")
    if len(topo['clients'])<2 or any(
            type(cn.get('cores',8)) is not int or
            cn.get('cores',8)<workers_per_cn for cn in topo['clients'][:2]):
        raise ValueError(f"both CNs require {workers_per_cn} usable cores")
    result=[]
    for i in range(count):
        spec={
            'name':'parallel-leaders-'+str(count), 'kind':'live_e2e',
            'profiles':['raw'], 'transports':['rdma'],
            'cn_counts':[2], 'workers_per_cn':[workers[i]],
            'locks':[keys[i]], 'distributions':['uniform'],
            'probes':[0], 'work_ns':[0], 'data_ops':[1],
            'rx_idle_us':[0], 'handoff_idle_us':[0],
            'handoff_preconnect':[0], 'warmup_ms':0,
            'measure_ms':1000,'paxos_slots':131072,'max_ops':90000,
            'repetitions':1, 'seed':2026+1000*i
        }
        case=runner.expand_live(spec,topo,2026,0)[0]
        case['id']=f'p{count}-r{repetition}-i{i}'
        # One baseline + five concurrent instances per round. Keep even
        # sequential runs on distinct ports: TIME_WAIT on a previous
        # server or SSH source port must never poison the next trial.
        case['port_base']=PORT_BASE
        case['port_offset']=(repetition-1)*ROUND_STRIDE+(
            5*PORT_STRIDE if count==1 else i*PORT_STRIDE)
        case['start_gate']=gate
        # Fail during planning if any port/worker budget is invalid.
        doc=runner.case_topology(topo,case,topo['benchmark']['ballot']+repetition*10+i)
        cluster.validate(doc)
        result.append(case)
    assert len(result)==count
    return result


def port_sets(topo,cases):
    """Pairwise-disjoint TCP listener/worker ranges on all shared machines."""
    ranges=[]
    for c in cases:
        ports=runner.case_topology(topo,c,4101)['ports']
        values={ports['ordering'],ports['data'],ports['coordinator']}
        values.update(range(ports['worker_base'],ports['worker_base']+64))
        ranges.append(values)
    if sum(map(len,ranges))!=len(set().union(*ranges)):
        raise ValueError("parallel instance TCP port ranges overlap")
    return ranges


def gate_path(executor, token):
    return '$HOME/'+executor.repo_dir+'/results/automation/gates/'+token+'.go'


def publish_gate(executor, mn4, token):
    """Atomically publish a shared MN4-clock GO timestamp 1.5 seconds ahead."""
    dest=gate_path(executor,token)
    tmp=dest+'.tmp'
    target=time.time_ns()+1_500_000_000
    cmd=('set -euo pipefail; mkdir -p "$HOME/'+executor.repo_dir+
         '/results/automation/gates"; test ! -e "'+dest+'"; '+
         'printf "%s\n" '+str(target)+' > "'+tmp+'"; mv -- "'+tmp+'" "'+dest+'"')
    executor.shell(mn4,cmd,timeout=15)
    return target


def clear_gate(executor, mn4, token):
    dest=gate_path(executor,token)
    executor.shell(mn4,'rm -f -- "'+dest+'" "'+dest+'.tmp"',timeout=15)


def wait_all_ready(executor, mn4, session, cases, futures, seconds=105):
    """The marker is printed AFTER all CN QPs and workers are ready for GO."""
    ready=set()
    deadline=time.monotonic()+seconds
    while len(ready)!=len(cases):
        for future in futures:
            if future.done() and future.exception() is not None:
                raise RuntimeError('instance bootstrap failed: '+str(future.exception()))
        for case in cases:
            if case['id'] in ready:
                continue
            jid=session+'-'+case['id']
            state=executor.agent(mn4,cfg=jid,job=jid+'-coordinator',
                                 role='coordinator',action='status',timeout=20)
            if state['finished']:
                raise RuntimeError('coordinator stopped before synchronized GO: '+
                                   state['log_tail'][-800:])
            if state['alive'] and 'LIVE_GO_GATE_READY' in state['log_tail']:
                ready.add(case['id'])
        if len(ready)==len(cases):
            return
        if time.monotonic()>deadline:
            raise TimeoutError('not all independent coordinators reached GO gate')
        time.sleep(.25)


def combine(count,repetition,rows,
            workers_per_cn=DEFAULT_PER_CN_WORKERS):
    if len(rows)!=count or any(row['status']!='PASS' for row in rows):
        raise ValueError('cannot aggregate missing/failed instances')
    starts=[int(row['go_epoch_ns']) for row in rows]
    skew=max(starts)-min(starts)
    if skew>GO_SKEW_LIMIT_NS:
        raise ValueError(f'unsynchronized GO windows, start skew {skew/1e6:.1f} ms')
    if any(int(r['measure_ms'])!=1000 or int(r['warmup_ms'])!=0 for r in rows):
        raise ValueError('measurement windows differ')
    workers=sum(int(row['worker_processes']) for row in rows)
    keys=sum(int(row['lock_count']) for row in rows)
    if workers!=2*workers_per_cn or keys!=TOTAL_KEYS:
        raise ValueError('client or keyspace mismatch between modes')
    rates=[float(row['goodput_ops_per_sec']) for row in rows]
    return dict(repetition=repetition,instances=count,workers_total=workers,
                keys_total=keys,aggregate_ops_per_sec=round(sum(rates),2),
                slowest_instance_ops_per_sec=round(min(rates),2),
                fastest_instance_ops_per_sec=round(max(rates),2),
                go_skew_ms=round(skew/1e6,3),
                worst_instance_p99_us=round(
                    max(int(r['latency_p99_ns']) for r in rows)/1000,2),
                measured_completions=sum(int(r['measured']) for r in rows),
                status='PASS')


def run_group(executor,topo,sha,session,root,count,rep,timeout,
              workers_per_cn=DEFAULT_PER_CN_WORKERS):
    token=f'{session}-p{count}-r{rep}'
    cases=make_cases(topo,count,rep,token,workers_per_cn)
    port_sets(topo,cases)
    node=topo['data_mn']
    print(f'  {count} leader(s), repetition {rep}: starting {count} independent job(s)',flush=True)
    collected=[]
    with ThreadPoolExecutor(max_workers=count) as pool:
        futures={pool.submit(runner.benchmark,executor,case,topo,sha,session,
                             root,topo['benchmark']['ballot']+100*rep+i,
                             timeout):case
                 for i,case in enumerate(cases)}
        gate_created=False
        try:
            wait_all_ready(executor,node,session,cases,futures)
            target=publish_gate(executor,node,token)
            gate_created=True
            print(f'    all {count} ready; GO target Unix ns {target}',flush=True)
        except Exception as err:
            # Release blocked workers before their normal per-job teardown.
            # The group still fails: never use these results as a valid sample.
            if not gate_created:
                try:
                    publish_gate(executor,node,token)
                    gate_created=True
                except Exception as e:
                    print(f'    GO recovery gate error: {e}',flush=True)
            for f in as_completed(futures):
                case=futures[f]
                try: f.result()
                except Exception as e:
                    print(f'    {case["id"]} cleanup: {e}',flush=True)
            raise RuntimeError('parallel startup/GO barrier failed: '+str(err)) from err
        finally:
            pass
        for future in as_completed(futures):
            case=futures[future]
            try:
                row=future.result()
                print(f'    {case["id"]} PASS {row["goodput_ops_per_sec"]} ops/s',flush=True)
            except Exception as e:
                row=runner.failure_row(case,sha,e)
                (root/case['id']).mkdir(exist_ok=True,parents=True)
                (root/case['id']/'failure.txt').write_text(str(e)+'\n')
                print(f'    {case["id"]} FAIL {e}',flush=True)
            row['parallel_instances']=count
            row['instance_id']=int(case['id'].rsplit('i',1)[-1])
            row['repetition']=rep
            row['instance_key_count']=case['locks']
            collected.append(row)
    if gate_created:
        clear_gate(executor,node,token)
    if len(collected)!=count or any(r['status']!='PASS' for r in collected):
        raise RuntimeError('parallel group contains failed or incomplete instances')
    return collected,combine(count,rep,collected,workers_per_cn)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--topology',type=Path,default=(
        HERE/'topology.local.json' if (HERE/'topology.local.json').exists()
        else HERE/'topology.json'))
    p.add_argument('--ssh-user',default=os.environ.get('USER',''))
    p.add_argument('--remote-dir',default='rdmaLock')
    p.add_argument('--repetitions',type=int,default=3)
    p.add_argument('--workers-per-cn',type=int,default=DEFAULT_PER_CN_WORKERS,
                   help='total worker processes on EACH CN (default 8); '
                        'distributed among five leaders without oversubscription')
    p.add_argument('--timeout',type=int,default=180)
    p.add_argument('--dry-run',action='store_true')
    p.add_argument('--no-auto-deps',action='store_true')
    args=p.parse_args()
    if not 1<=args.repetitions<=10 or not 60<=args.timeout<=3600:
        raise ValueError('repetitions must be 1..10, timeout 60..3600')
    worker_allocation(INSTANCE_COUNT,args.workers_per_cn)
    topo,_=cluster.read(args.topology)
    if len(topo['clients'])<2:
        raise ValueError('need two CNs')
    for count in (1,INSTANCE_COUNT):
        cases=make_cases(topo,count,1,'DRY_RUN_GATE',args.workers_per_cn)
        port_sets(topo,cases)
        print(f'PLAN: {count} leaders, {len(cases)} jobs; '+
              f'{2*args.workers_per_cn} total workers; '+
              f'{TOTAL_KEYS} total independent keys; '+
              f'workers/CN/instance={[c["workers_per_cn"] for c in cases]}; '+
              f'keys/instance={[c["locks"] for c in cases]}')
        for c in cases:
            cfg=runner.case_topology(topo,c,4101)
            print(f'  {c["id"]} ports={cfg["ports"]}')
    if args.dry_run:
        return 0
    sha=runner.git_commit()
    ips=runner.local_ip_set()
    if topo['data_mn']['ip'] not in ips:
        raise RuntimeError('run this experiment on MN4 itself (gate uses its local clock)')
    executor=runner.Executor(username=args.ssh_user,checkout=ROOT,local_ips=ips,
                             repo_dir=args.remote_dir)
    lock_file=ROOT/'results'/'automation'/'runner.lock'
    lock_file.parent.mkdir(exist_ok=True,parents=True)
    with lock_file.open('w') as lock:
        try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('another automation suite is active')
        session=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+f'-parallel-{os.getpid()}'
        root=ROOT/'results'/'automation'/session
        (root/'bootstrap').mkdir(parents=True)
        nodes=topo['replicas']+[topo['data_mn']]+topo['clients'][:2]
        print('Bootstrapping exact commit '+sha+' on 6 physical nodes',flush=True)
        for n in nodes:
            try:
                executor.shell(n,'hostname && id -un',timeout=18)
                result=executor.bootstrap(n,sha,topo['device'],topo['ib_port'],
                                          auto_deps=not args.no_auto_deps)
                (root/'bootstrap'/f'{n["id"]}.log').write_text(result)
            except Exception as e:
                (root/'bootstrap'/f'{n["id"]}.error.txt').write_text(str(e))
                raise
        (root/'manifest.json').write_text(json.dumps({
            'commit':sha,'note':'independent MuCQL instances, NOT an integrated sharded lock',
            'one_vs_five_total_workers':2*args.workers_per_cn,
            'workers_per_cn':args.workers_per_cn,
            'total_keys':TOTAL_KEYS,'key_distribution':'proportional to workers',
            'measured_ms':1000,'repetitions':args.repetitions,
            'port_stride':PORT_STRIDE},indent=2))
        cols=runner.COLUMNS+['parallel_instances','instance_id','repetition','instance_key_count']
        summary_cols=['repetition','instances','workers_total','keys_total',
                      'aggregate_ops_per_sec','slowest_instance_ops_per_sec',
                      'fastest_instance_ops_per_sec','go_skew_ms',
                      'worst_instance_p99_us','measured_completions','status']
        all_aggregates=[]
        with (root/'runs.csv').open('w',newline='') as f, (root/'aggregates.csv').open('w',newline='') as af:
            rw=csv.DictWriter(f,fieldnames=cols);rw.writeheader()
            aw=csv.DictWriter(af,fieldnames=summary_cols);aw.writeheader()
            # Alternate baseline and five-way group by repetition to reduce
            # long-term cluster load drift; never overlap these two modes.
            for rep in range(1,args.repetitions+1):
                for count in (1,INSTANCE_COUNT):
                    rows,agg=run_group(executor,topo,sha,session,root,count,rep,
                                       args.timeout,args.workers_per_cn)
                    for row in rows: rw.writerow(row)
                    aw.writerow(agg);f.flush();af.flush()
                    all_aggregates.append(agg)
                    print(f'  {count}-leader aggregate: {agg["aggregate_ops_per_sec"]:.2f} ops/s'+
                          f' (GO skew {agg["go_skew_ms"]:.2f} ms)',flush=True)
        single=[a['aggregate_ops_per_sec'] for a in all_aggregates if a['instances']==1]
        five=[a['aggregate_ops_per_sec'] for a in all_aggregates if a['instances']==5]
        speedup=statistics.median(five)/statistics.median(single)
        print(f'\nMedian single leader: {statistics.median(single):,.2f} ops/s',flush=True)
        print(f'Median five leaders: {statistics.median(five):,.2f} ops/s',flush=True)
        print(f'Aggregate throughput ratio: {speedup:.3f}x',flush=True)
        print('Independent services and logs; NOT per-key sharded correctness.',flush=True)
        print(f'Instance runs: {root/"runs.csv"}',flush=True)
        print(f'Aggregate rounds: {root/"aggregates.csv"}',flush=True)
        return 0


if __name__=='__main__':
    try: sys.exit(main())
    except (OSError,ValueError,RuntimeError,TimeoutError) as e:
        print('ERROR:',e,file=sys.stderr)
        sys.exit(2)
