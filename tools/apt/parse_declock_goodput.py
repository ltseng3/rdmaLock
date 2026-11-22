#!/usr/bin/env python3
"""Parse *successful* DecLock lock cycles, rejecting attempts-only baseline logs.

DecLock LLOG writes throughput to stderr, not to the remote per-CN stdout
latency files. Parse ONE combined run-on-all.sh stderr/stdout capture (as
the upstream getresults.sh uses 'tmp'), OR two separately captured CN stderr
logs, after verifying both CNs exited cleanly. Does not infer fake goodput.
"""
import argparse
import json
import math
import re
from pathlib import Path

SUCCESS = re.compile(r'\[host(\d+)\]\[core(\d+)\]\s+successful lock throughput ([0-9]+(?:\.[0-9]+)?) Mops')
COMPLETED = re.compile(r'\[host(\d+)\]\[core(\d+)\]\s+completed lock cycles (\d+)')


def parse(logs, cores_per_cn):
    if type(cores_per_cn) is not int or not 1<=cores_per_cn<=16:
        raise ValueError('cores_per_cn must be 1..16')
    if len(logs) not in (1,2):
        raise ValueError('supply one combined stderr capture or two per-CN stderr logs')
    seen={}
    completions={}
    for text in logs:
        rates=SUCCESS.findall(text)
        counts=COMPLETED.findall(text)
        expected=cores_per_cn*(2 if len(logs)==1 else 1)
        if len(rates)!=expected or len(counts)!=expected:
            raise ValueError('incomplete or uninstrumented DecLock stderr log; do not count attempts')
        hosts=set()
        for h,core,value in rates:
            key=(int(h),int(core))
            if key in seen:
                raise ValueError('duplicate DecLock core or duplicated CN log')
            v=float(value)
            if not math.isfinite(v) or v<0:
                raise ValueError('invalid DecLock successful lock goodput')
            seen[key]=v
            hosts.add(key[0])
        if len(hosts)!=(2 if len(logs)==1 else 1):
            raise ValueError('unexpected number of CN hosts in stderr capture')
        for h,core,value in counts:
            key=(int(h),int(core))
            if key in completions:raise ValueError('duplicate completed-cycle report')
            completions[key]=int(value)
    if (set(seen)!=set(completions) or len(seen)!=2*cores_per_cn or
            len({h for h,c in seen})!=2):
        raise ValueError('missing or mismatched host/core successful completion')
    if any(v==0 for v in completions.values()):
        raise ValueError('zero successful lock cycles reported by a core')
    return {
        'status':'PARSED_ONLY',
        'cn_nodes':2,'workers':len(seen),'completed_cycles':sum(completions.values()),
        'goodput_ops_per_sec':round(sum(seen.values())*1_000_000,2),
        'metric':'sum of per-core successful completed lock throughput',
        'request_attempts_used':False,
    }


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('cn_logs',type=Path,nargs='+',help='one combined stderr log or two independent CN stderr logs')
    p.add_argument('--cores-per-cn',type=int,required=True)
    p.add_argument('--out',type=Path)
    a=p.parse_args()
    result=parse([f.read_text() for f in a.cn_logs],a.cores_per_cn)
    result['input_files']=[str(f.resolve()) for f in a.cn_logs]
    data=json.dumps(result,indent=2)+'\n'
    if a.out:
        a.out.write_text(data)
    print(data,end='')


if __name__=='__main__':
    main()
