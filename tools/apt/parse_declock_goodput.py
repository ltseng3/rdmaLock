#!/usr/bin/env python3
"""Parse *successful* DecLock lock cycles, rejecting attempts-only baseline logs.

Run on the two unmodified per-CN logs produced by patched DecLock microbench
after verifying both CN processes exited cleanly. Does not infer fake goodput.
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
    if len(logs)!=2:
        raise ValueError('exactly two independent CN log files required')
    seen={}
    completions={}
    for text in logs:
        rates=SUCCESS.findall(text)
        counts=COMPLETED.findall(text)
        if len(rates)!=cores_per_cn or len(counts)!=cores_per_cn:
            raise ValueError('incomplete or uninstrumented DecLock log; do not count attempts')
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
        if len(hosts)!=1:
            raise ValueError('a CN log contains multiple host IDs')
        for h,core,value in counts:
            key=(int(h),int(core))
            if key in completions:raise ValueError('duplicate completed-cycle report')
            completions[key]=int(value)
    if set(seen)!=set(completions) or len({h for h,c in seen})!=2:
        raise ValueError('missing or mismatched host/core successful completion')
    if any(v==0 for v in completions.values()):
        raise ValueError('zero successful lock cycles reported by a core')
    return {
        'status':'PASS_PARSED_NOT_HARDWARE_VERIFIED',
        'cn_nodes':2,'workers':len(seen),'completed_cycles':sum(completions.values()),
        'goodput_ops_per_sec':round(sum(seen.values())*1_000_000,2),
        'metric':'sum of per-core successful completed lock throughput',
        'request_attempts_used':False,
    }


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('cn_logs',type=Path,nargs=2)
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
