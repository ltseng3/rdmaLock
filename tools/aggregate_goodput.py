#!/usr/bin/env python3
"""Conservative common-window cluster goodput from patched DecLock logs.

Do not add independently measured per-core throughput rates. This tool
counts only bins fully within the global common interval. It intentionally
undercounts operations in partial edge bins. Cross-node time calibration is
required: use --assume-synchronized explicitly or supply per-host offsets.
"""
import argparse
import collections
import json
from pathlib import Path
import re
import sys
P=re.compile(r'GOODPUT_BIN host=(\d+) core=(\d+) start_ns=(\d+) end_ns=(\d+) completed=(\d+)')
parser=argparse.ArgumentParser()
parser.add_argument('logs', nargs='+', type=Path)
parser.add_argument('--assume-synchronized',action='store_true',
                    help='Explicit assumption: hosts share a sufficiently accurate wall clock')
parser.add_argument('--offsets-json',type=Path,
                    help='JSON mapping host IDs to ns to ADD to their local timestamps')
parser.add_argument('--skew-bound-us',type=float,default=0,
                    help='Conservative +- clock uncertainty after calibration')
parser.add_argument('--trim-ms',type=float,default=0,
                    help='Trim this interval from BOTH ends of common window')
args=parser.parse_args()
if not args.assume_synchronized and args.offsets_json is None:
    parser.error('Cluster-time evidence required: --assume-synchronized or --offsets-json')
offsets=json.loads(args.offsets_json.read_text()) if args.offsets_json else {}
bins=collections.defaultdict(list)
for file in args.logs:
    for line in file.read_text(errors='replace').splitlines():
        m=P.search(line)
        if not m: continue
        host,core,start,end,n=map(int,m.groups())
        if end<=start: continue
        adjust=int(offsets.get(str(host),0))
        bins[(host,core)].append((start+adjust,end+adjust,n))
if not bins: sys.exit('No GOODPUT_BIN records found')
for key,items in bins.items():
    items.sort()
    for a,b in zip(items,items[1:]):
        if a[1]>b[0]: sys.exit(f'Overlapping bins for worker {key}')
skew=int(args.skew_bound_us*1000)
trim=int(args.trim_ms*1_000_000)
t0=max(items[0][0] for items in bins.values())+skew+trim
t1=min(items[-1][1] for items in bins.values())-skew-trim
if t1<=t0:sys.exit('No provably common measurement interval')
completed=0
per_worker={}
for key,items in sorted(bins.items()):
    c=sum(n for start,end,n in items if start>=t0 and end<=t1)
    per_worker[str(key)]=c
    completed+=c
# The denominator includes all omitted edge bins, intentionally a lower bound
# on the rate of completed operations over this interval.
print(json.dumps({'workers':len(bins),'common_start_ns':t0,
                  'common_end_ns':t1,'interval_s':(t1-t0)/1e9,
                  'completed_in_full_bins':completed,
                  'conservative_goodput_mops':completed/(t1-t0)*1e3,
                  'per_worker_completed':per_worker},indent=2))
