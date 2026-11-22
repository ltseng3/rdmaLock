#!/usr/bin/env python3
"""Parse the standalone RDMA smoke-test output and append a local CSV row."""
import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
import re
import sys

ORDERING = re.compile(r"One-sided RDMA phase2 committed=(\d+)\s+seconds=([0-9.eE+-]+)\s+ordered_entries_per_sec=([0-9.eE+-]+)")
RELEASE = re.compile(r"RDMA release mode=(CAS|WRITE_HINT)\s+operations=(\d+)\s+seconds=([0-9.eE+-]+)\s+ops_per_sec=([0-9.eE+-]+)")
FIELDS = ['utc', 'kind', 'mode', 'completed', 'seconds', 'ops_per_sec', 'mops', 'log']


def parse(text: str, kind: str) -> dict:
    m = (ORDERING if kind == 'ordering' else RELEASE).search(text)
    if m is None:
        raise ValueError('Expected benchmark summary not found; inspect raw log for ERROR or failures')
    if kind == 'ordering':
        count, secs, rate = m.groups()
        mode = 'RDMA_PHASE2'
    else:
        mode, count, secs, rate = m.groups()
    count = int(count)
    secs, rate = float(secs), float(rate)
    if count <= 0 or secs <= 0 or rate <= 0:
        raise ValueError('Invalid benchmark counters')
    if abs(count/secs-rate)/rate > 0.02:
        raise ValueError('Reported operations / elapsed time disagree')
    return {'kind': kind, 'mode': mode, 'completed': count,
            'seconds': f'{secs:.9f}', 'ops_per_sec': f'{rate:.3f}',
            'mops': f'{rate/1e6:.6f}'}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('kind', choices=('ordering', 'release'))
    p.add_argument('log', type=Path)
    p.add_argument('--csv', type=Path, default=Path('results/smoke.csv'))
    args = p.parse_args()
    try:
        row = parse(args.log.read_text(errors='replace'), args.kind)
    except (ValueError, OSError) as exc:
        sys.exit(f'ERROR: {exc}')
    row['utc'] = datetime.now(timezone.utc).isoformat(timespec='seconds')
    row['log'] = str(args.log)
    args.csv.parent.mkdir(parents=True, exist_ok=True)
    exists = args.csv.exists() and args.csv.stat().st_size > 0
    with args.csv.open('a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not exists: writer.writeheader()
        writer.writerow(row)
    print(f"{row['kind']} {row['mode']}: {row['completed']} operations in "
          f"{row['seconds']}s = {row['mops']} Mops/s (log: {row['log']})")


if __name__ == '__main__':
    main()
