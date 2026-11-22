#!/usr/bin/env python3
"""Summary of locally recorded transport smoke-test rates."""
import csv
from collections import defaultdict
from pathlib import Path
import statistics
import sys


def summarize(path: Path):
    groups = defaultdict(list)
    with path.open(newline='') as file:
        for row in csv.DictReader(file):
            groups[(row['kind'], row['mode'])].append(float(row['mops']))
    if not groups:
        raise ValueError('No valid rows')
    print(f'Hardware smoke-test summary: {path}')
    print('kind         mode        trials   median Mops/s   min-max Mops/s')
    for (kind, mode), rates in sorted(groups.items()):
        print(f'{kind:12} {mode:11} {len(rates):5d}   {statistics.median(rates):13.6f}   '
              f'{min(rates):.6f}..{max(rates):.6f}')
    print('The ordering and release tests are separate transports; neither rate is end-to-end lock goodput.')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit('Usage: python3 tools/apt/summarize.py results/TIMESTAMP/summary.csv')
    summarize(Path(sys.argv[1]))
