#!/usr/bin/env python3
"""Install tested RDMA prototype modules into an existing DecLock clone.

This does NOT register SYSTEM=mucql (the distributed networked backend is
not yet implemented). It leaves all DecLock baseline files intact except the
explicitly requested benchmark instrumentation, which it patches in place.
"""
import argparse
from pathlib import Path
from shutil import copytree, copy2
import subprocess
import sys
ap=argparse.ArgumentParser()
ap.add_argument('declock',type=Path)
ap.add_argument('--no-benchmark',action='store_true')
a=ap.parse_args()
repo=a.declock.resolve()
root=Path(__file__).resolve().parent.parent
if not (repo/'lock/dmlock/remote.cc').exists() or not (repo/'tests/microbench.cc').exists():
    sys.exit('Not a DecLock checkout: '+str(repo))
dst=repo/'experiments/mucql'
if dst.exists():
    sys.exit('Refusing to overwrite '+str(dst))
dst.parent.mkdir(parents=True,exist_ok=True)
copytree(root/'mucql',dst,ignore=lambda d,n:{x for x in n if x.startswith('build') or x=='__pycache__'})
(dst/'adapters').mkdir(exist_ok=True)
for f in (root/'adapters/declock').glob('rdma_handoff.*'):
    copy2(f,dst/'adapters'/f.name)
if not a.no_benchmark:
    subprocess.run([sys.executable,str(root/'tools/patch_declock_benchmark.py'),str(repo)],check=True)
print('Installed standalone core and adapter:',dst)
print('Do NOT use SYSTEM=mucql yet. No production networked quorum service exists.')
