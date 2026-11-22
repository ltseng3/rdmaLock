"""Minimal shape check for the DecLock benchmark source anchors."""
from pathlib import Path
from tempfile import TemporaryDirectory
import subprocess,sys
with TemporaryDirectory() as d:
 p=Path(d)/'tests/microbench.cc';p.parent.mkdir()
 p.write_text('#include <chrono>\nint main() {\n  int req_cnt = 0;\n  int fin_cnt = 0;\n  ssched.spawn([lcid, crtid, &local_rand, &global_rand,\n                   &req_cnt, &fin_cnt](R2_ASYNC) {\n          micro_exec_op(\n            MAKE_TID(lcid, crtid), line_num_to_key[line], requester, \n            op_array[lid] > 1 ? LOCK_EXCL : LOCK_SHARED, \n            value_addr[lid] + MN_LOCK_MR_SIZE(LOCK_NUM), \n            value_size[lid] ASYNC_ARGS\n          );\n          micro_gen_and_exec_op(MAKE_TID(lcid, crtid), requester, \n                global_rand, local_rand ASYNC_ARGS);\n      req_cnt++;\n\n#ifdef PERIODIC_THPT\n  ssched.run();\n\n  // Calculate the lock granting throughput.\n  LLOG("[host%u][core%u] request throughput %.4f Mops", LHID, lcid, req_thpt);\n}\n')
 patcher=Path(__file__).resolve().parents[2]/'tools/patch_declock_benchmark.py'
 r=subprocess.run([sys.executable,str(patcher),d],capture_output=True,text=True)
 if r.returncode: raise RuntimeError(r.stdout+r.stderr)
 s=p.read_text()
 for expected in ('completed_lock_cnt','GOODPUT_BIN host=','&goodput_bins','if (lock_rc == 0)'):
  assert expected in s,expected
 r=subprocess.run([sys.executable,str(patcher),d],capture_output=True,text=True)
 assert r.returncode==0 and 'Already patched' in r.stdout
 print('PASS: benchmark patch anchors, capture list, idempotence')
