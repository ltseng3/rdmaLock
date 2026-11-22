#!/usr/bin/env python3
"""Patch DecLock's benchmark to count successful lock cycles and emit time bins.

Safe to run twice; exits on unexpected source layouts. No lock implementation is
modified. Baseline binaries MUST be rebuilt with this same instrumented file.
"""
import argparse
from pathlib import Path

ap=argparse.ArgumentParser()
ap.add_argument('repo', type=Path, help='Path to dracit7/declock clone')
args=ap.parse_args()
p=args.repo/'tests/microbench.cc'
s=p.read_text()
if 'GOODPUT_BIN host=' in s:
    print('Already patched:', p)
    raise SystemExit(0)

def once(before,after):
    global s
    assert s.count(before)==1, f'Expected exactly one anchor, found {s.count(before)}: {before[:100]!r}'
    s=s.replace(before,after,1)

once('#include <chrono>', '#include <chrono>\n#include <cinttypes>\n#include <tuple>')
once("""  ssched.spawn([lcid, crtid, &local_rand, &global_rand,
                   &req_cnt, &fin_cnt](R2_ASYNC) {""", """  ssched.spawn([lcid, crtid, &local_rand, &global_rand,
                   &req_cnt, &completed_lock_cnt, &goodput_bin_completed,
                   &goodput_bins, &goodput_bin_steady, &goodput_bin_begin,
                   &fin_cnt](R2_ASYNC) {""")
once('  int req_cnt = 0;\n  int fin_cnt = 0;',  '''  int req_cnt = 0;                 // attempts (legacy field)
  int completed_lock_cnt = 0;      // successful acquire/data/release cycles
  // Bins are accumulated in the measured section but printed AFTER it.
  using BenchBin = std::tuple<int64_t, int64_t, uint64_t>;
  std::vector<BenchBin> goodput_bins;
  uint64_t goodput_bin_completed = 0;
  auto goodput_bin_begin = std::chrono::system_clock::now();
  auto goodput_bin_steady = steady_clock::now();
  int fin_cnt = 0;''')

once('''          micro_exec_op(
            MAKE_TID(lcid, crtid), line_num_to_key[line], requester, 
            op_array[lid] > 1 ? LOCK_EXCL : LOCK_SHARED, 
            value_addr[lid] + MN_LOCK_MR_SIZE(LOCK_NUM), 
            value_size[lid] ASYNC_ARGS
          );''','''          int lock_rc = micro_exec_op(
            MAKE_TID(lcid, crtid), line_num_to_key[line], requester,
            op_array[lid] > 1 ? LOCK_EXCL : LOCK_SHARED,
            value_addr[lid] + MN_LOCK_MR_SIZE(LOCK_NUM),
            value_size[lid] ASYNC_ARGS);
          if (lock_rc == 0) { completed_lock_cnt++; goodput_bin_completed++; }''')
once('''          micro_gen_and_exec_op(MAKE_TID(lcid, crtid), requester, 
                global_rand, local_rand ASYNC_ARGS);''','''          int lock_rc = micro_gen_and_exec_op(MAKE_TID(lcid, crtid), requester,
                global_rand, local_rand ASYNC_ARGS);
          if (lock_rc == 0) { completed_lock_cnt++; goodput_bin_completed++; }''')
once('''      req_cnt++;

#ifdef PERIODIC_THPT''','''      req_cnt++;
      // This sampling code is included in ALL compared lock binaries.
      // The time check already exists in the loop's duration test; here
      // we emit coarse, wall-clock-addressable bins only every 10 ms.
      auto goodput_now = steady_clock::now();
      if (goodput_now - goodput_bin_steady >= milliseconds(10)) {
        auto wall_end = std::chrono::system_clock::now();
        auto begin_ns = duration_cast<nanoseconds>(
            goodput_bin_begin.time_since_epoch()).count();
        auto end_ns = duration_cast<nanoseconds>(
            wall_end.time_since_epoch()).count();
        goodput_bins.emplace_back(begin_ns, end_ns, goodput_bin_completed);
        goodput_bin_steady = goodput_now;
        goodput_bin_begin = wall_end;
        goodput_bin_completed = 0;
      }

#ifdef PERIODIC_THPT''')
once('''  ssched.run();

  // Calculate the lock granting throughput.''','''  ssched.run();

  // Final partial bin. Use wall-clock labels only for postprocessing;
  // duration-based throughput below retains the existing monotonic timer.
  auto goodput_final_wall = std::chrono::system_clock::now();
  goodput_bins.emplace_back(
      duration_cast<nanoseconds>(goodput_bin_begin.time_since_epoch()).count(),
      duration_cast<nanoseconds>(goodput_final_wall.time_since_epoch()).count(),
      goodput_bin_completed);

  // Calculate the lock granting throughput.''')
once('''  LLOG("[host%u][core%u] request throughput %.4f Mops", LHID, lcid, req_thpt);''','''  LLOG("[host%u][core%u] request throughput %.4f Mops", LHID, lcid, req_thpt);
  LLOG("[host%u][core%u] completed lock cycles %d", LHID, lcid, completed_lock_cnt);
  LLOG("[host%u][core%u] successful lock throughput %.4f Mops",
       LHID, lcid, (double)completed_lock_cnt / (double)duration);
  for (const auto &bin : goodput_bins) {
    LLOG("GOODPUT_BIN host=%u core=%u start_ns=%" PRId64
         " end_ns=%" PRId64 " completed=%" PRIu64,
         LHID, lcid, std::get<0>(bin), std::get<1>(bin), std::get<2>(bin));
  }''')
p.write_text(s)
print('Patched:',p)
