#pragma once
// Drop-in DecLock verbs adapter. It does not implement JOIN / PREPARE or
// consensus. Include from lock/mucql/ alongside the original lib/rdma.h.
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include "rdma.h" // dracit7/declock/lib/rdma.h

namespace mucql { namespace declock {
// Fixed wire layout; all peers on Apt are little endian x86_64.
// For heterogeneous hosts, explicitly encode network endian instead.
static constexpr uint32_t kHandoffMagic = 0x4d43514c; // "MCQL"
static constexpr uint32_t kReleaseSlotsPerLock = 64;
struct HandoffWire {
  uint32_t magic;
  uint32_t reserved;
  uint64_t lock;
  uint64_t position;
  uint64_t predecessor_grant;
  uint64_t successor_grant;
  uint64_t deadline_ns;
};
static_assert(sizeof(HandoffWire)==48,"Unexpected handoff wire layout");

// Release memory is an 8-byte/position ring, unique grant IDs prevent ABA.
// Old stale WRITEs can obscure a new record, affecting progress, not safety.
// Keep this region separate from Mu/Paxos's protected replicated log.
inline uint64_t release_region_bytes(uint64_t lock_num) {
  return lock_num*kReleaseSlotsPerLock*sizeof(uint64_t);
}
inline uint64_t release_offset(uint64_t lock,uint64_t position) {
  return (lock*kReleaseSlotsPerLock + position%kReleaseSlotsPerLock)
          *sizeof(uint64_t);
}

// These functions use DecLock's existing UD notification and RC WRITE.
// Invoke release only after protected I/O quiescence. Any posted release must
// be drained BEFORE a new RC op on the same coroutine's scratch buffer.
class HandoffTransport {
 public:
  void send_handoff(uint32_t tid, uint16_t dest_mid, uint16_t dest_tid,
                    const HandoffWire& msg);
  void post_release(uint32_t tid, uint16_t data_mn, uint64_t raddr,
                    uint64_t grant_id);
  void drain_release(uint32_t tid);
 private:
  // Indexed by DecLock's tid convention (core*MAX_CRT_NUM + coro).
  struct Pending {bool valid=false;uint16_t mn=0;};
  Pending pending_[MAX_CLIENT_NUM]{};
};
}} // namespace
