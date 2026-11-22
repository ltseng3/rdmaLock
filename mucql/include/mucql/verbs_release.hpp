#pragma once
// RDMA release-marker transport for CloudLab/Apt InfiniBand (mlx4 / ConnectX-3).
// This header deliberately hides ibverbs types from callers and core tests.
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
namespace mucql {
struct ReleaseEndpointOptions {
  std::string device;       // empty -> first usable IB device (verify on Apt!)
  uint8_t port{1};
  uint32_t slots{4096};
};
// Remote atomics are the CERTIFYING path: 8-byte CAS is atomic at the responder.
// A plain RDMA WRITE is also supported as a performance-only HINT, but it must
// not be used as proof of release without a separate hardware atomicity proof.
enum class ReleaseMode { AtomicCas, WriteHint };
class VerbsReleaseServer {
 public:
  explicit VerbsReleaseServer(const ReleaseEndpointOptions& options);
  ~VerbsReleaseServer();
  VerbsReleaseServer(const VerbsReleaseServer&)=delete;
  VerbsReleaseServer& operator=(const VerbsReleaseServer&)=delete;
  void listen_forever(uint16_t tcp_port);
  uint64_t slot_value(size_t slot) const;
 private:
  struct Impl; std::unique_ptr<Impl> impl_;
};
class VerbsReleaseClient {
 public:
  VerbsReleaseClient(const std::string& tcp_host,uint16_t tcp_port,
                     const ReleaseEndpointOptions& options);
  ~VerbsReleaseClient();
  VerbsReleaseClient(const VerbsReleaseClient&)=delete;
  VerbsReleaseClient& operator=(const VerbsReleaseClient&)=delete;
  // One operation outstanding; no buffer reuse until complete(). A caller can
  // issue its direct UD handoff between post() and complete().
  void post(size_t slot,uint64_t grant_id,ReleaseMode mode);
  // Completes the posted operation, returning true if the atomic CAS installed
  // grant_id or found that same id already present (idempotent retry).
  // For WriteHint this confirms NIC completion ONLY, not proof of atomicity.
  bool complete();
  uint32_t slots() const;
  // Synchronous RC operations. CAS returns the old value, so callers must
  // verify ownership immediately (never spin to conceal a double grant).
  uint64_t compare_exchange(size_t slot, uint64_t expected, uint64_t desired);
  uint64_t read_slot(size_t slot);
  void write_slot(size_t slot, uint64_t value);
 private:
  struct Impl; std::unique_ptr<Impl> impl_;
};
} // namespace mucql
