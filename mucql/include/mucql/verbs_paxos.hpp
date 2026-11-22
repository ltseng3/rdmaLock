#pragma once
#include "mucql/paxos.hpp"
#include <cstdint>
#include <memory>
#include <string>
namespace mucql {
struct RdmaPaxosOptions {
  std::string device; // InfiniBand device e.g. mlx4_0
  uint8_t port{1};
  uint32_t slots{4096};
  // Don't enable until a NIC fence/drain test confirms old QPs cannot write
  // accepted slots after a new higher ballot is promised.
  bool allow_experimental_takeover{false};
};
// Phase 1: serialized control plane (TCP + hard QP fence).
// Phase 2: follower CPU does NOTHING: leader issues RDMA WRITE payload then
// RDMA atomic CAS to finalize the slot, and waits for local CQE.
// Distinct accepted region per ballot; crashes are crash-stop (no rejoining
// empty replicas under the same ID). Not a proven Mu implementation.
class RdmaAcceptor final : public Acceptor {
 public:
  RdmaAcceptor(std::string host,uint16_t tcp_port,RdmaPaxosOptions);
  ~RdmaAcceptor() override;
  Promise prepare(uint64_t ballot) override;
  bool accept(uint64_t ballot,const Record& value) override;
  void begin_accept(uint64_t ballot,const Record& value) override;
  bool finish_accept() override;
  void begin_accept_batch(uint64_t ballot,const std::vector<Record>& records) override;
  std::vector<bool> finish_accept_batch() override;
  void crash(bool) override {}
 private:
  struct Impl;std::unique_ptr<Impl> impl_;
};
void serve_rdma_paxos(uint16_t control_port,const RdmaPaxosOptions& options);
} // namespace mucql
