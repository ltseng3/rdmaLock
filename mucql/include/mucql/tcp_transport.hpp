#pragma once
// Reference network transport for custom Multi-Paxos. POSIX TCP, not RDMA.
// Intended to validate distributed quorum behavior without relying on Mu.
#include "mucql/paxos.hpp"
#include <string>
#include <mutex>
namespace mucql {
class TcpAcceptor final: public Acceptor {
 public:
  TcpAcceptor(std::string host,uint16_t port);
  ~TcpAcceptor() override;
  Promise prepare(uint64_t ballot) override;
  bool accept(uint64_t ballot, const Record& value) override;
  void crash(bool) override {}
 private:
  bool connect_locked();
  void close_locked();
  std::mutex mu_;
  std::string host_;
  uint16_t port_;
  int fd_{-1};
};
// Blocks forever serving a crash-stop acceptor; no disk persistence. On
// restart the process MUST NOT rejoin under its previous replica identity
// until state transfer / reconfiguration is implemented.
int run_tcp_acceptor(uint16_t port);
} // namespace mucql
