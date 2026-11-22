#pragma once
// Small Multi-Paxos ordering engine with abstract acceptors.
// The test acceptor, TCP transport, and EXPERIMENTAL one-sided RDMA adapter
// use this implementation. Crash-stop replicas only; hardware fencing of
// RDMA old leaders has not yet been validated on CloudLab Apt.
#include "mucql/core.hpp"
#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
namespace mucql {
struct Accepted { uint64_t ballot; Record value; };
struct Promise {
  bool granted{false};
  std::map<uint64_t,Accepted> accepted;
};
class Acceptor {
 public:
  virtual ~Acceptor() = default;
  virtual Promise prepare(uint64_t ballot);
  virtual bool accept(uint64_t ballot, const Record& value);
  // Phase 2 may be pipelined across replicas. The default acceptor runs the
  // operation synchronously; the one-sided RDMA acceptor posts asynchronously
  // and finishes after all follower WRs have been issued.
  virtual void begin_accept(uint64_t ballot, const Record& value) {
    last_accept_=accept(ballot,value);
  }
  virtual bool finish_accept() {return last_accept_;}
  virtual void crash(bool down); // unavailable, memory retained (crash-stop survivor)
 private:
  std::mutex mu_;
  uint64_t promised_{0};
  std::map<uint64_t,Accepted> accepted_;
  bool down_{false};
  bool last_accept_{false};
};
// Acceptors may be local, TCP, or an opt-in experimental RDMA transport.
class LocalQuorum {
 public:
  explicit LocalQuorum(size_t nodes=3);
  explicit LocalQuorum(std::vector<std::shared_ptr<Acceptor>> replicas);
  std::vector<std::shared_ptr<Acceptor>>& nodes(){return nodes_;}
  const std::vector<std::shared_ptr<Acceptor>>& nodes() const{return nodes_;}
  size_t majority() const{return nodes_.size()/2+1;}
 private:
  std::vector<std::shared_ptr<Acceptor>> nodes_;
};
class PaxosJournal final: public Journal {
 public:
  // Ballots must be globally unique and monotonically increasing across
  // *actual* leadership attempts. In-process tests pass unique ballots.
  PaxosJournal(LocalQuorum& quorum,uint64_t ballot);
  // Phase 1: promise from majority, then phase 2 re-proposes all accepted
  // values to establish a recoverable contiguous chosen prefix.
  void take_over();
  Record commit(Record proposal) override;
  std::vector<Record> snapshot() const override;
 private:
  bool accept_to_majority(const Record& r);
  LocalQuorum& quorum_;
  uint64_t ballot_;
  mutable std::mutex mu_;
  bool leader_{false};
  std::vector<Record> decided_;
};
}
