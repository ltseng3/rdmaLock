#pragma once
// MuCQL exclusive-lock protocol engine. Reference implementation, not consensus.
// C++17, no third-party dependencies. All timestamps are absolute ns in the
// clock's reference frame. The clock interface must return *sound* bounds.
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace mucql {
using LockId = uint64_t;
using ClientId = uint64_t; // incarnation-qualified; never reused on restart
using RequestId = uint64_t; // unique within (lock, client)
using Position = uint64_t;
using GrantId = uint64_t; // globally unique journal index; never reused
using Deadline = uint64_t;

struct Bounds { Deadline lower, upper; };
class Clock {
 public:
  virtual ~Clock() = default;
  virtual Bounds now() const = 0;
};

struct Ticket {
  LockId lock{};
  ClientId client{};
  RequestId request{};
  Position position{};
  GrantId grant_id{};
  GrantId predecessor_id{}; // 0 only for queue head
  // 0: unprepared; a ticket may never be activated unless prepared.
  Deadline deadline{};
};
struct ReleaseMark { LockId lock{}; Position position{}; GrantId grant_id{}; };
struct Handoff { Ticket successor; GrantId predecessor_id{}; Position predecessor{}; };

struct Record {
  enum class Type : uint8_t { Join, Prepare } type{};
  uint64_t index{}; // assigned by Journal::commit, forms GrantId for Join
  LockId lock{};
  ClientId client{};
  RequestId request{};
  Position first{}; // queue position for Join, start position for Prepare
  Position last{};  // end position (inclusive) for Prepare
  Deadline deadline{}; // one fixed deadline for whole prepared range
};

// Contract for production replacement: commit() returns only after a record is
// decided in one linearizable quorum history. On takeover, snapshot() returns
// that decided prefix, including any adopted records, in order. This does NOT
// implement quorum replication and is NOT safe to use as a distributed log.
class Journal {
 public:
  virtual ~Journal() = default;
  virtual Record commit(Record proposal) = 0;
  virtual std::vector<Record> snapshot() const = 0;
};
class MemoryJournal final : public Journal {
 public:
  Record commit(Record r) override;
  std::vector<Record> snapshot() const override;
 private:
  mutable std::mutex mu_;
  std::vector<Record> log_;
};

struct Config {
  uint64_t horizon_ns{500'000};
  size_t prepare_batch{32};
  size_t prepared_ahead{64};
};

// A leader commits JOIN and PREPARE records. Release markers are deliberately
// outside the log, and can all vanish on a leader crash.
class Leader {
 public:
  Leader(Journal& journal, const Clock& clock, Config config = {});
  Ticket join(LockId lock, ClientId client, RequestId request);
  // Returns tickets *newly* prepared with the same chosen deadline.
  std::vector<Ticket> prepare(LockId lock, size_t max_count);
  std::optional<Ticket> ticket(LockId lock, Position position) const;
  std::vector<Ticket> ordered(LockId lock) const;
  // Observe a release slot. A positive value can justify early activation
  // ONLY with a certifying marker (e.g. remote CAS) and a client-after-
  // quiescence contract. Ordinary RDMA WRITE is a hint, not an atomic proof.
  void observe(const ReleaseMark& mark);
  bool may_start(LockId lock, Position position) const;
  Position prepared_count(LockId lock) const;
  // Fully rebuild state from committed history, but drop volatile markers.
  void recover();
 private:
  struct State {
    std::vector<Ticket> queue;
    std::map<std::pair<ClientId,RequestId>, Position> requests;
    size_t prepared{0};
    std::unordered_map<Position, GrantId> released;
  };
  void replay(const Record& r);
  static bool add_overflows(uint64_t a, uint64_t b);
  Journal& journal_;
  const Clock& clock_;
  Config config_;
  mutable std::mutex mu_;
  std::unordered_map<LockId, State> state_;
};

// On client restart all positions are WAITING and old ClientId cannot be used.
// Production must persist retired states or use a new incarnation ID.
class Client {
 public:
  enum class Status : uint8_t { Waiting, Holding, Retired };
  explicit Client(ClientId id) : id_(id) {}
  void on_prepared(const Ticket& ticket);
  bool activate_handoff(const Handoff& h, const Clock& clock);
  bool activate_start(const Ticket& t, bool leader_authorized,
                      const Clock& clock);
  // Caller promises that data effects and all in-flight operations completed.
  // Returns a marker and optional direct handoff. Neither waits for the other.
  struct Release { ReleaseMark mark; std::optional<Handoff> handoff; };
  Release retire(const Ticket& current, const std::optional<Ticket>& next);
  Status status(GrantId id) const;
  // Gate every protected operation and its completion. Caller additionally
  // enforces that effects do not persist after deadline.
  bool may_use(const Ticket& t, const Clock& clock) const;
 private:
  struct Local { Ticket ticket; Status status{Status::Waiting}; };
  bool activate(const Ticket& t, const Clock& clock);
  ClientId id_;
  std::unordered_map<GrantId, Local> grants_;
};

} // namespace mucql
