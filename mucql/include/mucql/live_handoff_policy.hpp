#pragma once
// LIVE coordinator routing policy for a committed PREPARE ticket.
//
// A successor with a nonzero position may start directly ONLY if the
// coordinator has already verified the exact predecessor's release marker.
// Otherwise the predecessor's retired-worker thread must deliver an RDMA CAS.
// Merely receiving a PREPARE, knowing the predecessor's deadline, or seeing
// an unrelated completed grant never authorizes a non-head successor.
#include "mucql/core.hpp"
#include <optional>
#include <set>
#include <stdexcept>

namespace mucql {

enum class PreparedSuccessorRoute { HeadStart, CertifiedLateStart, DirectHandoff };

inline PreparedSuccessorRoute choose_prepared_successor_route(
    const Ticket& successor, const std::optional<Ticket>& predecessor,
    const std::set<GrantId>& certified_releases, bool enable_certified_late_start) {
  if(!successor.lock || !successor.client || !successor.grant_id || !successor.deadline)
    throw std::logic_error("invalid prepared successor ticket");
  if(successor.position==0) {
    if(successor.predecessor_id || predecessor)
      throw std::logic_error("head ticket has a predecessor");
    return PreparedSuccessorRoute::HeadStart;
  }
  if(!successor.predecessor_id || !predecessor ||
     predecessor->lock!=successor.lock ||
     predecessor->position!=successor.position-1 ||
     predecessor->grant_id!=successor.predecessor_id ||
     predecessor->grant_id==successor.grant_id ||
     !predecessor->deadline)
    throw std::logic_error("successor's prepared predecessor mismatch");
  if(enable_certified_late_start &&
     certified_releases.count(successor.predecessor_id))
    return PreparedSuccessorRoute::CertifiedLateStart;
  return PreparedSuccessorRoute::DirectHandoff;
}

} // namespace mucql
