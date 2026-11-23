#include "mucql/live_handoff_policy.hpp"
#include <cassert>
#include <iostream>
#include <set>
#include <stdexcept>
using namespace mucql;

struct FixedClock : Clock {Bounds now() const override{return {100,101};}};

int main() {
  FixedClock clock;
  MemoryJournal journal;
  Leader leader(journal,clock);
  Client first(1),late(2),other(3);
  const Ticket a=leader.join(77,1,1);
  const Ticket prepared_a=leader.prepare(77,1).front();
  first.on_prepared(prepared_a);
  assert(choose_prepared_successor_route(prepared_a,{}, {},true)==
         PreparedSuccessorRoute::HeadStart);
  assert(first.activate_start(prepared_a,true,clock));
  // The predecessor completed BEFORE its successor even JOINed.
  const auto released=first.retire(prepared_a,std::nullopt);
  std::set<GrantId> certified;
  const Ticket b=leader.join(77,2,1);
  assert(b.position==1&&b.predecessor_id==a.grant_id);
  const Ticket prepared_b=leader.prepare(77,1).front();
  late.on_prepared(prepared_b);
  assert(!leader.may_start(77,1)); // not yet observed by Leader
  assert(choose_prepared_successor_route(prepared_b,prepared_a,certified,true)==
         PreparedSuccessorRoute::DirectHandoff);
  assert(!late.activate_start(prepared_b,false,clock));
  // A remote READ of grant a's completed release marker is required before
  // adding this exact grant to the coordinator's certified set.
  certified.insert(released.mark.grant_id);
  assert(choose_prepared_successor_route(prepared_b,prepared_a,certified,false)==
         PreparedSuccessorRoute::DirectHandoff); // legacy control
  assert(choose_prepared_successor_route(prepared_b,prepared_a,certified,true)==
         PreparedSuccessorRoute::CertifiedLateStart);
  assert(late.activate_start(prepared_b,true,clock));
  assert(!late.activate_start(prepared_b,true,clock)); // no duplicate holding
  assert(late.status(prepared_b.grant_id)==Client::Status::Holding);

  // A different, completed grant cannot authorize this successor.
  const Ticket c=leader.join(77,3,1);
  const Ticket prepared_c=leader.prepare(77,1).front();
  other.on_prepared(prepared_c);
  assert(choose_prepared_successor_route(prepared_c,prepared_b,certified,true)==
         PreparedSuccessorRoute::DirectHandoff);
  assert(!other.activate_start(prepared_c,false,clock));
  const auto handoff=late.retire(prepared_b,prepared_c);
  assert(handoff.handoff.has_value());
  // Even when the predecessor released before its notification arrived,
  // the uncertified path can still activate via the predecessor's CAS.
  assert(other.activate_handoff(*handoff.handoff,clock));
  assert(!other.activate_handoff(*handoff.handoff,clock));
  certified.insert(handoff.mark.grant_id);
  assert(choose_prepared_successor_route(prepared_c,prepared_b,certified,true)==
         PreparedSuccessorRoute::CertifiedLateStart);

  // A mismatched predecessor's lock, position, or grant is never accepted.
  for(int bad=0;bad<3;bad++) {
    auto wrong=prepared_b;
    if(bad==0)wrong.lock++;
    if(bad==1)wrong.position++;
    if(bad==2)wrong.grant_id++;
    bool rejected=false;
    try{(void)choose_prepared_successor_route(prepared_c,wrong,certified,true);}
    catch(const std::logic_error&){rejected=true;}
    assert(rejected);
  }
  bool rejected=false;
  try{(void)choose_prepared_successor_route(prepared_c,{},certified,true);}
  catch(const std::logic_error&){rejected=true;}
  assert(rejected);
  std::cout<<"PASS certified late successor start and fallback handoff routing\n";
}
