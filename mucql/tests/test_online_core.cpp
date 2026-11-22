#include "mucql/core.hpp"
#include "mucql/paxos.hpp"
#include <cassert>
#include <iostream>
#include <memory>
#include <vector>
using namespace mucql;
struct Nonexpiring:Clock {Bounds now()const override{return {0,0};}};
int main(){
  Nonexpiring clk;
  LocalQuorum q(3);
  PaxosJournal journal(q,7001);journal.take_over();journal.set_phase2_window(1);
  Config config;config.horizon_ns=1000000000;config.prepare_batch=4;config.prepared_ahead=8;
  Leader leader(journal,clk,config);
  std::vector<Ticket> original;
  for(uint64_t i=0;i<1000;i++){
    auto next=leader.join(1,1+i%10,i+1);
    assert(next.position==i);
    auto new_prepared=leader.prepare(1,4);
    if(i<8){assert(!new_prepared.empty());}
    else if(new_prepared.empty()){
      // The predecessor's certified release must be observed before this
      // bounded window advances. No timeout-driven stealing is permitted.
      assert(leader.prepared_count(1)-i <= 8);
    }
    if(i){
      leader.observe({1,i-1,original.back().grant_id});
      auto prepared=leader.prepare(1,4);
      (void)prepared;
    }
    original.push_back(next);
  }
  // Restore from the same quorum. Volatile release observations vanish;
  // protected positions remain prepared/ordered, but not automatically free.
  PaxosJournal restored(q,7002);restored.take_over();
  Leader recovered(restored,clk,config);
  assert(recovered.ordered(1).size()==1000);
  assert(recovered.prepared_count(1)<=1000);
  assert(!recovered.may_start(1,1));
  auto again=recovered.join(1,1,1);
  assert(again.grant_id==original[0].grant_id);
  assert(restored.decided_count()==journal.decided_count());
  std::cout<<"PASS online bounded replay without release-marker persistence\n";
}
