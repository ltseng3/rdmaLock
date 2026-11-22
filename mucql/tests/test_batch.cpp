#include "mucql/paxos.hpp"
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <vector>
using namespace mucql;
struct FixedClock final: Clock {Bounds now()const override{return {100,101};}};
int main(){
  FixedClock clk;
  LocalQuorum q(3);
  PaxosJournal j(q,101);j.take_over();
  Leader l(j,clk);
  std::vector<std::pair<ClientId,RequestId>> reqs;
  for(uint64_t i=0;i<32;i++)reqs.emplace_back(i+1,1);
  auto first=l.join_batch(7,reqs);
  assert(first.size()==32);
  for(size_t i=0;i<first.size();i++){
    assert(first[i].position==i);
    assert(first[i].grant_id==i+1);
    assert(first[i].predecessor_id==(i?first[i-1].grant_id:0));
  }
  auto prepared=l.prepare(7,32);assert(prepared.size()==32);
  bool duplicate=false;
  try{l.join_batch(7,{{77,1},{77,1}});}catch(const std::invalid_argument&){duplicate=true;}
  assert(duplicate);assert(l.ordered(7).size()==32);
  PaxosJournal j2(q,102);j2.take_over();
  Leader l2(j2,clk);assert(l2.ordered(7).size()==32);
  q.nodes()[2]->crash(true);
  auto next=l2.join_batch(7,{{101,1},{102,1}});
  assert(next.size()==2&&next[0].position==32&&next[1].position==33);
  q.nodes()[1]->crash(true);
  bool failed=false;
  try{l2.join_batch(7,{{103,1},{104,1}});}catch(const std::runtime_error&){failed=true;}
  assert(failed);
  bool leader_lost=false;
  try{l2.join(7,107,1);}catch(const std::runtime_error&){leader_lost=true;}
  assert(leader_lost);
  q.nodes()[1]->crash(false);
  PaxosJournal j3(q,103);j3.take_over();
  Leader l3(j3,clk);
  assert(l3.ordered(7).size()==36); // adopted unacknowledged batch
  for(size_t i=0;i<36;i++)assert(l3.ordered(7)[i].position==i);
  std::cout<<"PASS batch append, duplicate rejection, quorum failure, takeover adoption\n";
}
