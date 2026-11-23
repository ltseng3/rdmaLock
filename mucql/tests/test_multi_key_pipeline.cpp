#include "mucql/core.hpp"
#include "mucql/paxos.hpp"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>
using namespace mucql;

struct FixedClock final:Clock {Bounds now() const override{return {100,101};}};

struct InspectAcceptor final:Acceptor {
  size_t multi_batches=0,largest=0;
  void begin_accept_batch(uint64_t ballot,const std::vector<Record>& batch) override {
    largest=std::max(largest,batch.size());
    if(batch.size()>1)++multi_batches;
    Acceptor::begin_accept_batch(ballot,batch);
  }
};

int main(){
  FixedClock clock;
  std::vector<std::shared_ptr<Acceptor>> replicas;
  std::vector<std::shared_ptr<InspectAcceptor>> observed;
  for(int i=0;i<3;i++){
    auto r=std::make_shared<InspectAcceptor>();
    replicas.push_back(r);observed.push_back(r);
  }
  LocalQuorum quorum(replicas);
  PaxosJournal journal(quorum,100);journal.take_over();journal.set_phase2_window(16);
  Config cfg;cfg.prepared_ahead=2;cfg.prepare_batch=1;
  Leader leader(journal,clock,cfg);
  std::vector<JoinRequest> requests;
  std::vector<LockId> locks;
  for(int i=0;i<16;i++){
    requests.push_back({uint64_t(1000+i),uint64_t(1+i),1});
    locks.push_back(uint64_t(1000+i));
  }
  const auto joined=leader.join_many(requests);
  assert(joined.size()==16);
  for(size_t i=0;i<joined.size();i++){
    assert(joined[i].lock==locks[i] && joined[i].position==0);
    assert(joined[i].grant_id==i+1 && !joined[i].predecessor_id);
    assert(!joined[i].deadline);
  }
  const auto prepared=leader.prepare_many(locks,1);
  assert(prepared.size()==16);
  for(size_t i=0;i<prepared.size();i++){
    assert(prepared[i].grant_id==joined[i].grant_id);
    assert(prepared[i].deadline>0);
    assert(leader.ticket(locks[i],0)->deadline==prepared[i].deadline);
  }
  auto stats=journal.phase2_stats();
  assert(stats.max_window==16 && stats.windows==2);
  assert(stats.multi_record_windows==2 && stats.records==32);
  for(const auto& replica:observed)
    assert(replica->largest==16 && replica->multi_batches==2);

  // A cross-key batch may also contain several JOINs for one key.
  const std::vector<JoinRequest> mixed={
    {1000,41,1},{1001,42,1},{1000,43,1},{1000,44,1}
  };
  auto next=leader.join_many(mixed);
  assert(next.size()==4);
  assert(next[0].position==1 && next[0].predecessor_id==joined[0].grant_id);
  assert(next[1].position==1 && next[1].predecessor_id==joined[1].grant_id);
  assert(next[2].position==2 && next[2].predecessor_id==next[0].grant_id);
  assert(next[3].position==3 && next[3].predecessor_id==next[2].grant_id);
  // One range per key, no duplicate PREPARE for the same key in one batch.
  assert(leader.prepare_many({1000,1001},1).size()==2);
  assert(leader.prepare_many({1000,1001},1).empty()); // lookahead bound
  leader.observe({1000,0,joined[0].grant_id});
  auto more=leader.prepare_many({1000,1001},1);
  assert(more.size()==1 && more[0].grant_id==next[2].grant_id);
  const auto ordered=leader.ordered(1000);
  for(size_t i=1;i<ordered.size();i++)
    assert(ordered[i].predecessor_id==ordered[i-1].grant_id);

  auto count=journal.decided_count();
  try{leader.join_many({{1000,41,1}});assert(false);}
  catch(const std::invalid_argument&){}
  try{leader.join_many({{1000,99,1},{1000,99,1}});assert(false);}
  catch(const std::invalid_argument&){}
  try{leader.prepare_many({1000,1000},1);assert(false);}
  catch(const std::invalid_argument&){}
  assert(journal.decided_count()==count);

  // Failed quorum batches cannot expose a JOIN ticket or PREPARE.
  replicas[1]->crash(true);replicas[2]->crash(true);
  bool failed=false;
  try{leader.join_many({{77777,3,1},{88888,4,1}});}
  catch(const std::runtime_error&){failed=true;}
  assert(failed && leader.ordered(77777).empty());
  assert(journal.decided_count()==count);

  // All prior cross-key JOIN/PREPARE records replay on takeover unchanged.
  replicas[1]->crash(false);replicas[2]->crash(false);
  PaxosJournal recovered_j(quorum,101);recovered_j.take_over();
  Leader recovered(recovered_j,clock,cfg);
  for(int i=0;i<16;i++){
    const auto q=recovered.ordered(locks[i]);
    assert(!q.empty() && q[0].grant_id==joined[i].grant_id);
    assert(q[0].deadline==prepared[i].deadline);
  }
  std::cout<<"PASS cross-lock RDMA Phase-2 batching, dependences, and recovery\n";
}
