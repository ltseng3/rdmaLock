#include "mucql/paxos.hpp"
#include <cassert>
#include <iostream>
#include <set>
using namespace mucql;
struct FixedClock:Clock {Bounds now() const override{return {100,101};}};
struct Deferred:Acceptor {
  std::map<uint64_t,std::vector<bool>> replies;
  std::set<uint64_t> ready;
  void submit_batch(uint64_t id,uint64_t ballot,const std::vector<Record>& records) override {
    Acceptor::begin_accept_batch(ballot,records);
    replies[id]=Acceptor::finish_accept_batch();
  }
  std::optional<std::vector<bool>> poll_batch(uint64_t id) override {
    if(!ready.count(id))return std::nullopt;
    auto result=replies.at(id);replies.erase(id);return result;
  }
};
struct Fixture {
  std::vector<std::shared_ptr<Deferred>> replicas;
  LocalQuorum quorum;
  Fixture():quorum(make()){}
  std::vector<std::shared_ptr<Acceptor>> make(){
    std::vector<std::shared_ptr<Acceptor>> out;
    for(int i=0;i<3;++i){auto r=std::make_shared<Deferred>();replicas.push_back(r);out.push_back(r);}
    return out;
  }
  void ready(uint64_t id,size_t n=3){for(size_t i=0;i<n;++i)replicas[i]->ready.insert(id);}
};
static Record join(uint64_t key){Record r{};r.lock=key;r.client=key;r.request=1;return r;}
int main(){
  {
    Fixture f;PaxosJournal journal(f.quorum,100);journal.take_over();
    FixedClock clock;Leader leader(journal,clock);
    auto first=journal.submit_batch(leader.stage_join_many({{7,1,1}}));
    assert(leader.ordered(7).empty()&&!leader.can_stage({7}));
    bool blocked=false;
    try{leader.stage_prepare_many({7},1);}catch(const std::logic_error&){blocked=true;}
    assert(blocked);
    blocked=false;
    try{leader.join(7,2,1);}catch(const std::logic_error&){blocked=true;}
    assert(blocked);
    blocked=false;
    try{leader.prepare(7,1);}catch(const std::logic_error&){blocked=true;}
    assert(blocked);
    auto second=journal.submit_batch(leader.stage_join_many({{8,2,1}}));
    f.ready(second);
    assert(journal.poll_commits().empty()&&journal.snapshot().empty());
    // The later ready batch cannot cross a gap, but a third submission can
    // enter while the first batch is still awaiting any completion.
    auto third=journal.submit_batch(leader.stage_join_many({{9,3,1}}));f.ready(third);
    f.ready(first,2); // quorum publishes without waiting for replica 3
    auto commits=journal.poll_commits();assert(commits.size()==3);
    for(size_t i=0;i<commits.size();++i){
      assert(commits[i].records[0].index==i+1);
      auto tickets=leader.apply_committed(commits[i].records);
      assert(tickets.size()==1&&!tickets[0].deadline);
    }
    assert(journal.available_slots()==31&&!journal.pipeline_idle());
    auto prep=journal.submit_batch(leader.stage_prepare_many({7,8,9},1));
    assert(!leader.ticket(7,0)->deadline);
    f.ready(prep);commits=journal.poll_commits();assert(commits.size()==1);
    auto tickets=leader.apply_committed(commits[0].records);
    for(auto& t:tickets)assert(t.deadline&&t.grant_id>=1&&t.grant_id<=3);
    f.ready(first);assert(journal.poll_commits().empty()&&journal.pipeline_idle());
    auto stats=journal.phase2_stats();
    assert(stats.max_inflight_batches>=3&&stats.overlap_submissions>=2);
    PaxosJournal recovered(f.quorum,101);recovered.take_over();Leader replayed(recovered,clock);
    assert(replayed.ticket(7,0)->deadline==tickets[0].deadline);
  }
  {
    Fixture f;PaxosJournal journal(f.quorum,200);journal.take_over();
    std::vector<uint64_t> ids;
    for(size_t i=0;i<32;++i)ids.push_back(journal.submit_batch({join(i+1)}));
    assert(journal.available_slots()==0&&journal.submitted_count()==32);
    bool full=false;try{journal.submit_batch({join(33)});}catch(const std::logic_error&){full=true;}
    assert(full&&journal.submitted_count()==32);
    for(auto id:ids)f.ready(id,2);
    assert(journal.poll_commits().size()==32&&journal.available_slots()==0);
    // Published-but-not-drained buffers cannot be overwritten.
    f.ready(ids[0]);assert(journal.poll_commits().empty()&&journal.available_slots()==1);
    auto next=journal.submit_batch({join(33)});f.ready(next);
    assert(journal.poll_commits().front().records.front().index==33);
  }
  {
    Fixture f;PaxosJournal journal(f.quorum,300);journal.take_over();
    auto a=journal.submit_batch({join(1),join(2)});
    auto b=journal.submit_batch({join(3)});f.ready(b);
    // Each entry needs its own majority, not a majority for the batch.
    f.replicas[0]->replies[a]={true,false};
    f.replicas[1]->replies[a]={false,true};
    f.replicas[2]->replies[a]={false,false};f.ready(a);
    bool failed=false;try{journal.poll_commits();}catch(const std::runtime_error&){failed=true;}
    assert(failed&&journal.snapshot().empty());
    failed=false;try{journal.submit_batch({join(4)});}catch(const std::runtime_error&){failed=true;}
    assert(failed);
    PaxosJournal recovered(f.quorum,301);recovered.take_over();
    assert(recovered.snapshot().size()==3); // adopt accepted uncertain tail
  }
  std::cout<<"PASS async overlap, ordered quorum publication, credits, dependencies and recovery\n";
}
