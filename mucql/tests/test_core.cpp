#include "mucql/core.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <iostream>
#include <random>
#include <thread>
#include <vector>
using namespace mucql;

struct ManualClock final: Clock {
  uint64_t t=100, uncertainty=2;
  Bounds now() const override { return {t-uncertainty,t+uncertainty}; }
  void set(uint64_t x){t=x;}
};

void basic_handoff() {
  MemoryJournal j; ManualClock clock;
  Config cfg;cfg.horizon_ns=250; cfg.prepare_batch=16;cfg.prepared_ahead=16;
  Leader leader(j,clock,cfg);
  std::vector<Client> clients;
  for(int i=0;i<8;i++)clients.emplace_back(100+i);
  std::vector<Ticket> q;
  for(int i=0;i<8;i++) {
    q.push_back(leader.join(42,100+i,1));
    assert(q.back().position==static_cast<uint64_t>(i));
    assert(leader.join(42,100+i,1).grant_id==q.back().grant_id);
  }
  auto prep=leader.prepare(42,8);assert(prep.size()==8);
  for(int i=0;i<8;i++) {
    q[i]=prep[i]; clients[i].on_prepared(q[i]);
    assert(q[i].deadline==q[0].deadline);
    if(i>0)assert(q[i].predecessor_id==q[i-1].grant_id);
  }
  assert(leader.may_start(42,0));assert(!leader.may_start(42,1));
  assert(clients[0].activate_start(q[0],true,clock));
  for(int i=0;i<7;i++) {
    auto r=clients[i].retire(q[i],q[i+1]);
    assert(!clients[i].may_use(q[i],clock));
    assert(r.handoff.has_value());
    // Deliver direct notification *before* release marker, or lose marker.
    assert(clients[i+1].activate_handoff(*r.handoff,clock));
    assert(!clients[i+1].activate_handoff(*r.handoff,clock));
    if(i%2==0) leader.observe(r.mark);
    assert(!clients[i].activate_start(q[i],true,clock));
  }
  assert(clients[7].status(q[7].grant_id)==Client::Status::Holding);
  // All volatile release marks vanish on leader change.
  Leader recovered(j,clock,cfg);
  assert(!recovered.may_start(42,7));
  // Previous holder is gone: expiry permits *duplicate* START of same ticket.
  clock.set(q[6].deadline+5);
  assert(recovered.may_start(42,7));
  assert(!clients[7].activate_start(q[7],true,clock));
  assert(!clients[7].may_use(q[7],clock));
}

void late_preparation_and_expiry() {
  MemoryJournal j;ManualClock clk;Config cfg;cfg.horizon_ns=20;
  Leader l(j,clk,cfg);
  auto a=l.join(1,1,1);auto b=l.join(1,2,1);
  assert(!a.deadline && !b.deadline);
  clk.set(1000);
  auto prep=l.prepare(1,2);assert(prep.size()==2);
  assert(prep[0].deadline==1022);
  assert(!l.may_start(1,1));
  clk.set(1025); // lower = 1023 > predecessor expiry
  assert(l.may_start(1,1));
  Client B(2);B.on_prepared(prep[1]);
  assert(!B.activate_start(prep[1],true,clk)); // expired ticket is unusable
  // Same lock's new ticket must have nondecreasing deadline.
  l.join(1,3,1);clk.set(2000);
  auto next=l.prepare(1,1);assert(next.size()==1);
  assert(next[0].deadline>prep[1].deadline);
}

void wrong_handoff_and_stale_markers() {
  MemoryJournal j;ManualClock clk; Leader l(j,clk);
  l.join(7,1,1);l.join(7,2,1);auto q=l.prepare(7,2);
  Client A(1),B(2); A.on_prepared(q[0]);B.on_prepared(q[1]);
  assert(A.activate_start(q[0],true,clk));
  auto hand=A.retire(q[0],q[1]);
  auto bad=*hand.handoff;bad.predecessor_id++;
  assert(!B.activate_handoff(bad,clk));
  l.observe(ReleaseMark{7,0,0xfeed});
  assert(!l.may_start(7,1));
  l.observe(hand.mark);
  assert(l.may_start(7,1));
  assert(B.activate_start(q[1],true,clk));
  assert(!B.activate_handoff(*hand.handoff,clk));
}

void bounded_pipeline() {
  MemoryJournal j; ManualClock clk;
  Config c;c.horizon_ns=100;c.prepare_batch=8;c.prepared_ahead=12;
  Leader l(j,clk,c);
  for(int i=0;i<40;i++)l.join(88,1000+i,123);
  assert(l.prepare(88,8).size()==8);
  assert(l.prepare(88,8).size()==4); // should cap at 12
  assert(l.prepare(88,8).empty());
  auto q=l.ordered(88);
  for(int i=0;i<8;i++)l.observe({88,static_cast<Position>(i),q[i].grant_id});
  assert(l.prepare(88,8).size()==8);
  auto r=l.ordered(88);
  for(int i=1;i<20;i++)assert(r[i].deadline>=r[i-1].deadline);
}

void thread_safe_ordering() {
  MemoryJournal j;ManualClock clk;Leader l(j,clk);
  constexpr int n=8, per=100;
  std::vector<std::thread> threads;
  for(int c=0;c<n;c++)threads.emplace_back([&,c]{
    for(int k=0;k<per;k++){
      auto q=l.join(99,static_cast<ClientId>(c+1),k+1);
      assert(q.client==static_cast<ClientId>(c+1));
    }
  });
  for(auto& t:threads)t.join();
  auto q=l.ordered(99);assert(q.size()==n*per);
  for(size_t i=0;i<q.size();i++){
    assert(q[i].position==i);
    assert(i==0 || q[i].predecessor_id==q[i-1].grant_id);
  }
  Leader other(j,clk);assert(other.ordered(99).size()==q.size());
}
int main() {
  basic_handoff();late_preparation_and_expiry();wrong_handoff_and_stale_markers();
  bounded_pipeline();thread_safe_ordering();
  std::cout<<"PASS: exclusive core, recovery, stale markers, deadlines, 8-thread ordering\n";
}
