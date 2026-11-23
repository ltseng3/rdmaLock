#include "mucql/verbs_paxos.hpp"
#include "mucql/core.hpp"
#include <iostream>
#include <cstdlib>
#include <thread>
#include <chrono>
#include <stdexcept>
using namespace mucql;
struct ClockImpl :Clock {Bounds now()const override{return {100,101};}};
static void require(bool x){if(!x)throw std::runtime_error("mock RDMA Paxos test failed");}
int main(){
  RdmaPaxosOptions o;o.slots=64;o.allow_experimental_takeover=true;
  for(int i=0;i<3;i++){
    auto port=static_cast<uint16_t>(29660+i);
    std::thread([o,port]{serve_rdma_paxos(port,o);}).detach();
  }
  // Under the injected mock-MR latency, replicas must provision their
  // accepted buffers BEFORE advertising phase-1 TCP readiness.
  if(const char* delay=std::getenv("MUCQL_MOCK_ACCEPTED_MR_DELAY_MS"))
    std::this_thread::sleep_for(std::chrono::milliseconds(std::stoul(delay)+300));
  else
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
  std::vector<std::shared_ptr<Acceptor>> reps;
  for(int i=0;i<3;i++)reps.push_back(std::make_shared<RdmaAcceptor>("127.0.0.1",29660+i,o));
  LocalQuorum q(reps);PaxosJournal j(q,10);j.take_over();
  // A phase-2 QP must remain live even if the TCP bootstrap socket is idle.
  if(const char* idle=std::getenv("MUCQL_TEST_PAXOS_IDLE_MS"))
    std::this_thread::sleep_for(std::chrono::milliseconds(std::stoul(idle)));
  ClockImpl clk;Leader a(j,clk);
  for(int i=0;i<6;i++){auto t=a.join(7,100+i,1);require(t.position==static_cast<uint64_t>(i));}
  auto prepared=a.prepare(7,6);require(prepared.size()==6);
  // One RDMA batch contains different lock IDs, not repeated log entries
  // for a single hot key. Both the JOIN and PREPARE stages post their
  // payload/CAS WRs to each replica before draining the CQ.
  j.set_phase2_window(16);
  std::vector<JoinRequest> cross;
  std::vector<LockId> keys;
  for(int i=0;i<8;i++){
    cross.push_back({uint64_t(1000+i),uint64_t(200+i),1});
    keys.push_back(uint64_t(1000+i));
  }
  auto joined_cross=a.join_many(cross);
  auto prepared_cross=a.prepare_many(keys,1);
  require(joined_cross.size()==8 && prepared_cross.size()==8);
  for(size_t i=0;i<8;i++)
    require(joined_cross[i].grant_id==prepared_cross[i].grant_id &&
            prepared_cross[i].deadline>0);
  require(j.phase2_stats().max_window==8);
  require(j.phase2_stats().multi_record_windows>=2);
  require(j.snapshot().size()==23);
  std::vector<std::shared_ptr<Acceptor>> newer;
  for(int i=0;i<3;i++)newer.push_back(std::make_shared<RdmaAcceptor>("127.0.0.1",29660+i,o));
  LocalQuorum q2(newer);PaxosJournal newer_j(q2,20);newer_j.take_over();
  Leader recovered(newer_j,clk);require(recovered.ordered(7).size()==6);
  require(recovered.ordered(7)[5].deadline==prepared[5].deadline);
  for(size_t i=0;i<8;i++){
    const auto q=recovered.ordered(keys[i]);
    require(q.size()==1 && q[0].grant_id==joined_cross[i].grant_id);
    require(q[0].deadline==prepared_cross[i].deadline);
  }
  require(recovered.join(7,199,1).position==6);
  bool lost_leadership=false;
  try{a.join(7,888,1);}catch(const std::exception&){lost_leadership=true;}
  require(lost_leadership);
  std::cout<<"RDMA one-sided phase-2 simulated quorum/recovery PASS\n";
  std::cout.flush();
  // The mock-only acceptors are detached infinite servers; standard static
  // destruction races them. End the isolated test process after verification.
  std::_Exit(0);
}
