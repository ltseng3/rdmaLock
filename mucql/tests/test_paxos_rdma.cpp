#include "mucql/verbs_paxos.hpp"
#include "mucql/core.hpp"
#include <iostream>
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
  std::this_thread::sleep_for(std::chrono::milliseconds(120));
  std::vector<std::shared_ptr<Acceptor>> reps;
  for(int i=0;i<3;i++)reps.push_back(std::make_shared<RdmaAcceptor>("127.0.0.1",29660+i,o));
  LocalQuorum q(reps);PaxosJournal j(q,10);j.take_over();
  ClockImpl clk;Leader a(j,clk);
  for(int i=0;i<6;i++){auto t=a.join(7,100+i,1);require(t.position==static_cast<uint64_t>(i));}
  auto prepared=a.prepare(7,6);require(prepared.size()==6);
  require(j.snapshot().size()==7);
  std::vector<std::shared_ptr<Acceptor>> newer;
  for(int i=0;i<3;i++)newer.push_back(std::make_shared<RdmaAcceptor>("127.0.0.1",29660+i,o));
  LocalQuorum q2(newer);PaxosJournal newer_j(q2,20);newer_j.take_over();
  Leader recovered(newer_j,clk);require(recovered.ordered(7).size()==6);
  require(recovered.ordered(7)[5].deadline==prepared[5].deadline);
  require(recovered.join(7,199,1).position==6);
  bool lost_leadership=false;
  try{a.join(7,888,1);}catch(const std::exception&){lost_leadership=true;}
  require(lost_leadership);
  std::cout<<"RDMA one-sided phase-2 simulated quorum/recovery PASS\n";
}
