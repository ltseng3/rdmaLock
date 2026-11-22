#include "mucql/verbs_paxos.hpp"
#include "mucql/core.hpp"
#include <iostream>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
using namespace mucql;
struct TestClock final:Clock {Bounds now()const override{return {100,101};}};
int main(int argc,char**argv){
  try{
    if(argc<3){std::cerr<<"Usage:\n  mucql_rdma_paxos server <tcp-port> [device] [--allow-experimental-takeover]\n"
                           "  mucql_rdma_paxos client <ballot> <count> <host:port> <host:port> <host:port> [device] [batch=1..32]\n";
      return 2;}
    RdmaPaxosOptions o;
    if(const char* p=std::getenv("MUCQL_IB_PORT"))o.port=static_cast<uint8_t>(std::stoul(p));
    if(const char* p=std::getenv("MUCQL_PAXOS_SLOTS")){
      const auto n=std::stoull(p);
      if(n<16||n>1000000)throw std::invalid_argument("MUCQL_PAXOS_SLOTS must be 16..1000000");
      o.slots=static_cast<uint32_t>(n);
    }
    if(std::string(argv[1])=="server"){
      for(int i=3;i<argc;i++){
        if(std::string(argv[i])=="--allow-experimental-takeover")o.allow_experimental_takeover=true;
        else o.device=argv[i];
      }
      serve_rdma_paxos(static_cast<uint16_t>(std::stoul(argv[2])),o);
    }else if(std::string(argv[1])=="client"){
      if(argc<7)return 2;
      auto ballot=std::stoull(argv[2]);auto count=std::stoull(argv[3]);
      if(argc>=8)o.device=argv[7];
      size_t batch=argc>=9?std::stoull(argv[8]):1;
      if(batch==0||batch>32)throw std::invalid_argument("ordering batch must be 1..32");
      if(count>o.slots-1)throw std::invalid_argument("too many records for current journal slots");
      std::vector<std::shared_ptr<Acceptor>> reps;
      for(int i=4;i<7;i++){
        std::string hp=argv[i];auto sep=hp.rfind(':');if(sep==std::string::npos)throw std::invalid_argument("host:port");
        reps.push_back(std::make_shared<RdmaAcceptor>(hp.substr(0,sep),
                     static_cast<uint16_t>(std::stoul(hp.substr(sep+1))),o));
      }
      LocalQuorum q(reps);PaxosJournal j(q,ballot);j.take_over();
      TestClock clock;Leader leader(j,clock);
      auto start=std::chrono::steady_clock::now();
      for(uint64_t i=0;i<count;){
        std::vector<std::pair<ClientId,RequestId>> reqs;
        for(uint64_t k=0;k<batch && i<count;++k,++i)
          reqs.emplace_back(ballot*1'000'000ULL+i,1);
        leader.join_batch(7,reqs);
      }
      const double secs=std::chrono::duration<double>(
          std::chrono::steady_clock::now()-start).count();
      std::cout<<"One-sided RDMA phase2 committed="<<count
               <<" batch="<<batch
               <<" seconds="<<secs<<" ordered_entries_per_sec="<<count/secs<<'\n';
    }else return 2;
  }catch(const std::exception&e){std::cerr<<"ERROR: "<<e.what()<<'\n';return 1;}
}
