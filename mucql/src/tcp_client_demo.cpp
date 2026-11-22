#include "mucql/tcp_transport.hpp"
#include <chrono>
#include <iostream>
#include <sstream>
#include <stdexcept>
using namespace mucql;
struct ClockStub final : Clock{
  Bounds now() const override{return {100,102};}
};
int main(int argc,char** argv){
  if(argc<6){std::cerr<<"Usage: mucql_paxos_demo <ballot> <count> host:port host:port host:port\n";return 2;}
  uint64_t ballot=std::stoull(argv[1]);int n=std::stoi(argv[2]);
  std::vector<std::shared_ptr<Acceptor>> reps;
  for(int i=3;i<argc;i++){
    std::string hp=argv[i];auto delim=hp.rfind(':');
    if(delim==std::string::npos)throw std::invalid_argument("host:port required");
    reps.push_back(std::make_shared<TcpAcceptor>(hp.substr(0,delim),
                static_cast<uint16_t>(std::stoul(hp.substr(delim+1)))));
  }
  LocalQuorum q(reps);PaxosJournal j(q,ballot);j.take_over();
  ClockStub clock;Leader l(j,clock);
  auto start=std::chrono::steady_clock::now();
  for(int i=0;i<n;i++)l.join(7,ballot*1'000'000ULL+i,1);
  auto ns=std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now()-start).count();
  std::cout<<"committed="<<n<<" seconds="<<ns/1e9
           <<" proposals_per_sec="<<n/(ns/1e9)<<"\n";
}
