#include "mucql/verbs_paxos.hpp"
#include <cstdlib>
#include <iostream>
#include <unistd.h>
#include <stdexcept>
#include <string>
using namespace mucql;
int main(){
  if(setenv("MUCQL_MOCK_REJECT_ACCEPTED_MR","1",1)!=0)
    throw std::runtime_error("setenv failed");
  RdmaPaxosOptions o;o.device="mock0";o.slots=64;
  // serve() must reject unavailable pinned memory before opening/listening.
  // If it unexpectedly enters the accept loop this process is stuck, so a
  // five-second alarm converts that regression into a deterministic failure.
  alarm(5);
  try{
    serve_rdma_paxos(0,o);
    std::cerr<<"unexpected: server became ready despite MR failure\n";
    return 1;
  }catch(const std::exception& e){
    const std::string message=e.what();
    if(message.find("accepted MR registration")==std::string::npos ||
       message.find("slots=64")==std::string::npos ||
       message.find("bytes=8192")==std::string::npos){
      std::cerr<<"unexpected preflight exception: "<<message<<"\n";
      return 1;
    }
    std::cout<<"RDMA accepted-log MR preflight rejected unavailable memory: PASS\n";
    return 0;
  }
}
