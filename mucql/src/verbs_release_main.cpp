#include "mucql/verbs_release.hpp"
#include <iostream>
#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string>
using namespace mucql;
int main(int argc,char**argv){
  try{
    if(argc<3){std::cerr<<"Usage: mucql_verbs_release server <tcp-port> [ib-device] [slots]\n"
              <<"       mucql_verbs_release client <host> <tcp-port> <count> [ib-device] [cas|write]\n";
      return 2;}
    ReleaseEndpointOptions opts;
    if(const char* p=std::getenv("MUCQL_IB_PORT"))opts.port=static_cast<uint8_t>(std::stoul(p));
    if(std::string(argv[1])=="server"){
      auto port=static_cast<uint16_t>(std::stoul(argv[2]));
      if(argc>=4)opts.device=argv[3];if(argc>=5)opts.slots=std::stoul(argv[4]);
      VerbsReleaseServer server(opts);server.listen_forever(port);
    }else if(std::string(argv[1])=="client"){
      if(argc<5)return 2;
      std::string host=argv[2];auto port=static_cast<uint16_t>(std::stoul(argv[3]));
      size_t count=std::stoull(argv[4]);if(argc>=6)opts.device=argv[5];
      ReleaseMode mode=(argc>=7&&std::string(argv[6])=="write")?ReleaseMode::WriteHint:ReleaseMode::AtomicCas;
      VerbsReleaseClient c(host,port,opts);
      auto start=std::chrono::steady_clock::now();
      for(size_t i=0;i<count;i++){
        // Distinct slots: a CAS marker is write-once and cannot be overwritten.
        size_t slot=i%c.slots();
        c.post(slot,i+1,mode);
        if(!c.complete())throw std::runtime_error("release slot collision");
      }
      auto secs=std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();
      std::cout<<"RDMA release mode="<<(mode==ReleaseMode::AtomicCas?"CAS":"WRITE_HINT")
               <<" operations="<<count<<" seconds="<<secs<<" ops_per_sec="<<count/secs<<'\n';
    }else return 2;
    return 0;
  }catch(const std::exception& e){std::cerr<<"ERROR: "<<e.what()<<'\n';return 1;}
}
