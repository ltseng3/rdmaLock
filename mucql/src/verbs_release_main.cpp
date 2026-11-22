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
              <<"       mucql_verbs_release client <host> <tcp-port> <count> [ib-device] [cas|write]\n"
              <<"       mucql_verbs_release check <host> <tcp-port> [ib-device]\n";
      return 2;}
    ReleaseEndpointOptions opts;
    if(const char* p=std::getenv("MUCQL_IB_PORT"))opts.port=static_cast<uint8_t>(std::stoul(p));
    if(std::string(argv[1])=="server"){
      auto port=static_cast<uint16_t>(std::stoul(argv[2]));
      if(argc>=4)opts.device=argv[3];if(argc>=5)opts.slots=std::stoul(argv[4]);
      VerbsReleaseServer server(opts);server.listen_forever(port);
    }else if(std::string(argv[1])=="check"){
      if(argc<4)return 2;
      if(argc>=5)opts.device=argv[4];
      mucql::VerbsReleaseClient c(argv[2],static_cast<uint16_t>(std::stoul(argv[3])),opts);
      if(c.slots()<16)throw std::runtime_error("check requires >=16 remote slots");
      const auto slot=c.slots()-1;
      const uint64_t a=0x0102030405060708ULL;
      const uint64_t b=0x1122334455667788ULL;
      if(c.read_slot(slot)!=0)throw std::runtime_error("check slot already occupied; restart release server");
      if(c.compare_exchange(slot,0,a)!=0)throw std::runtime_error("CAS initial value mismatch");
      if(c.read_slot(slot)!=a)throw std::runtime_error("CAS failed to install value");
      if(c.compare_exchange(slot,0,b)!=a)throw std::runtime_error("CAS old-value/endianness mismatch");
      if(c.read_slot(slot)!=a)throw std::runtime_error("failed CAS changed target");
      if(c.compare_exchange(slot,a,0)!=a)throw std::runtime_error("CAS clear/old-value mismatch");
      c.write_slot(slot,b);
      if(c.read_slot(slot)!=b)throw std::runtime_error("RDMA READ/WRITE mismatch");
      c.write_slot(slot,0);
      if(c.read_slot(slot)!=0)throw std::runtime_error("check failed to reset slot");
      std::cout<<"RDMA release check CAS_nonzero=PASS READ_WRITE=PASS slot="<<slot<<"\n";
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
