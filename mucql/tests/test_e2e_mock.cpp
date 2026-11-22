// Whole execution in ONE process so fake NIC QP/memory registries are shared.
// This is NOT a cross-process or native RNIC test.
#define MUCQL_E2E_LIBRARY
#include "../src/lock_e2e_main.cpp"
#include <csignal>
#include <exception>
#include <iostream>
#include <thread>
#include <vector>
#include <cstdlib>
int main(int argc,char**argv){
  std::signal(SIGALRM,[](int){std::_Exit(22);});alarm(18);
  try {
    const auto count=argc>1?std::stoull(argv[1]):48ULL;
    const auto nworkers=argc>2?std::stoull(argv[2]):2ULL;
    const auto probe=argc>3?std::stoull(argv[3]):1ULL;
    const auto work=argc>4?std::stoull(argv[4]):10ULL;
    if(nworkers<2||nworkers>4||count<2||count>300)throw std::runtime_error("bad test args");
    mucql::ReleaseEndpointOptions ro;ro.slots=4096;ro.device="mock0";
    auto* release=new mucql::VerbsReleaseServer(ro);
    std::thread([release]{release->listen_forever(29820);}).detach();
    for(int i=0;i<3;i++){
      std::thread([i]{mucql::RdmaPaxosOptions o;o.device="mock0";
        mucql::serve_rdma_paxos(uint16_t(29821+i),o);}).detach();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    std::exception_ptr err;
    std::thread coord([&]{try{
      std::vector<std::string> args={"mucql_lock_e2e","coordinator","29810","3001",
        std::to_string(count),std::to_string(nworkers),"127.0.0.1:29820",
        "127.0.0.1:29821","127.0.0.1:29822","127.0.0.1:29823",
        "mock0",std::to_string(probe),std::to_string(work)};
      std::vector<char*> a;for(auto& s:args)a.push_back(s.data());
      if(coordinator(int(a.size()),a.data())!=0)throw std::runtime_error("coordinator failed");
    }catch(...){err=std::current_exception();}});
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    std::vector<std::thread> ws;
    std::vector<std::exception_ptr> ws_err(nworkers);
    for(size_t i=0;i<nworkers;i++)ws.emplace_back([&,i]{try{
      std::vector<std::string> args={"mucql_lock_e2e","worker","127.0.0.1:29810",
        std::to_string(i),"127.0.0.1",std::to_string(29811+i),"mock0"};
      std::vector<char*> a;for(auto& s:args)a.push_back(s.data());
      if(worker(int(a.size()),a.data())!=0)throw std::runtime_error("worker failed");
    }catch(...){ws_err[i]=std::current_exception();}});
    for(auto& t:ws)t.join();
    coord.join();
    if(err)std::rethrow_exception(err);
    for(auto&e:ws_err)if(e)std::rethrow_exception(e);
    std::cout<<"LOCK E2E mock multi-client verified PASS"<<std::endl;
    std::cout.flush();
    // Listener threads are process-scoped; terminate before destruction of
    // mock NIC registries (this is only a dedicated test executable).
    std::_Exit(0);
  }catch(const std::exception& e){std::cerr<<"E2E MOCK FAIL: "<<e.what()<<std::endl;
    std::_Exit(1);}
}
