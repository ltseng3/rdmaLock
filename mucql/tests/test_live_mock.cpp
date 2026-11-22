// In-process mock-RNIC integration test. Each test is a fresh process.
#define MUCQL_LIVE_LIBRARY
#include "../src/lock_live_main.cpp"
#include <csignal>
using namespace mucql_live;
int main(int argc,char**argv){
  std::signal(SIGALRM,[](int){std::_Exit(22);});alarm(35);
  try{
    // Real TCP loopback test for both sides of the admission transport.
    // The remaining integration test exercises the same code with mock RNICs.
    int listenfd=listen_tcp(0);
    sockaddr_in addr{};socklen_t len=sizeof(addr);
    if(getsockname(listenfd,reinterpret_cast<sockaddr*>(&addr),&len)!=0)
      fail("getsockname admission TCP");
    int clientfd=connect_tcp("127.0.0.1",ntohs(addr.sin_port));
    int serverfd=accept_tcp(listenfd);
    for(int fd:{clientfd,serverfd}){
      int val=0;socklen_t optlen=sizeof(val);
      if(getsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&val,&optlen)!=0||val!=1)
        fail("CN admission TCP_NODELAY missing");
    }
    close(serverfd);close(clientfd);close(listenfd);
    uint64_t workers=argc>1?std::stoull(argv[1]):3;
    uint64_t locks=argc>2?std::stoull(argv[2]):1;
    uint64_t join=argc>3?std::stoull(argv[3]):1;
    uint64_t phase=argc>4?std::stoull(argv[4]):1;
    uint64_t prep=argc>5?std::stoull(argv[5]):1;
    uint64_t ahead=argc>6?std::stoull(argv[6]):2;
    uint64_t zipf=argc>7?std::stoull(argv[7]):0;
    uint64_t probe=argc>8?std::stoull(argv[8]):1;
    // Use non-ephemeral TCP ports. Ephemeral client source ports can otherwise
    // collide with mock bootstrap listeners in rapid/repeated CTest runs.
    // A PID-derived prefix also isolates concurrent test processes.
    const uint16_t base=uint16_t(12000+(getpid()%100)*200);
    // The mock test's operation budget must never be its performance limiter.
    // Fast CI hosts can execute >2,900 simulated operations in 170 ms.
    constexpr uint64_t kTestSlots=131072, kTestMaxOps=90000;
    if(setenv("MUCQL_PAXOS_SLOTS", "131072", 1)!=0)fail("setenv mock slots");
    auto* region=new VerbsReleaseServer(release_opts("mock0",kTestSlots+2*locks+32));
    std::thread([region,base]{try{region->listen_forever(uint16_t(base+20));}
      catch(const std::exception& e){std::cerr<<"mock data server: "<<e.what()<<std::endl;std::_Exit(24);}}).detach();
    for(int r=0;r<3;r++)std::thread([r,base]{try{RdmaPaxosOptions o;o.device="mock0";o.slots=kTestSlots;
      serve_rdma_paxos(uint16_t(base+21+r),o);
    }catch(const std::exception& e){std::cerr<<"mock ordering server: "<<e.what()<<std::endl;std::_Exit(24);}}).detach();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::exception_ptr coord_error;
    std::thread coordinator_thread([&]{try{
      std::vector<std::string> a={"mucql_lock_live","coordinator",std::to_string(base+10),"90001",std::to_string(kTestMaxOps),std::to_string(workers),
       "127.0.0.1:"+std::to_string(base+20),"127.0.0.1:"+std::to_string(base+21),
       "127.0.0.1:"+std::to_string(base+22),"127.0.0.1:"+std::to_string(base+23),
       "mock0",std::to_string(join),std::to_string(phase),std::to_string(prep),std::to_string(ahead),
       std::to_string(std::max<uint64_t>(1,ahead/2)),"20",std::to_string(probe),"0","50","120",std::to_string(locks),std::to_string(zipf),"2026"};
      std::vector<char*> v;for(auto& s:a)v.push_back(s.data());
      if(coordinator(int(v.size()),v.data())!=0)fail("coordinator returned nonzero");
    }catch(...){coord_error=std::current_exception();}});
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    std::vector<std::thread> wt;wt.reserve(workers);
    std::vector<std::exception_ptr> errs(workers);
    for(uint64_t i=0;i<workers;i++)wt.emplace_back([&,i]{try{
      std::vector<std::string> a={"mucql_lock_live","worker","127.0.0.1:"+std::to_string(base+10),
         std::to_string(i),"127.0.0.1",std::to_string(base+31+i),"mock0"};
      std::vector<char*> v;for(auto& s:a)v.push_back(s.data());
      if(worker(int(v.size()),v.data())!=0)fail("worker returned nonzero");
    }catch(...){errs[i]=std::current_exception();}});
    for(auto& w:wt)w.join();
    coordinator_thread.join();
    if(coord_error)std::rethrow_exception(coord_error);
    for(auto& e:errs)if(e)std::rethrow_exception(e);
    std::cout<<"LIVE MOCK PASS workers="<<workers<<" locks="<<locks<<std::endl;
    std::cout.flush();std::_Exit(0);
  }catch(const std::exception& e){std::cerr<<"LIVE MOCK FAIL: "<<e.what()<<std::endl;std::_Exit(1);}
}