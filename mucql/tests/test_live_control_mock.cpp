#include "mucql/live_control.hpp"
#include <atomic>
#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
using mucql::LiveControlChannel;
struct Frame{uint64_t id; char data[88];};
int main(){
  int fds[2];if(socketpair(AF_UNIX,SOCK_STREAM,0,fds))throw std::runtime_error("socketpair");
  std::unique_ptr<LiveControlChannel> right;
  std::exception_ptr startup_error;
  std::thread init([&]{try{right=std::make_unique<LiveControlChannel>(fds[1],"mock0",1,true,sizeof(Frame));}
    catch(...){startup_error=std::current_exception();}});
  std::unique_ptr<LiveControlChannel> left;
  try{left=std::make_unique<LiveControlChannel>(fds[0],"mock0",1,true,sizeof(Frame));}
  catch(...){shutdown(fds[1],SHUT_RDWR);init.join();throw;}
  init.join();if(startup_error)std::rethrow_exception(startup_error);
  for(uint64_t i=1;i<=512;i++){
    Frame a{};a.id=i;std::memset(a.data,int(i%247),sizeof(a.data));
    Frame b{};left->send(&a,sizeof(a));right->recv(&b,sizeof(b));
    if(std::memcmp(&a,&b,sizeof(a)))throw std::runtime_error("client -> leader corruption");
    a.id+=100000;right->send(&a,sizeof(a));left->recv(&b,sizeof(b));
    if(std::memcmp(&a,&b,sizeof(a)))throw std::runtime_error("leader -> client corruption");
  }
  Frame dummy{};bool rejected=false;
  try{left->send(&dummy,sizeof(dummy)-1);}catch(const std::runtime_error&){rejected=true;}
  if(!rejected)throw std::runtime_error("invalid frame length accepted");
  std::atomic<bool> cancelled{false};
  std::thread rx([&]{try{right->recv(&dummy,sizeof(dummy));}
    catch(const std::runtime_error&){cancelled=true;}});
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  right->stop();rx.join();
  if(!cancelled)throw std::runtime_error("recv cancellation failed");
  right.reset();left.reset();close(fds[0]);close(fds[1]);
  std::cout<<"Control channel: 1024 mock RDMA frames, buffers recycled, cancel PASS\n";
}
