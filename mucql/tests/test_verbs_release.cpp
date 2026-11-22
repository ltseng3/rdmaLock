#include "mucql/verbs_release.hpp"
#include <thread>
#include <chrono>
#include <stdexcept>
static void require(bool x){if(!x)throw std::runtime_error("mock RDMA release test failed");}
#include <iostream>
int main(){
  mucql::ReleaseEndpointOptions opts;opts.slots=100;
  mucql::VerbsReleaseServer server(opts);
  std::thread([&]{server.listen_forever(29652);}).detach();
  std::this_thread::sleep_for(std::chrono::milliseconds(90));
  mucql::VerbsReleaseClient c("127.0.0.1",29652,opts);
  c.post(1,117,mucql::ReleaseMode::AtomicCas);
  require(c.complete());
  require(server.slot_value(1)==117);
  c.post(1,117,mucql::ReleaseMode::AtomicCas);require(c.complete());
  c.post(1,118,mucql::ReleaseMode::AtomicCas);require(!c.complete());
  require(server.slot_value(1)==117);
  c.post(2,200,mucql::ReleaseMode::WriteHint);require(c.complete());
  require(server.slot_value(2)==200);
  require(c.read_slot(1)==117);
  require(c.compare_exchange(3,0,501)==0);
  require(server.slot_value(3)==501);
  require(c.compare_exchange(3,0,502)==501);
  require(server.slot_value(3)==501);
  require(c.compare_exchange(3,501,0)==501);
  require(server.slot_value(3)==0);
  c.write_slot(4,777);
  require(c.read_slot(4)==777);
  std::cout<<"RDMA release mock integration PASS\n";
}
