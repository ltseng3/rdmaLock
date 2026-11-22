// Batch-prepared exclusive-lock handoff benchmark. See tools/apt/E2E.md.
// No clock-based recovery: once the queue is committed, position 0 alone
// receives START; every later position requires an atomic direct RDMA CAS
// from its committed predecessor, following irreversible local retirement.
// This avoids assuming cross-machine clock bounds that Apt does not supply.
#include "mucql/core.hpp"
#include "mucql/paxos.hpp"
#include "mucql/verbs_paxos.hpp"
#include "mucql/verbs_release.hpp"
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <limits>

namespace {
using namespace mucql;
using Steady=std::chrono::steady_clock;
constexpr uint64_t kMagic=0x4d5543514c4c4b33ULL; // MUCQLLK3
constexpr uint64_t kLock=7;
constexpr uint64_t kOwnerExtra=2;
constexpr uint64_t kDataExtra=3;
constexpr size_t kMaxWorkers=8;
constexpr size_t kMaxPositions=3000; // 4096 RDMA Paxos slots incl PREPARE
struct NoExpiryClock : Clock {
  Bounds now() const override { return {0,0}; }
};
struct Hello {uint64_t magic,index,port;char ip[64];};
struct PeerWire {uint64_t port;char ip[64];};
struct ConfigWire {uint64_t magic,count,workers,self,release_port,verify,work_ns;
                   char release_host[64];};
struct TicketWire {uint64_t lock,client,request,position,grant_id,predecessor_id,deadline;};
struct Done {uint64_t magic,executed,violations;};
static_assert(sizeof(TicketWire)==56, "wire layout requires x86_64");
[[noreturn]] void fail(std::string s){throw std::runtime_error("lock e2e: "+s);}
void send_exact(int f,const void* p,size_t n){auto* b=(const char*)p;
  while(n){auto k=::send(f,b,n,MSG_NOSIGNAL);if(k<0&&errno==EINTR)continue;
    if(k<=0)fail("send");
    b+=k;
    n-=size_t(k);
  }
}
void recv_exact(int f,void* p,size_t n){auto* b=(char*)p;
  while(n){auto k=::recv(f,b,n,0);if(k<0&&errno==EINTR)continue;
    if(k<=0)fail("recv/peer disconnected");
    b+=k;
    n-=size_t(k);
  }
}
void set_timeout(int fd){timeval t{};t.tv_sec=40;
  setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&t,sizeof(t));
  setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&t,sizeof(t));}
int listen_tcp(uint16_t port){int s=socket(AF_INET,SOCK_STREAM,0);if(s<0)fail("socket");
  int opt=1;setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
  sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(port);a.sin_addr.s_addr=INADDR_ANY;
  if(bind(s,(sockaddr*)&a,sizeof(a))||listen(s,16))fail("bind/listen "+std::to_string(port));
  return s;}
int connect_tcp(const std::string& host,uint16_t port){addrinfo h{};h.ai_socktype=SOCK_STREAM;
  addrinfo* res=nullptr;auto p=std::to_string(port);
  if(getaddrinfo(host.c_str(),p.c_str(),&h,&res))fail("DNS "+host);
  int s=-1;for(auto*a=res;a;a=a->ai_next){int t=socket(a->ai_family,a->ai_socktype,a->ai_protocol);
    if(t<0)continue;
    if(::connect(t,a->ai_addr,a->ai_addrlen)==0){s=t;break;}
    close(t);}freeaddrinfo(res);if(s<0)fail("connect "+host+":"+p);
  set_timeout(s);return s;}
std::string host_of(const char* p){return std::string(p,strnlen(p,64));}
void host_to(char* d,const std::string& h){if(h.empty()||h.size()>=64)fail("bad host/ip");
  std::memset(d,0,64);std::memcpy(d,h.c_str(),h.size());}
uint16_t port_of(const std::string& s){auto v=std::stoul(s);if(v<1024||v>65535)fail("invalid port");return uint16_t(v);}
std::pair<std::string,uint16_t> endpoint(const std::string& s){
  auto p=s.rfind(':');if(p==std::string::npos)fail("expected IP:PORT, got "+s);
  return {s.substr(0,p),port_of(s.substr(p+1))};}
TicketWire pack(const Ticket& t){return {t.lock,t.client,t.request,t.position,
  t.grant_id,t.predecessor_id,t.deadline};}
Ticket unpack(const TicketWire& t){return {t.lock,t.client,t.request,t.position,
  t.grant_id,t.predecessor_id,t.deadline};}
ReleaseEndpointOptions opts(const std::string& dev,size_t slots){
  ReleaseEndpointOptions o;o.device=dev;o.slots=uint32_t(slots);
  if(const char* p=getenv("MUCQL_IB_PORT"))o.port=uint8_t(std::stoul(p));
  return o;
}
RdmaPaxosOptions paxos_opts(const std::string& dev){
  RdmaPaxosOptions o;o.device=dev;
  if(const char* p=getenv("MUCQL_IB_PORT"))o.port=uint8_t(std::stoul(p));
  return o;
}
void busy_for(uint64_t ns){const auto end=Steady::now()+std::chrono::nanoseconds(ns);
  while(Steady::now()<end){} }

int coordinator(int argc,char**argv){
  // coordinator <port> <ballot> <positions> <workers> <releaseIP:port>
  //             <r1IP:port> <r2IP:port> <r3IP:port> [device] [probe=0|1] [work_ns]
  if(argc<10)fail("coordinator <port> <ballot> <positions> <workers> <releaseIP:port> <r1:port> <r2:port> <r3:port> [device] [probe=0|1] [work_ns]");
  auto port=port_of(argv[2]);uint64_t ballot=std::stoull(argv[3]);
  auto count=std::stoull(argv[4]),workers=std::stoull(argv[5]);
  if(!ballot||count<2||count>kMaxPositions||workers<2||workers>kMaxWorkers||workers>count)
    fail("positions must be 2..3000, workers must be 2..8 and <=positions");
  auto [release_host,release_port]=endpoint(argv[6]);
  const std::string device=argc>10?argv[10]:"mlx4_0";
  const bool verify=argc>11?std::stoull(argv[11])!=0:true;
  const uint64_t work_ns=argc>12?std::stoull(argv[12]):0;
  if(work_ns>1000000000ULL)fail("work_ns too large");
  int server=listen_tcp(port);std::cout<<"lock coordinator listening tcp="<<port
    <<" positions="<<count<<" workers="<<workers<<" probe="<<verify<<std::endl;
  std::vector<int> conns(workers,-1);
  std::vector<PeerWire> peers(workers);
  for(size_t n=0;n<workers;n++){
    int c=accept(server,nullptr,nullptr);if(c<0)fail("accept worker");set_timeout(c);
    Hello h{};recv_exact(c,&h,sizeof(h));
    if(h.magic!=kMagic||h.index>=workers||conns[h.index]>=0||h.port>65535||h.port<1024||host_of(h.ip).empty())
      fail("invalid or duplicate worker HELLO");
    conns[h.index]=c;peers[h.index].port=h.port;
    host_to(peers[h.index].ip,host_of(h.ip));
    std::cout<<"registered worker="<<h.index<<" peer="<<h.ip<<":"<<h.port<<std::endl;
  }
  // Prepare entire finite queue BEFORE starting the handoff timer. This
  // measures chain throughput separately from the ordering/preparation cost.
  auto start_order=Steady::now();
  auto po=paxos_opts(device);
  std::vector<std::shared_ptr<Acceptor>> replicas;
  for(int i=7;i<10;i++){
    auto [h,p]=endpoint(argv[i]);
    replicas.push_back(std::make_shared<RdmaAcceptor>(h,p,po));
  }
  LocalQuorum q(replicas);PaxosJournal journal(q,ballot);journal.take_over();
  NoExpiryClock clock;
  Config cfg;cfg.horizon_ns=std::numeric_limits<uint64_t>::max()/2;
  cfg.prepare_batch=count;cfg.prepared_ahead=count;
  Leader leader(journal,clock,cfg);
  for(uint64_t i=0;i<count;i++){
    auto t=leader.join(kLock,1+i%workers,1+i);
    if(t.position!=i)fail("unexpected ticket position");
  }
  auto prepared=leader.prepare(kLock,count);
  if(prepared.size()!=count)fail("failed to commit full prepare batch");
  auto order_seconds=std::chrono::duration<double>(Steady::now()-start_order).count();
  // Send the exact same committed schedule and peer endpoints to all clients.
  for(size_t w=0;w<workers;w++){
    ConfigWire c{kMagic,count,workers,w,release_port,uint64_t(verify),work_ns,{}};
    host_to(c.release_host,release_host);
    send_exact(conns[w],&c,sizeof(c));
    send_exact(conns[w],peers.data(),peers.size()*sizeof(PeerWire));
    for(const auto& t:prepared){auto wt=pack(t);send_exact(conns[w],&wt,sizeof(wt));}
  }
  for(auto c:conns){uint8_t ready=0;recv_exact(c,&ready,1);
    if(ready!='R')fail("worker did not become RDMA-ready");}
  auto started=Steady::now();
  for(auto c:conns){uint8_t go='G';send_exact(c,&go,1);}
  uint64_t completed=0,violations=0;
  for(auto c:conns){Done d{};recv_exact(c,&d,sizeof(d));
    if(d.magic!=kMagic)fail("invalid worker completion");
    completed+=d.executed;violations+=d.violations;}
  auto handoff_seconds=std::chrono::duration<double>(Steady::now()-started).count();
  for(int c:conns)close(c);
  close(server);
  // Independent hardware verification AFTER stopwatch; markers are write-once
  // CAS and the protected data slot contains the final ticket position.
  VerbsReleaseClient rdma(release_host,release_port,opts(device,count+16));
  uint64_t checked=0;
  for(size_t i=0;i<count;i++){
    if(rdma.read_slot(i)!=prepared[i].grant_id)fail("missing/corrupt release marker at "+std::to_string(i));
    checked++;
  }
  if(rdma.read_slot(count+kDataExtra)!=count)fail("wrong protected RDMA data (final position)");
  if(verify&&rdma.read_slot(count+kOwnerExtra)!=0)fail("owner slot not clear");
  if(completed!=count||violations)fail("missing operations or ownership violations");
  std::cout<<"LOCK_E2E positions="<<count<<" workers="<<workers
    <<" completed="<<completed<<" violations="<<violations
    <<" verified_markers="<<checked<<" ordered_entries="<<journal.snapshot().size()
    <<" probe="<<verify<<" work_ns="<<work_ns
    <<" ordering_seconds="<<order_seconds
    <<" handoff_seconds="<<handoff_seconds
    <<" goodput_ops_per_sec="<<completed/handoff_seconds
    <<" including_ordering_ops_per_sec="<<completed/(order_seconds+handoff_seconds)
    <<" status=PASS"<<std::endl;
  return 0;
}
int worker(int argc,char**argv){
  // worker <coordinator-ip:port> <index> <self-ib-ip> <listen-port> [device]
  if(argc<6)fail("worker <coordinatorIP:port> <index> <selfIP> <listen-port> [device]");
  auto [host,port]=endpoint(argv[2]);uint64_t index=std::stoull(argv[3]);
  const std::string self_ip=argv[4];const auto listenport=port_of(argv[5]);
  const std::string device=argc>6?argv[6]:"mlx4_0";
  if(index>=kMaxWorkers)fail("worker index outside 0..7");
  // Process-lifetime listener: the RNIC services handoffs with no worker CPU
  // callback. The listener intentionally survives until process exit.
  auto* inbound=new VerbsReleaseServer(opts(device,4096));
  std::thread([inbound,listenport]{inbound->listen_forever(listenport);}).detach();
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  int c=connect_tcp(host,port);
  Hello h{kMagic,index,listenport,{}};host_to(h.ip,self_ip);send_exact(c,&h,sizeof(h));
  ConfigWire config{};recv_exact(c,&config,sizeof(config));
  if(config.magic!=kMagic||config.self!=index||config.workers<2||
     config.workers>kMaxWorkers||config.count>kMaxPositions||config.count<2)
    fail("invalid coordinator config");
  std::vector<PeerWire> peers(config.workers);
  recv_exact(c,peers.data(),peers.size()*sizeof(PeerWire));
  std::vector<Ticket> tickets; tickets.reserve(config.count);
  Client client(index+1);NoExpiryClock clock;
  for(uint64_t i=0;i<config.count;i++){
    TicketWire wire{};recv_exact(c,&wire,sizeof(wire));Ticket t=unpack(wire);
    if(t.position!=i||t.lock!=kLock||t.client!=1+i%config.workers||!t.grant_id||!t.deadline||
       (i>0&&t.predecessor_id!=tickets.back().grant_id))fail("invalid committed ticket schedule");
    tickets.push_back(t);
    if(t.client==index+1)client.on_prepared(t);
  }
  // Next owner is always on a different process (workers >=2).
  auto& nextpeer=peers[(index+1)%config.workers];
  auto r_opts=opts(device,4096);
  VerbsReleaseClient outgoing(host_of(nextpeer.ip),uint16_t(nextpeer.port),r_opts);
  VerbsReleaseClient releases(host_of(config.release_host),uint16_t(config.release_port),r_opts);
  std::unique_ptr<VerbsReleaseClient> data;
  if(config.verify)data=std::make_unique<VerbsReleaseClient>(
      host_of(config.release_host),uint16_t(config.release_port),r_opts);
  if(releases.slots()<config.count+16||outgoing.slots()<config.count+16)
    fail("server lacks slots; restart with count+16 or more");
  uint8_t ready='R';send_exact(c,&ready,1);
  uint8_t go=0;recv_exact(c,&go,1);if(go!='G')fail("invalid GO");
  uint64_t completed=0;
  for(uint64_t i=index;i<config.count;i+=config.workers){
    const Ticket& t=tickets[i];
    if(i==0){if(!client.activate_start(t,true,clock))fail("head START rejected");}
    else{
      auto wait_start=Steady::now();
      for(;;){auto v=inbound->slot_value(i);
        if(v==t.grant_id)break;
        if(v!=0)fail("foreign/stale handoff at "+std::to_string(i));
        if(Steady::now()-wait_start>std::chrono::seconds(30))fail("handoff timed out at "+std::to_string(i));
        std::this_thread::yield();
      }
      if(!client.activate_handoff(Handoff{t,t.predecessor_id,i-1},clock))
        fail("handoff rejected at "+std::to_string(i));
    }
    if(!client.may_use(t,clock))fail("no lock ownership at "+std::to_string(i));
    // Safety probe must FAIL on overlapping owners, not wait or retry.
    if(data){
      const auto old=data->compare_exchange(config.count+kOwnerExtra,0,t.grant_id);
      if(old!=0)fail("MUTUAL EXCLUSION VIOLATION at "+std::to_string(i)+" owner="+std::to_string(old));
    }
    // Actual one-sided protected memory access. Completion BEFORE retire.
    releases.write_slot(config.count+kDataExtra,i+1);
    if(config.work_ns)busy_for(config.work_ns);
    if(data){
      const auto old=data->compare_exchange(config.count+kOwnerExtra,t.grant_id,0);
      if(old!=t.grant_id)fail("critical-section owner changed unexpectedly");
    }
    // Retirement is irrevocable. Subsequent steps may run in parallel on
    // different QPs; no dependency on the ordering leader's CPU.
    std::optional<Ticket> next;
    if(i+1<config.count)next=tickets[i+1];
    auto action=client.retire(t,next);
    releases.post(i,t.grant_id,ReleaseMode::AtomicCas);
    if(action.handoff){
      auto old=outgoing.compare_exchange(i+1,0,action.handoff->successor.grant_id);
      if(old!=0&&old!=action.handoff->successor.grant_id)
        fail("handoff CAS conflict at "+std::to_string(i+1));
    }
    if(!releases.complete())fail("release marker CAS mismatch");
    completed++;
  }
  Done d{kMagic,completed,0};send_exact(c,&d,sizeof(d));close(c);
  std::cout<<"LOCK_WORKER id="<<index<<" executed="<<completed<<" status=PASS"<<std::endl;
  return 0;
}
}
#ifndef MUCQL_E2E_LIBRARY
int main(int argc,char**argv){
  try{
    if(argc<2)fail("Usage: mucql_lock_e2e coordinator|worker ... ; see tools/apt/E2E.md");
    std::string mode=argv[1];
    if(mode=="coordinator")return coordinator(argc,argv);
    if(mode=="worker")return worker(argc,argv);
    fail("unknown mode "+mode);
  }catch(const std::exception&e){std::cerr<<"ERROR: "<<e.what()<<'\n';return 1;}
}

#endif // MUCQL_E2E_LIBRARY
