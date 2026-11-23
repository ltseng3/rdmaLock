// Bounded, LIVE CN-admitted locking. Not a lease/failover experiment.
// One outstanding JOIN per worker. Every ticket is a committed JOIN + PREPARE;
// predecessors receive the prepared successor and issue one-sided RDMA handoff.
#include "mucql/core.hpp"
#include "mucql/paxos.hpp"
#include "mucql/verbs_paxos.hpp"
#include "mucql/verbs_release.hpp"
#include "mucql/live_control.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <cmath>
#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <optional>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace mucql_live {
using namespace mucql;
using ClockType=std::chrono::steady_clock;
using ns=std::chrono::nanoseconds;
static constexpr uint64_t kMagic=0x4d5543514c4c4956ULL; // MUCQLLIV
static constexpr uint64_t kLockBase=1;
static constexpr uint64_t kMaxWorkers=64;
static constexpr uint32_t kMaxSlots=1000000;
static constexpr uint64_t kMaxLocks=100000; // Independent lock population; not log history.
static constexpr uint64_t kPrepareLogReserve=64; // JOIN must leave room for PREPARE.
bool join_log_has_room(uint64_t decided,uint64_t incoming,uint64_t slots){
  // Decided-record count and lock population are independent. Every JOIN
  // consumes a log index, but a key with no JOIN consumes none. Keep a
  // fixed conservative PREPARE reserve, and fail explicitly at exhaustion.
  return decided<slots && incoming<=slots-decided &&
         slots-decided-incoming>kPrepareLogReserve;
}
enum MsgType:uint64_t {Join=1,Done=2,Quiescent=3,TicketReady=4,HandoffReady=5,Go=6,Stop=7,Finish=8};
struct TicketWire {uint64_t lock,client,request,position,grant,predecessor,deadline;};
struct Message {uint64_t magic,type,flags,latency_ns,admit_ns,wait_ns;TicketWire ticket;};
struct Hello {uint64_t magic,index,port;char ip[64];};
struct Peer {uint64_t port;char ip[64];};
struct Options {uint64_t ballot{},max_ops{},workers{},lock_count{1},join_aggregate{1},phase2_window{1},
  prepare_range{1},lookahead{2},replenish_threshold{1},batch_wait_us{0},probe{0},work_ns{0},warmup_ms{200},measure_ms{500},
  seed{2026},zipf{0},paxos_slots{4096},data_ops{1};};
struct Config {uint64_t magic,workers,self,lock_count,max_ops,paxos_slots,probe,work_ns,
  warmup_ms,measure_ms,seed,zipf,transport,data_ops;uint64_t data_port;char data_host[64];};
struct EndlessClock:Clock {Bounds now() const override{return {0,0};}};
struct Event {size_t worker;Message m;};
bool rdma_admission(){
  const char* v=std::getenv("MUCQL_CN_TRANSPORT");
  if(!v||std::strcmp(v,"rdma")==0)return true;
  if(std::strcmp(v,"tcp")==0)return false;
  throw std::runtime_error("MUCQL_CN_TRANSPORT must be rdma or tcp");
}
uint8_t control_port(){
  const char* p=std::getenv("MUCQL_IB_PORT");
  if(!p)return 1;
  auto n=std::stoul(p);
  if(n<1||n>2)throw std::runtime_error("MUCQL_IB_PORT must be 1 or 2");
  return uint8_t(n);
}
[[noreturn]] void fail(const std::string& s){throw std::runtime_error("mucql_live: "+s);}
uint64_t number(const char* s,uint64_t lo,uint64_t hi,const std::string& name){
  try{size_t end=0;std::string a=s;auto n=std::stoull(a,&end);if(end!=a.size()||n<lo||n>hi)fail("bad "+name);return n;}
  catch(const std::exception&){fail("bad "+name);}
}
uint64_t env_number(const char* name,uint64_t default_value,uint64_t high){
  const char* s=std::getenv(name);
  return s?number(s,0,high,name):default_value;
}
void host_to(char* dst,const std::string& host){if(host.empty()||host.size()>63)fail("host too long");std::memset(dst,0,64);std::memcpy(dst,host.c_str(),host.size());}
std::string host_of(const char* p){return std::string(p,strnlen(p,64));}
std::pair<std::string,uint16_t> endpoint(const std::string& s){auto i=s.rfind(':');
  if(i==std::string::npos)fail("expected host:port");
  return {s.substr(0,i),uint16_t(number(s.c_str()+i+1,1024,65535,"port"))};}
TicketWire wire(const Ticket& t){return {t.lock,t.client,t.request,t.position,t.grant_id,t.predecessor_id,t.deadline};}
Ticket unpack(const TicketWire& w){return {w.lock,w.client,w.request,w.position,w.grant,w.predecessor,w.deadline};}
Message message(MsgType type,const Ticket& t={}){Message m{};m.magic=kMagic;m.type=type;m.ticket=wire(t);return m;}
void send_exact(int fd,const void* p,size_t n){auto b=static_cast<const char*>(p);
  while(n){auto k=::send(fd,b,n,MSG_NOSIGNAL);if(k<0&&errno==EINTR)continue;
    if(k<=0)fail("send disconnected");
    n-=size_t(k);b+=k;}}
void recv_exact(int fd,void* p,size_t n){auto b=static_cast<char*>(p);
  while(n){auto k=::recv(fd,b,n,0);if(k<0&&errno==EINTR)continue;
    if(k<=0)fail("recv disconnected");
    n-=size_t(k);b+=k;}}
// Avoid Nagle/delayed-ACK stalls on small CN admission/control frames.
// This identical transport configuration applies to raw and full MuCQL.
void tcp_no_delay(int fd){
  const int one=1;
  if(setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one))!=0)
    fail(std::string("TCP_NODELAY: ")+std::strerror(errno));
}
int accept_tcp(int listener){
  int fd;
  do{fd=::accept(listener,nullptr,nullptr);}while(fd<0&&errno==EINTR);
  if(fd<0)fail(std::string("accept CN: ")+std::strerror(errno));
  tcp_no_delay(fd);
  return fd;
}
int listen_tcp(uint16_t port){int fd=socket(AF_INET,SOCK_STREAM,0);if(fd<0)fail("socket");
  int opt=1;setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
  sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(port);a.sin_addr.s_addr=INADDR_ANY;
  if(bind(fd,(sockaddr*)&a,sizeof(a))||listen(fd,128))fail("bind/listen "+std::to_string(port));
  return fd;}
int connect_tcp(const std::string& host,uint16_t port){addrinfo hints{};hints.ai_family=AF_INET;hints.ai_socktype=SOCK_STREAM;
  addrinfo* ai=nullptr;auto p=std::to_string(port);
  if(getaddrinfo(host.c_str(),p.c_str(),&hints,&ai))fail("resolve "+host);
  int fd=-1;for(auto* a=ai;a;a=a->ai_next){int f=socket(a->ai_family,a->ai_socktype,a->ai_protocol);
    if(f<0)continue;
    if(::connect(f,a->ai_addr,a->ai_addrlen)==0){tcp_no_delay(f);fd=f;break;}
    close(f);}
  freeaddrinfo(ai);if(fd<0)fail("TCP connect "+host+":"+p);return fd;}
ReleaseEndpointOptions release_opts(const std::string& dev,uint64_t slots){ReleaseEndpointOptions o;
  o.device=dev;o.slots=uint32_t(slots);
  if(const char* p=getenv("MUCQL_IB_PORT"))o.port=uint8_t(number(p,1,2,"IB port"));
  return o;}
RdmaPaxosOptions paxos_opts(const std::string& dev,uint64_t slots){RdmaPaxosOptions o;
  o.device=dev;o.slots=uint32_t(slots);
  if(const char* p=getenv("MUCQL_IB_PORT"))o.port=uint8_t(number(p,1,2,"IB port"));
  return o;}
void busy_for(uint64_t n){auto end=ClockType::now()+ns(n);while(ClockType::now()<end){}}
// Every log index is a unique write-once marker/handoff slot; data and owner
// addresses are disjoint, fixed-width per lock and require no slot reuse.
uint64_t release_slots(const Options& o){return o.paxos_slots+2*o.lock_count+32;}
uint64_t data_slot(const Options& o,LockId lock){return o.paxos_slots+lock-kLockBase;}
uint64_t owner_slot(const Options& o,LockId lock){return o.paxos_slots+o.lock_count+lock-kLockBase;}
void validate(const Options& o){
  if(o.workers<2||o.workers>kMaxWorkers||o.lock_count<1||o.lock_count>kMaxLocks||
     o.join_aggregate<1||o.join_aggregate>32||o.phase2_window<1||o.phase2_window>32||
     o.prepare_range<1||o.prepare_range>128||o.lookahead<2||o.lookahead>4096||
     o.prepare_range>o.lookahead||o.replenish_threshold<1||
     o.replenish_threshold>o.lookahead||o.batch_wait_us>5000||o.probe>1||o.work_ns>1000000000||
     o.warmup_ms>60000||o.measure_ms<50||o.measure_ms>120000||o.zipf>1||
     o.data_ops<1||o.data_ops>32||
     o.paxos_slots<4096||o.paxos_slots>kMaxSlots||o.max_ops<o.workers||
     o.max_ops>o.paxos_slots-o.paxos_slots/64||release_slots(o)>2000000)
      fail("invalid LIVE options or insufficient write-once log capacity");
}
uint64_t percentile(std::vector<uint64_t> v,double p){if(v.empty())return 0;
  std::sort(v.begin(),v.end());return v[size_t((v.size()-1)*p)];}

int coordinator(int argc,char**argv){
  // coordinator port ballot max_ops workers data:port r1:port r2:port r3:port
  //   device join_aggregation phase2_window prepare_range lookahead
  //   replenish_threshold batch_wait_us probe work_ns warmup_ms measure_ms lock_count zipf seed
  if(argc!=24)fail("coordinator <port> <ballot> <max_ops> <workers> <data:port> <r1> <r2> <r3> <device> <join_aggregate> <phase2_window> <prepare_range> <lookahead> <replenish_threshold> <batch_wait_us> <probe> <work_ns> <warmup_ms> <measure_ms> <locks> <zipf=0|1> <seed>");
  const auto port=uint16_t(number(argv[2],1024,65535,"port"));Options o;
  o.ballot=number(argv[3],1,UINT64_MAX,"ballot");
  o.max_ops=number(argv[4],2,kMaxSlots,"max_ops");
  o.workers=number(argv[5],2,kMaxWorkers,"workers");
  const auto [data_host,data_port]=endpoint(argv[6]);
  const std::string device=argv[10];
  o.join_aggregate=number(argv[11],1,32,"join_aggregate");
  o.phase2_window=number(argv[12],1,32,"phase2_window");
  o.prepare_range=number(argv[13],1,128,"prepare_range");
  o.lookahead=number(argv[14],2,4096,"lookahead");
  o.replenish_threshold=number(argv[15],1,4096,"replenish_threshold");
  o.batch_wait_us=number(argv[16],0,5000,"batch_wait_us");
  o.probe=number(argv[17],0,1,"probe");
  o.work_ns=number(argv[18],0,1000000000,"work_ns");
  o.warmup_ms=number(argv[19],0,60000,"warmup_ms");
  o.measure_ms=number(argv[20],50,120000,"measure_ms");
  o.lock_count=number(argv[21],1,kMaxLocks,"lock_count");
  o.zipf=number(argv[22],0,1,"zipf");
  o.seed=number(argv[23],1,UINT64_MAX,"seed");
  // MUCQL_PAXOS_SLOTS is shared with acceptors; this prevents CLI sprawl.
  if(const char* s=getenv("MUCQL_PAXOS_SLOTS"))o.paxos_slots=number(s,4096,kMaxSlots,"paxos_slots");
  // Matches DecLock MICROBENCH_SHOT_NUM: one signaled, completed 8-byte
  // RDMA WRITE per shot, within the held exclusive-lock critical section.
  if(const char* s=getenv("MUCQL_DATA_OPS"))o.data_ops=number(s,1,32,"data_ops");
  validate(o);
  const bool control_rdma=rdma_admission();
  const auto rx_idle_us=env_number("MUCQL_RX_IDLE_US",0,100);
  const auto handoff_idle_us=env_number("MUCQL_HANDOFF_IDLE_US",0,100);
  const auto handoff_preconnect=env_number("MUCQL_HANDOFF_PRECONNECT",1,1);
  int listenfd=listen_tcp(port);
  std::cout<<"LIVE coordinator listening tcp="<<port<<" workers="<<o.workers
           <<" locks="<<o.lock_count<<" max_ops="<<o.max_ops<<" transport="<<(control_rdma?"rdma":"tcp")<<std::endl;
  std::vector<int> sockets(o.workers,-1);std::vector<Peer> peers(o.workers);
  for(size_t n=0;n<o.workers;n++){
    int fd=accept_tcp(listenfd);
    Hello h{};recv_exact(fd,&h,sizeof(h));
    if(h.magic!=kMagic||h.index>=o.workers||sockets[h.index]!=-1||h.port<1024||h.port>65535||host_of(h.ip).empty())
      fail("duplicate/invalid worker HELLO");
    sockets[h.index]=fd;peers[h.index].port=h.port;host_to(peers[h.index].ip,host_of(h.ip));
  }
  std::vector<std::shared_ptr<Acceptor>> replicas;
  for(int i=7;i<10;i++){auto [h,p]=endpoint(argv[i]);
    replicas.push_back(std::make_shared<RdmaAcceptor>(h,p,paxos_opts(device,o.paxos_slots)));}
  LocalQuorum q(replicas);PaxosJournal journal(q,o.ballot);journal.take_over();
  journal.set_phase2_window(o.phase2_window);
  EndlessClock clock; mucql::Config lc;lc.horizon_ns=std::numeric_limits<uint64_t>::max()/2;
  lc.prepare_batch=o.prepare_range;lc.prepared_ahead=o.lookahead;
  Leader leader(journal,clock,lc);
  VerbsReleaseClient release(data_host,data_port,release_opts(device,release_slots(o)));
  for(size_t w=0;w<o.workers;w++){
    Config c{};c.magic=kMagic;c.workers=o.workers;c.self=w;c.lock_count=o.lock_count;
    c.max_ops=o.max_ops;c.paxos_slots=o.paxos_slots;c.probe=o.probe;c.work_ns=o.work_ns;
    c.warmup_ms=o.warmup_ms;c.measure_ms=o.measure_ms;c.seed=o.seed;c.zipf=o.zipf;
    c.transport=control_rdma?1:0;c.data_ops=o.data_ops;
    c.data_port=data_port;host_to(c.data_host,data_host);
    send_exact(sockets[w],&c,sizeof(c));send_exact(sockets[w],peers.data(),sizeof(Peer)*peers.size());
  }
  std::vector<std::unique_ptr<LiveControlChannel>> channels(o.workers);
  for(size_t w=0;w<o.workers;w++)
    channels[w]=std::make_unique<LiveControlChannel>(sockets[w],device,control_port(),control_rdma,sizeof(Message));
  for(auto fd:sockets){char ready=0;recv_exact(fd,&ready,1);if(ready!='R')fail("worker not RDMA-ready");}
  std::mutex mu;std::condition_variable cv;std::deque<Event> received;
  std::string recv_error;bool closed=false;
  std::vector<std::thread> readers;
  for(size_t w=0;w<o.workers;w++)readers.emplace_back([&,w]{
    try{for(;;){Message m{};channels[w]->recv(&m,sizeof(m));if(m.magic!=kMagic)fail("bad worker message");
      {std::lock_guard<std::mutex> lock(mu);received.push_back({w,m});}cv.notify_one();
      if(m.type==Quiescent)break;}}
    catch(const std::exception& e){std::lock_guard<std::mutex> lock(mu);
      if(!closed && recv_error.empty())recv_error="worker "+std::to_string(w)+": "+e.what();
      cv.notify_one();}
  });
  auto send_to=[&](size_t w,Message m){channels[w]->send(&m,sizeof(m));};
  auto start=ClockType::now();
  for(size_t w=0;w<o.workers;w++)send_to(w,message(Go));
  auto end=start+std::chrono::milliseconds(o.warmup_ms+o.measure_ms);
  uint64_t joined=0,done=0,measured=0,prepare_records=0;
  uint64_t actual_join_batches=0,join_batch_sum=0;
  uint64_t join_key_visits=0,prepare_key_visits=0;
  std::vector<uint64_t> latency,admission,waits;
  // Keep all historical tickets for end-of-run verification, but only visit
  // keys with newly committed JOINs or newly certified completions in the
  // critical-path scheduling loop. Scanning every key ever seen made a
  // 100,000-key uniform workload slower than a contended Zipf workload.
  std::map<LockId,std::deque<Event>> waiting;
  std::set<LockId> join_ready, prepare_ready;
  std::map<LockId,std::vector<Ticket>> known;
  std::map<LockId,size_t> progress_frontier;
  std::map<GrantId,Ticket> granted;std::set<GrantId> completed;
  std::vector<bool> quiesced(o.workers,false);size_t quiescent_count=0;
  bool stopped=false;
  try{
    for(;;){
      if(!stopped&&ClockType::now()>=end){
        stopped=true;for(size_t w=0;w<o.workers;w++)send_to(w,message(Stop));}
      std::deque<Event> events;
      {std::unique_lock<std::mutex> lock(mu);
        if(received.empty()&&recv_error.empty()&&!(stopped&&quiescent_count==o.workers))
          cv.wait_for(lock,std::chrono::milliseconds(1));
        if(!recv_error.empty())fail(recv_error);
        events.swap(received);
      }
      for(const auto& e:events){
        const auto& m=e.m;
        if(m.type==Join){
          if(quiesced[e.worker])fail("JOIN after quiescence");
          if(m.ticket.client!=e.worker+1||m.ticket.request==0||m.ticket.lock<kLockBase||
             m.ticket.lock>=kLockBase+o.lock_count)fail("invalid CN JOIN");
          waiting[m.ticket.lock].push_back(e);
          join_ready.insert(m.ticket.lock);
        }else if(m.type==Done){
          const auto it=granted.find(m.ticket.grant);
          if(it==granted.end()||it->second.client!=e.worker+1||it->second.lock!=m.ticket.lock||
             it->second.position!=m.ticket.position||!completed.insert(m.ticket.grant).second)
            fail("invalid or duplicate CN completion grant="+std::to_string(m.ticket.grant)+
              " client="+std::to_string(m.ticket.client)+" sender="+std::to_string(e.worker)+
              " got="+(it==granted.end()?"missing":std::to_string(it->second.client))+
              " position="+std::to_string(m.ticket.position)+" oldpos="+(it==granted.end()?"missing":std::to_string(it->second.position))+" oldlock="+(it==granted.end()?"missing":std::to_string(it->second.lock))+" completions="+std::to_string(completed.count(m.ticket.grant)));
          if(release.read_slot(m.ticket.grant)!=m.ticket.grant)fail("uncertified release marker");
          leader.observe({it->second.lock,it->second.position,it->second.grant_id});
          // A completed predecessor may free a PREPARE lookahead window.
          // The benchmark clock never advances, so release notification
          // (or a new JOIN) is the only reason to revisit this lock.
          prepare_ready.insert(it->second.lock);
          done++;
          if(m.flags&1){measured++;latency.push_back(m.latency_ns);admission.push_back(m.admit_ns);
            waits.push_back(m.wait_ns);}
        }else if(m.type==Quiescent){
          if(quiesced[e.worker])fail("duplicate quiescent");
          quiesced[e.worker]=true;quiescent_count++;
        }else fail("invalid CN message type");
      }
      // Aggregate *actual* concurrent CN admissions. No synthetic ticket list.
      // Delay is explicitly tunable and charged to the timed request latency.
      if(!waiting.empty()&&o.batch_wait_us){
        auto total=0UL;for(auto& w:waiting)total+=w.second.size();
        if(total<o.join_aggregate && !stopped){
          std::this_thread::sleep_for(std::chrono::microseconds(o.batch_wait_us));
          std::deque<Event> later;
          {
            std::lock_guard<std::mutex> lock(mu);
            later.swap(received);
          }
          for(const auto& e:later){
            if(e.m.type==Join){
              waiting[e.m.ticket.lock].push_back(e);
              join_ready.insert(e.m.ticket.lock);
            }
            else{
              std::lock_guard<std::mutex> lock(mu);
              received.push_back(e);
            }
          }
        }
      }
      // Iterate just the keys that actually received JOINs. Process in
      // ascending lock order as before; erase drained queues immediately.
      for(LockId lock:join_ready){
        auto pending=waiting.find(lock);
        if(pending==waiting.end()||pending->second.empty())
          fail("JOIN worklist key has no pending requests");
        auto& queue=pending->second;
        ++join_key_visits;
        while(!queue.empty()){
          const size_t cap=std::min<size_t>(o.join_aggregate,queue.size());
          if(!join_log_has_room(journal.decided_count(),cap,o.paxos_slots) || joined+cap>o.max_ops)
            fail("nonrecycling journal/operation cap exhausted BEFORE timed interval completed");
          std::vector<std::pair<ClientId,RequestId>> reqs;
          for(size_t i=0;i<cap;i++){
            const auto e=queue.front();queue.pop_front();reqs.emplace_back(e.worker+1,e.m.ticket.request);
          }
          auto new_tickets=leader.join_batch(lock,reqs);
          if(new_tickets.size()!=cap)fail("JOIN batch mismatch");
          for(auto& t:new_tickets){known[lock].push_back(t);joined++;}
          actual_join_batches++;join_batch_sum+=cap;
        }
        waiting.erase(pending);
        prepare_ready.insert(lock);
      }
      join_ready.clear();
      // PREPARE must be committed BEFORE the owning CN or predecessor sees
      // the successor as activatable. The queue advances only on certified CAS.
      for(LockId lock:prepare_ready){
        auto candidate=known.find(lock);
        if(candidate==known.end()||candidate->second.empty())
          fail("PREPARE worklist key has no committed JOIN");
        auto& tickets=candidate->second;
        ++prepare_key_visits;
        // A low-watermark trigger, with 1 = reactive and higher = proactive.
        auto initial_prepared=leader.prepared_count(lock);
        auto& progress=progress_frontier[lock];
        while(progress<tickets.size()&&completed.count(tickets[progress].grant_id))progress++;
        if(initial_prepared>=tickets.size()||initial_prepared-progress>o.replenish_threshold)continue;
        // Once below the trigger, fill toward the configured prepared window.
        // This separates window depth from *when* replenishment begins.
        for(;;){
          if(leader.prepared_count(lock)>=tickets.size())break;
          if(journal.decided_count()+1>=o.paxos_slots)fail("journal full before PREPARE");
          auto batch=leader.prepare(lock,o.prepare_range);
          if(batch.empty())break;
          prepare_records++;
          for(const auto& t:batch){
            granted[t.grant_id]=t;
            send_to(t.client-1,message(TicketReady,t));
            if(t.position>0){
              const auto pred=leader.ticket(lock,t.position-1);
              if(!pred||!pred->deadline||pred->grant_id!=t.predecessor_id)
                fail("prepared successor without prepared predecessor");
              send_to(pred->client-1,message(HandoffReady,t));
            }
          }
        }
      }
      // A full lookahead window can only reopen on a certified release;
      // Done re-arms the lock. Never carry idle keys into another pass.
      prepare_ready.clear();
      if(stopped&&quiescent_count==o.workers&&done==joined)break;
    }
    // Out of stopwatch: independently verify every release, last protected
    // write per lock, and owner probe. No warmup request was precommitted.
    for(const auto& [grant,t]:granted){if(!completed.count(grant)||release.read_slot(grant)!=grant)
      fail("missing final release marker "+std::to_string(grant));}
    for(const auto& [lock,tickets]:known){
      const auto last=tickets.back();
      if(release.read_slot(data_slot(o,lock))!=last.grant_id)fail("protected data final value mismatch");
      if(o.probe&&release.read_slot(owner_slot(o,lock))!=0)fail("owner slot not cleared");
    }
    if(measured==0)fail("no measured completions");
    std::cout<<"LIVE_E2E status=PASS scope=live_steady_state workers="<<o.workers
      <<" locks="<<o.lock_count<<" zipf="<<o.zipf
      <<" read_ratio=0 data_size_bytes=8 data_ops="<<o.data_ops
      <<" data_writes_measured="<<(measured*o.data_ops)
      <<" warmup_ms="<<o.warmup_ms<<" measure_ms="<<o.measure_ms
      <<" completed="<<done<<" measured="<<measured<<" joins="<<joined
      <<" verified_markers="<<completed.size()<<" violations=0"
      <<" join_aggregate="<<o.join_aggregate<<" phase2_window="<<o.phase2_window
      <<" prepare_range="<<o.prepare_range<<" lookahead="<<o.lookahead
      <<" replenish_threshold="<<o.replenish_threshold<<" batch_wait_us="<<o.batch_wait_us<<" join_batches="<<actual_join_batches
      <<" actual_join_batch_mean="<<(actual_join_batches?double(join_batch_sum)/actual_join_batches:0)
      <<" prepare_records="<<prepare_records
      <<" join_key_visits="<<join_key_visits
      <<" prepare_key_visits="<<prepare_key_visits
      <<" distinct_keys_seen="<<known.size()
      <<" latency_p50_ns="<<percentile(latency,0.5)
      <<" latency_p95_ns="<<percentile(latency,0.95)
      <<" latency_p99_ns="<<percentile(latency,0.99)
      <<" admission_p50_ns="<<percentile(admission,0.5)
      <<" admission_p99_ns="<<percentile(admission,0.99)
      <<" handoff_wait_p99_ns="<<percentile(waits,0.99)
      <<" goodput_ops_per_sec="<<1000.0*measured/o.measure_ms
      <<" rx_idle_us="<<rx_idle_us
      <<" handoff_idle_us="<<handoff_idle_us
      <<" handoff_preconnect="<<handoff_preconnect
      <<" transport="<<(control_rdma?"rdma":"tcp")
      <<" probe="<<o.probe<<" work_ns="<<o.work_ns<<std::endl;
    for(size_t w=0;w<o.workers;w++)send_to(w,message(Finish));
  }catch(...){
    {std::lock_guard<std::mutex> lock(mu);closed=true;}
    for(auto& ch:channels)if(ch)ch->stop();
    for(auto fd:sockets)shutdown(fd,SHUT_RDWR);
    for(auto& t:readers)if(t.joinable())t.join();
    for(auto fd:sockets)close(fd);
    close(listenfd);
    throw;
  }
  {std::lock_guard<std::mutex> lock(mu);closed=true;}
  for(auto& ch:channels)if(ch)ch->stop();
  for(auto fd:sockets)shutdown(fd,SHUT_RDWR);
  for(auto& t:readers)if(t.joinable())t.join();
  for(auto fd:sockets)close(fd);
    close(listenfd);
  return 0;
}

// A deterministic, identical-distribution workload on the CNs. Per-context
// seeds and lock populations can be replayed by both locking backends.
struct Locks {
  uint64_t count;std::mt19937_64 rng;std::vector<double> cdf;
  explicit Locks(uint64_t n,uint64_t seed,bool zipf):count(n),rng(seed){
    if(zipf){cdf.reserve(n);double sum=0;
      for(uint64_t i=0;i<n;i++){sum+=1.0/std::pow(double(i+1),0.99);cdf.push_back(sum);}
      for(auto& x:cdf)x/=sum;}}
  uint64_t next(){if(cdf.empty())return kLockBase+(rng()%count);
    std::uniform_real_distribution<double> uniform(0,1);double v=uniform(rng);
    return kLockBase+size_t(std::lower_bound(cdf.begin(),cdf.end(),v)-cdf.begin());}
};
int worker(int argc,char**argv){
  // worker coordinator:port index self_ip listen_port [device]
  if(argc<6||argc>7)fail("worker <coordinator:port> <index> <self-ip> <listen-port> [device]");
  auto [leader_host,leader_port]=endpoint(argv[2]);auto id=number(argv[3],0,kMaxWorkers-1,"worker index");
  auto self_ip=std::string(argv[4]);auto local_port=uint16_t(number(argv[5],1024,65535,"listen port"));
  std::string device=argc>6?argv[6]:"mlx4_0";
  // A bounded one-sided RNIC inbox. Unique grant IDs prevent slot aliasing.
  const auto slots=(getenv("MUCQL_PAXOS_SLOTS")?number(getenv("MUCQL_PAXOS_SLOTS"),4096,kMaxSlots,"slots"):4096);
  auto* inbox=new VerbsReleaseServer(release_opts(device,slots));
  std::thread([inbox,local_port]{inbox->listen_forever(local_port);}).detach();
  std::this_thread::sleep_for(std::chrono::milliseconds(120));
  int fd=connect_tcp(leader_host,leader_port);
  Hello h{};h.magic=kMagic;h.index=id;h.port=local_port;host_to(h.ip,self_ip);
  send_exact(fd,&h,sizeof(h));
  Config cfg{};recv_exact(fd,&cfg,sizeof(cfg));
  const bool control_rdma=rdma_admission();
  const auto rx_idle_us=env_number("MUCQL_RX_IDLE_US",0,100);
  const auto handoff_idle_us=env_number("MUCQL_HANDOFF_IDLE_US",0,100);
  const bool handoff_preconnect=env_number("MUCQL_HANDOFF_PRECONNECT",1,1)!=0;
  (void)rx_idle_us; // LiveControlChannel reads the same validated setting.
  if(cfg.magic!=kMagic||cfg.self!=id||cfg.workers>kMaxWorkers||cfg.workers<2||
     cfg.paxos_slots!=slots||cfg.lock_count<1||cfg.lock_count>kMaxLocks||
     cfg.transport!=(control_rdma?1ULL:0ULL))fail("bad LIVE config/slot/transport mismatch");
  std::vector<Peer> peers(cfg.workers);recv_exact(fd,peers.data(),sizeof(Peer)*peers.size());
  LiveControlChannel channel(fd,device,control_port(),control_rdma,sizeof(Message));
  Options o{};o.paxos_slots=slots;o.lock_count=cfg.lock_count;o.work_ns=cfg.work_ns;
  o.probe=cfg.probe;o.warmup_ms=cfg.warmup_ms;o.measure_ms=cfg.measure_ms;
  o.seed=cfg.seed;o.zipf=cfg.zipf;o.max_ops=cfg.max_ops;o.data_ops=cfg.data_ops;
  if(o.data_ops<1||o.data_ops>32)fail("bad LIVE data_ops in coordinator config");
  if(const char* n=getenv("MUCQL_DATA_OPS"))
    if(number(n,1,32,"data_ops")!=o.data_ops)fail("worker/coordinator data_ops mismatch");
  VerbsReleaseClient release(host_of(cfg.data_host),uint16_t(cfg.data_port),release_opts(device,release_slots(o)));
  std::unique_ptr<VerbsReleaseClient> probe;
  if(o.probe)probe=std::make_unique<VerbsReleaseClient>(host_of(cfg.data_host),uint16_t(cfg.data_port),release_opts(device,release_slots(o)));
  if(release.slots()<release_slots(o))fail("data server lacks LIVE slots");
  std::mutex mu;std::condition_variable cv;std::deque<Ticket> assigned,successors;
  std::set<GrantId> retired;
  std::string stream_error;bool go=false,stop=false,finished=false;
  std::thread reader([&]{try{
    for(;;){Message m{};channel.recv(&m,sizeof(m));if(m.magic!=kMagic)fail("bad coordinator message");
      {std::lock_guard<std::mutex> lock(mu);
        if(m.type==TicketReady){auto t=unpack(m.ticket);if(t.client!=id+1||t.grant_id>=slots)fail("foreign/invalid ticket");assigned.push_back(t);}
        else if(m.type==HandoffReady){auto t=unpack(m.ticket);if(t.grant_id>=slots||!t.predecessor_id)fail("invalid handoff");successors.push_back(t);}
        else if(m.type==Go)go=true;
        else if(m.type==Stop)stop=true;
        else if(m.type==Finish){finished=true;cv.notify_all();break;}
        else fail("invalid control type");}
      cv.notify_all();}
    }catch(const std::exception& e){std::lock_guard<std::mutex> lock(mu);stream_error=e.what();cv.notify_all();}
  });
  std::thread handoff([&]{try{
    std::map<ClientId,std::unique_ptr<VerbsReleaseClient>> outgoing;
    for(;;){Ticket successor;
      {std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock,[&]{return !stream_error.empty()||finished||!successors.empty();});
        if(!stream_error.empty())break;
        if(successors.empty()){if(finished)break;continue;}
        successor=successors.front();successors.pop_front();
      }
      if(successor.client<1||successor.client>cfg.workers)fail("bad successor client");
      auto& remote=outgoing[successor.client];
      // Pre-connect as soon as the committed PREPARE reveals the successor.
      // This is only bootstrap work: the CAS is still forbidden until the
      // predecessor has retired and its release marker has completed.
      if(handoff_preconnect && !remote)
        remote=std::make_unique<VerbsReleaseClient>(host_of(peers[successor.client-1].ip),
          uint16_t(peers[successor.client-1].port),release_opts(device,slots));
      {
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock,[&]{return !stream_error.empty()||retired.count(successor.predecessor_id);});
        if(!stream_error.empty())break;
      }
      if(!remote)remote=std::make_unique<VerbsReleaseClient>(host_of(peers[successor.client-1].ip),
        uint16_t(peers[successor.client-1].port),release_opts(device,slots));
      auto old=remote->compare_exchange(successor.grant_id,0,successor.grant_id);
      if(old!=0&&old!=successor.grant_id)fail("conflicting one-sided handoff");
    }
  }catch(const std::exception& e){std::lock_guard<std::mutex> lock(mu);stream_error=e.what();cv.notify_all();}});
  {char ready='R';send_exact(fd,&ready,1);}
  auto finish_threads=[&]{
    channel.stop();
    shutdown(fd,SHUT_RDWR);
    if(reader.joinable())reader.join();
    {std::lock_guard<std::mutex> lock(mu);finished=true;}cv.notify_all();
    if(handoff.joinable())handoff.join();
    close(fd);
  };
  try{
    {std::unique_lock<std::mutex> lock(mu);cv.wait(lock,[&]{return go||!stream_error.empty();});
      if(!stream_error.empty())fail(stream_error);}
    auto start=ClockType::now();
    auto deadline=start+std::chrono::milliseconds(o.warmup_ms+o.measure_ms);
    Locks locks(o.lock_count,o.seed^(0x9e3779b97f4a7c15ULL*(id+1)),o.zipf);
    Client client(id+1);
    uint64_t request=0,completed=0,measured=0;
    for(;;){
      {std::lock_guard<std::mutex> lock(mu);
        if(stop||ClockType::now()>=deadline)break;
        if(!stream_error.empty())fail(stream_error);}
      auto send_at=ClockType::now();
      Ticket asked{};asked.client=id+1;asked.lock=locks.next();asked.request=++request;
      {auto join_msg=message(Join,asked);channel.send(&join_msg,sizeof(join_msg));}
      Ticket t;
      {std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock,[&]{return !assigned.empty()||!stream_error.empty();});
        if(!stream_error.empty())fail(stream_error);
        t=assigned.front();assigned.pop_front();}
      if(t.client!=id+1||t.request!=request||t.lock!=asked.lock)fail("out-of-order ticket response");
      auto admit_at=ClockType::now();
      client.on_prepared(t);
      uint64_t handoff_wait_ns=0;
      if(t.position==0){if(!client.activate_start(t,true,EndlessClock{}))fail("head start rejected");}
      else{
        auto wait_from=ClockType::now();
        uint32_t handoff_polls=0;
        for(;;){auto current=inbox->slot_value(t.grant_id);
          if(current==t.grant_id)break;
          if(current!=0)fail("stale/foreign CAS in inbox");
          {std::lock_guard<std::mutex> lock(mu);if(!stream_error.empty())fail(stream_error);}
          if(ClockType::now()-wait_from>std::chrono::seconds(30))fail("handoff starvation timeout");
          if(handoff_idle_us && (++handoff_polls%32)==0)
            std::this_thread::sleep_for(std::chrono::microseconds(handoff_idle_us));
          else
            std::this_thread::yield();}
        handoff_wait_ns=std::chrono::duration_cast<ns>(ClockType::now()-wait_from).count();
        if(!client.activate_handoff({t,t.predecessor_id,t.position-1},EndlessClock{}))fail("handoff activation rejected");
      }
      if(!client.may_use(t,EndlessClock{}))fail("lock ownership denied");
      if(probe&&probe->compare_exchange(owner_slot(o,t.lock),0,t.grant_id)!=0)fail("MUTUAL EXCLUSION VIOLATION");
      // DecLock's exclusive microbenchmark submits N sequential 8-byte
      // RDMA WRITEs and waits for each CQ completion while holding the lock.
      // Every WRITE targets the protected per-key value on the data MN.
      // This is actual RNIC activity, not a local CPU delay.
      for(uint64_t shot=0;shot<o.data_ops;++shot)
        release.write_slot(data_slot(o,t.lock),t.grant_id);
      if(o.work_ns)busy_for(o.work_ns);
      if(probe&&probe->compare_exchange(owner_slot(o,t.lock),t.grant_id,0)!=t.grant_id)fail("owner slot changed");
      auto action=client.retire(t,std::nullopt);
      if(action.handoff)fail("unexpected speculative handoff");
      release.post(t.grant_id,t.grant_id,ReleaseMode::AtomicCas);
      if(!release.complete())fail("release marker CAS mismatch");
      {std::lock_guard<std::mutex> lock(mu);retired.insert(t.grant_id);}cv.notify_all();
      auto completed_at=ClockType::now();
      Message done=message(Done,t);
      auto elapsed=completed_at-start;
      if(elapsed>=std::chrono::milliseconds(o.warmup_ms)&&elapsed<std::chrono::milliseconds(o.warmup_ms+o.measure_ms)){
        done.flags=1;measured++;
      }
      done.latency_ns=std::chrono::duration_cast<ns>(completed_at-send_at).count();
      done.admit_ns=std::chrono::duration_cast<ns>(admit_at-send_at).count();
      done.wait_ns=handoff_wait_ns;
      channel.send(&done,sizeof(done));
      completed++;
    }
    {auto qmsg=message(Quiescent);channel.send(&qmsg,sizeof(qmsg));}
    {std::unique_lock<std::mutex> lock(mu);
      cv.wait(lock,[&]{return finished||!stream_error.empty();});
      if(!stream_error.empty())fail(stream_error);
    }
    finish_threads();
    std::cout<<"LIVE_WORKER id="<<id<<" completed="<<completed<<" measured="<<measured<<" status=PASS"<<std::endl;
    return 0;
  }catch(...){finish_threads();throw;}
}
}
#ifndef MUCQL_LIVE_LIBRARY
int main(int argc,char**argv){
  try{if(argc<2)throw std::runtime_error("usage: mucql_lock_live coordinator|worker ...");
    const auto cmd=std::string(argv[1]);
    if(cmd=="coordinator")return mucql_live::coordinator(argc,argv);
    if(cmd=="worker")return mucql_live::worker(argc,argv);
    throw std::runtime_error("unknown command");
  }catch(const std::exception& e){std::cerr<<"LIVE ERROR: "<<e.what()<<std::endl;return 1;}
}
#endif
