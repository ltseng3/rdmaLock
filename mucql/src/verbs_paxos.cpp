#include "mucql/verbs_paxos.hpp"
#include <infiniband/verbs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <unistd.h>
#include <cerrno>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>
#include <memory>
namespace mucql {
namespace {
[[noreturn]] void fail(const std::string& s){throw std::runtime_error("RDMA Paxos: "+s);}
void send_all(int f,const void*p,size_t n){auto*b=static_cast<const char*>(p);
  while(n){auto k=::send(f,b,n,MSG_NOSIGNAL);if(k<0&&errno==EINTR)continue;
    if(k<=0)fail("TCP send");
    b+=k;n-=static_cast<size_t>(k);}}
void recv_all(int f,void*p,size_t n){auto*b=static_cast<char*>(p);
  while(n){auto k=::recv(f,b,n,0);if(k<0&&errno==EINTR)continue;
    if(k==0)fail("TCP receive: peer closed connection during phase-1 handshake");
    if(k<0)fail("TCP receive: "+std::string(std::strerror(errno)));
    b+=k;n-=static_cast<size_t>(k);}}
uint32_t gen_psn(){return static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count())&0xffffff;}
constexpr uint64_t kMagic=0x4d5543514c524431ULL; // MUCQLRD1
struct Control {uint64_t magic,ballot,flags;};
struct Answer {uint64_t magic,ok,count;};
struct QpWire {uint64_t addr;uint32_t rkey,qpn,psn,slots;uint16_t lid;uint8_t port,pad;};
static_assert(sizeof(QpWire)==32,"bad QpWire");
struct ValueWire {uint64_t index,type,lock,client,request,first,last,deadline;};
static_assert(sizeof(ValueWire)==64,"bad ValueWire");
struct AcceptedWire {uint64_t ballot;ValueWire v;};
static_assert(sizeof(AcceptedWire)==72,"bad AcceptedWire");
struct alignas(64) AcceptedSlot {ValueWire v;uint64_t marker;uint8_t padding[56];};
static_assert(sizeof(AcceptedSlot)==128,"bad AcceptedSlot");
ValueWire pack(const Record&r){return {r.index,static_cast<uint64_t>(r.type),r.lock,r.client,r.request,r.first,r.last,r.deadline};}
Record unpack(const ValueWire&w){if(w.type>1||!w.index)fail("invalid record");
  Record r{};r.index=w.index;r.type=static_cast<Record::Type>(w.type);
  r.lock=w.lock;r.client=w.client;r.request=w.request;r.first=w.first;r.last=w.last;r.deadline=w.deadline;return r;}
uint64_t mark(uint64_t i){if(i>UINT64_MAX/2)fail("index overflow");return i*2+1;}
struct Dev {
  ibv_context* ctx{};ibv_pd* pd{};ibv_port_attr attr{};uint8_t port{};
  explicit Dev(const RdmaPaxosOptions&o):port(o.port){
    int count=0;auto** d=ibv_get_device_list(&count);if(!d||count==0)fail("no RNIC");
    ibv_device* selected=nullptr;
    for(int i=0;i<count;i++){
      if(!o.device.empty()&&o.device!=ibv_get_device_name(d[i]))continue;
      auto*c=ibv_open_device(d[i]);if(!c)continue;
      ibv_port_attr a{};auto good=!ibv_query_port(c,port,&a)&&a.state==IBV_PORT_ACTIVE&&a.link_layer==IBV_LINK_LAYER_INFINIBAND;
      ibv_close_device(c);if(good){selected=d[i];break;}
    }
    if(!selected){ibv_free_device_list(d);fail("no active IB port (choose mlx4 device via ibv_devinfo)");}
    ctx=ibv_open_device(selected);ibv_free_device_list(d);
    if(!ctx||ibv_query_port(ctx,port,&attr))fail("query port");
    pd=ibv_alloc_pd(ctx);if(!pd)fail("alloc pd");
  }
  ~Dev(){if(pd)ibv_dealloc_pd(pd);if(ctx)ibv_close_device(ctx);}
};
struct QP {
  Dev& d;ibv_cq* cq{};ibv_qp* qp{};uint32_t psn{};
  explicit QP(Dev&d):d(d),psn(gen_psn()){
    cq=ibv_create_cq(d.ctx,128,nullptr,nullptr,0);if(!cq)fail("create cq");
    ibv_qp_init_attr init{};init.send_cq=cq;init.recv_cq=cq;init.qp_type=IBV_QPT_RC;
    init.cap.max_send_wr=128;init.cap.max_recv_wr=2;init.cap.max_send_sge=1;init.cap.max_recv_sge=1;
    qp=ibv_create_qp(d.pd,&init);if(!qp)fail("create QP");
    ibv_qp_attr a{};a.qp_state=IBV_QPS_INIT;a.pkey_index=0;a.port_num=d.port;
    a.qp_access_flags=IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_ATOMIC;
    if(ibv_modify_qp(qp,&a,IBV_QP_STATE|IBV_QP_PKEY_INDEX|IBV_QP_PORT|IBV_QP_ACCESS_FLAGS))fail("QP init");
  }
  ~QP(){if(qp)ibv_destroy_qp(qp);if(cq)ibv_destroy_cq(cq);}
  QpWire info(uintptr_t a=0,uint32_t key=0,uint32_t slots=0)const {
    return {a,key,qp->qp_num,psn,slots,d.attr.lid,d.port,0};
  }
  void connect(const QpWire& peer){
    ibv_qp_attr a{};a.qp_state=IBV_QPS_RTR;a.path_mtu=IBV_MTU_1024;
    a.dest_qp_num=peer.qpn;a.rq_psn=peer.psn;a.max_dest_rd_atomic=1;a.min_rnr_timer=12;
    a.ah_attr.is_global=0;a.ah_attr.dlid=peer.lid;a.ah_attr.port_num=d.port;
    if(ibv_modify_qp(qp,&a,IBV_QP_STATE|IBV_QP_AV|IBV_QP_PATH_MTU|IBV_QP_DEST_QPN|
       IBV_QP_RQ_PSN|IBV_QP_MAX_DEST_RD_ATOMIC|IBV_QP_MIN_RNR_TIMER))fail("QP RTR");
    a={};a.qp_state=IBV_QPS_RTS;a.sq_psn=psn;a.timeout=14;a.retry_cnt=7;
    a.rnr_retry=7;a.max_rd_atomic=1;
    if(ibv_modify_qp(qp,&a,IBV_QP_STATE|IBV_QP_SQ_PSN|IBV_QP_TIMEOUT|
       IBV_QP_RETRY_CNT|IBV_QP_RNR_RETRY|IBV_QP_MAX_QP_RD_ATOMIC))fail("QP RTS");
  }
  void fence(){
    if(!qp)return;
    ibv_qp_attr a{};a.qp_state=IBV_QPS_ERR;
    if(ibv_modify_qp(qp,&a,IBV_QP_STATE))fail("failed to fence old leader QP");
  }
  void poll(){auto start=std::chrono::steady_clock::now();
    while(true){ibv_wc wc{};int count=ibv_poll_cq(cq,1,&wc);
      if(count<0)fail("CQ poll failed");
      if(count==1){if(wc.status!=IBV_WC_SUCCESS)fail(ibv_wc_status_str(wc.status));return;}
      if(std::chrono::steady_clock::now()-start>std::chrono::seconds(3))fail("CQ timeout");
      std::this_thread::yield();
    }
  }
};
int listen_tcp(uint16_t port){int s=socket(AF_INET,SOCK_STREAM,0);if(s<0)fail("socket");
  int yes=1;setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));
  sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(port);a.sin_addr.s_addr=INADDR_ANY;
  if(bind(s,reinterpret_cast<sockaddr*>(&a),sizeof(a))||listen(s,64))fail("listen");
  return s;}
int connect_tcp(const std::string&h,uint16_t port){addrinfo hints{};hints.ai_socktype=SOCK_STREAM;
  addrinfo* res=nullptr;auto p=std::to_string(port);if(getaddrinfo(h.c_str(),p.c_str(),&hints,&res))fail("DNS");
  int sock=-1;for(auto* a=res;a;a=a->ai_next){int s=socket(a->ai_family,a->ai_socktype,a->ai_protocol);
    if(s<0)continue;
    if(::connect(s,a->ai_addr,a->ai_addrlen)==0){sock=s;break;}
    ::close(s);}
  freeaddrinfo(res);if(sock<0)fail("connect to "+h);return sock;}
void set_timeout(int fd){timeval t{};t.tv_sec=10;
  if(setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&t,sizeof(t))!=0 ||
     setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&t,sizeof(t))!=0)
    fail("setting TCP phase-1 timeout: "+std::string(std::strerror(errno)));
}
void clear_receive_timeout(int fd){
  // A prepared QP must stay alive while its leader is active, including
  // multi-second benchmark measurements. Never expire it just for idleness.
  timeval t{};
  if(setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&t,sizeof(t))!=0)
    fail("clearing TCP session timeout: "+std::string(std::strerror(errno)));
}
struct Active {
  Dev& d;uint64_t ballot{};uint32_t slots{};void* mem{};ibv_mr* mr{};QP qp;
  explicit Active(Dev&d,uint64_t b,uint32_t n):d(d),ballot(b),slots(n),qp(d){
    if(posix_memalign(&mem,4096,static_cast<size_t>(n)*sizeof(AcceptedSlot)))fail("malloc");
    std::memset(mem,0,static_cast<size_t>(n)*sizeof(AcceptedSlot));
    mr=ibv_reg_mr(d.pd,mem,static_cast<size_t>(n)*sizeof(AcceptedSlot),
        IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_ATOMIC);
    if(!mr){
      const int error=errno;
      rlimit limit{};
      const bool have_limit=getrlimit(RLIMIT_MEMLOCK,&limit)==0;
      const auto soft=have_limit?
        (limit.rlim_cur==RLIM_INFINITY?std::string("unlimited"):
         std::to_string(static_cast<unsigned long long>(limit.rlim_cur))):
        std::string("unavailable");
      std::free(mem);mem=nullptr;
      fail("accepted MR registration: slots="+std::to_string(n)+
           " bytes="+std::to_string(static_cast<size_t>(n)*sizeof(AcceptedSlot))+
           " errno="+std::to_string(error)+" ("+std::strerror(error)+
           ") RLIMIT_MEMLOCK_soft="+soft);
    }
  }
  ~Active(){if(mr)ibv_dereg_mr(mr);std::free(mem);}
  QpWire info()const{return qp.info(reinterpret_cast<uintptr_t>(mem),mr->rkey,slots);}
  void collect(std::map<uint64_t,Accepted>& out)const {
    auto* entries=static_cast<const AcceptedSlot*>(mem);
    for(uint64_t i=0;i<slots;i++){
      uint64_t m=__atomic_load_n(&entries[i].marker,__ATOMIC_ACQUIRE);
      if(m!=mark(i+1))continue;
      ValueWire v=entries[i].v;
      if(v.index!=i+1)fail("corrupt accepted slot marker/value");
      auto r=unpack(v);auto it=out.find(r.index);
      if(it==out.end()||it->second.ballot<ballot)out[r.index]=Accepted{ballot,r};
    }
  }
};
struct Server {
  Dev dev;RdmaPaxosOptions opts;std::mutex mu;uint64_t promised{};
  std::map<uint64_t,Accepted> accepted;
  std::shared_ptr<Active> current;
  explicit Server(const RdmaPaxosOptions&o):dev(o),opts(o){
    if(o.slots<1||o.slots>1000000)fail("slots range");
    // The previous design registered the entire accepted log only AFTER
    // announcing TCP readiness. Large logs could therefore pass readiness
    // and fail on all three followers during phase 1. Provision the first
    // session here, before declaring a healthy server; reuse its QP/MR for
    // the first ballot so startup doesn't allocate the pinned log twice.
    std::cout<<"RDMA Paxos provisioning accepted log slots="<<o.slots
             <<" bytes="<<(size_t(o.slots)*sizeof(AcceptedSlot))<<std::endl;
    current=std::make_shared<Active>(dev,0,opts.slots);
  }
  void serve(int c){
    try{
      set_timeout(c);Control hello{};recv_all(c,&hello,sizeof(hello));
      if(hello.magic!=kMagic||hello.ballot==0)fail("bad prepare request");
      std::shared_ptr<Active> session;
      {
        // Serialize all ballot transitions; old QP fencing MUST precede the
        // phase-1 accepted-state snapshot and the promise to a new leader.
        std::lock_guard<std::mutex> l(mu);
        if(hello.ballot<=promised ||
           (promised && !opts.allow_experimental_takeover)){
          Answer no{kMagic,0,0};send_all(c,&no,sizeof(no));::close(c);return;
        }
        if(current && current->ballot==0){
          // First ballot uses the already registered but unused accepted log.
          // The first QP is still in INIT; never fence an unused QP.
          current->ballot=hello.ballot;
          session=current;
        }else{
          if(current){current->qp.fence();current->collect(accepted);}
          session=std::make_shared<Active>(dev,hello.ballot,opts.slots);
          current=session;
        }
        promised=hello.ballot;
        Answer ok{kMagic,1,accepted.size()};send_all(c,&ok,sizeof(ok));
        for(const auto& kv:accepted){AcceptedWire x{kv.second.ballot,pack(kv.second.value)};
          send_all(c,&x,sizeof(x));}
        auto remote=session->info();send_all(c,&remote,sizeof(remote));
        QpWire leader{};recv_all(c,&leader,sizeof(leader));session->qp.connect(leader);
        uint8_t ready=0x51;send_all(c,&ready,1);
      }
      // This is an established RC QP, not a phase-1 request. Clearing the
      // receive timeout keeps the bootstrap socket/QP alive until the leader
      // disconnects, independent of measurement duration.
      clear_receive_timeout(c);
      uint8_t dummy=0;while(::recv(c,&dummy,1,0)>0){}
    }catch(const std::exception&e){std::cerr<<"RDMA acceptor session: "<<e.what()<<'\n';}
    ::close(c);
  }
};
}
struct RdmaAcceptor::Impl {
  Dev dev;std::unique_ptr<QP> qp;RdmaPaxosOptions opts;std::string host;uint16_t port{};
  int sock{-1};uint64_t prepared_ballot{};QpWire remote{};
  ValueWire value{};uint64_t atomic_old{};ibv_mr* value_mr{};ibv_mr* atomic_mr{};
  static constexpr size_t kBatchCap=32;
  std::array<ValueWire,kBatchCap> batch_values{};
  std::array<uint64_t,kBatchCap> batch_old{};
  ibv_mr* batch_values_mr{};ibv_mr* batch_old_mr{};
  std::mutex mu;bool pending{false};bool poisoned{false};size_t pending_batch{0};
  std::map<uint64_t,ValueWire> posted; // forbid same-ballot slot equivocation
  Impl(std::string h,uint16_t p,RdmaPaxosOptions o):dev(o),opts(std::move(o)),host(std::move(h)),port(p){
    value_mr=ibv_reg_mr(dev.pd,&value,sizeof(value),IBV_ACCESS_LOCAL_WRITE);
    atomic_mr=ibv_reg_mr(dev.pd,&atomic_old,sizeof(atomic_old),IBV_ACCESS_LOCAL_WRITE);
    batch_values_mr=ibv_reg_mr(dev.pd,batch_values.data(),sizeof(batch_values),IBV_ACCESS_LOCAL_WRITE);
    batch_old_mr=ibv_reg_mr(dev.pd,batch_old.data(),sizeof(batch_old),IBV_ACCESS_LOCAL_WRITE);
    if(!value_mr||!atomic_mr||!batch_values_mr||!batch_old_mr)
      fail("register local accepted buffers");
  }
  ~Impl(){if(sock>=0)::close(sock);if(value_mr)ibv_dereg_mr(value_mr);
    if(atomic_mr)ibv_dereg_mr(atomic_mr);
    if(batch_values_mr)ibv_dereg_mr(batch_values_mr);
    if(batch_old_mr)ibv_dereg_mr(batch_old_mr);}
};
RdmaAcceptor::RdmaAcceptor(std::string host,uint16_t p,RdmaPaxosOptions o):impl_(new Impl(std::move(host),p,std::move(o))){}
RdmaAcceptor::~RdmaAcceptor()=default;
Promise RdmaAcceptor::prepare(uint64_t ballot){
  std::lock_guard<std::mutex> l(impl_->mu);
  if(impl_->sock>=0){::close(impl_->sock);impl_->sock=-1;}
  impl_->prepared_ballot=0;
  // A new QP is mandatory on each ballot; reconnecting an old QP would
  // defeat acceptor-side permission fencing of lower-ballot leaders.
  std::string stage="opening QP and TCP bootstrap";
  try{
    impl_->qp=std::make_unique<QP>(impl_->dev);
    impl_->sock=connect_tcp(impl_->host,impl_->port);set_timeout(impl_->sock);
    stage="waiting for phase-1 promise";
    Control h{kMagic,ballot,0};send_all(impl_->sock,&h,sizeof(h));
    Answer response{};recv_all(impl_->sock,&response,sizeof(response));
    stage="receiving accepted state";
    if(response.magic!=kMagic||!response.ok||response.count>impl_->opts.slots)return {};
    Promise promise{};promise.granted=true;
    for(uint64_t i=0;i<response.count;i++){
      AcceptedWire entry{};recv_all(impl_->sock,&entry,sizeof(entry));
      auto val=unpack(entry.v);
      promise.accepted[val.index]=Accepted{entry.ballot,val};
    }
    recv_all(impl_->sock,&impl_->remote,sizeof(impl_->remote));
    if(impl_->remote.slots!=impl_->opts.slots)fail("slot count mismatch");
    stage="connecting RC QP";
    auto local=impl_->qp->info();send_all(impl_->sock,&local,sizeof(local));
    impl_->qp->connect(impl_->remote);
    stage="waiting for RC QP ready";
    uint8_t ready=0;recv_all(impl_->sock,&ready,1);if(ready!=0x51)fail("QP handshake");
    impl_->posted.clear();
    impl_->poisoned=false;impl_->pending=false;impl_->pending_batch=0;
    impl_->prepared_ballot=ballot;
    return promise;
  }catch(const std::exception&e){
    std::cerr<<"RDMA prepare failed at "<<impl_->host<<":"<<impl_->port
             <<" ["<<stage<<"]: "<<e.what()<<'\n';
    return {};
  }
}
bool RdmaAcceptor::accept(uint64_t ballot,const Record& r){
  begin_accept(ballot,r);
  return finish_accept();
}
void RdmaAcceptor::begin_accept(uint64_t ballot,const Record&r){
  std::lock_guard<std::mutex> l(impl_->mu);
  if(impl_->pending || impl_->pending_batch || impl_->poisoned)
    throw std::logic_error("undrained/failed RDMA QP: recover with a higher ballot");
  if(!impl_->qp || ballot!=impl_->prepared_ballot||r.index==0||r.index>impl_->remote.slots)
    throw std::runtime_error("unprepared ballot or out-of-range slot");
  ValueWire w=pack(r);
  auto previous=impl_->posted.find(r.index);
  if(previous!=impl_->posted.end() &&
     std::memcmp(&previous->second,&w,sizeof(w))!=0)
    throw std::logic_error("same-ballot accepted-slot equivocation");
  impl_->posted[r.index]=w;
  impl_->value=w;impl_->atomic_old=0;
  const uint64_t base=impl_->remote.addr+(r.index-1)*sizeof(AcceptedSlot);
  ibv_sge data{};data.addr=reinterpret_cast<uintptr_t>(&impl_->value);
  data.length=sizeof(ValueWire);data.lkey=impl_->value_mr->lkey;
  ibv_sge cas_result{};cas_result.addr=reinterpret_cast<uintptr_t>(&impl_->atomic_old);
  cas_result.length=8;cas_result.lkey=impl_->atomic_mr->lkey;
  ibv_send_wr write{};write.sg_list=&data;write.num_sge=1;write.opcode=IBV_WR_RDMA_WRITE;
  write.wr.rdma.remote_addr=base;write.wr.rdma.rkey=impl_->remote.rkey;
  ibv_send_wr cas{};cas.sg_list=&cas_result;cas.num_sge=1;
  cas.opcode=IBV_WR_ATOMIC_CMP_AND_SWP;cas.send_flags=IBV_SEND_SIGNALED;
  cas.wr.atomic.remote_addr=base+offsetof(AcceptedSlot,marker);
  cas.wr.atomic.rkey=impl_->remote.rkey;cas.wr.atomic.compare_add=0;
  cas.wr.atomic.swap=mark(r.index);
  write.next=&cas;
  ibv_send_wr* bad=nullptr;
  if(ibv_post_send(impl_->qp->qp,&write,&bad)){
    impl_->poisoned=true;
    throw std::runtime_error("posting RDMA phase-2 failed; QP cannot be reused");
  }
  impl_->pending=true;
}
bool RdmaAcceptor::finish_accept(){
  std::lock_guard<std::mutex> l(impl_->mu);
  if(!impl_->pending)return false;
  try{impl_->qp->poll();}
  catch(const std::exception&e){
    // QP may still carry outstanding operations. This object is not reused
    // until prepare() creates a fresh QP for a strictly higher ballot.
    impl_->pending=false;
    impl_->poisoned=true;
    std::cerr<<"RDMA phase-2 completion failed: "<<e.what()<<'\n';
    return false;
  }
  impl_->pending=false;
  return impl_->atomic_old==0||impl_->atomic_old==mark(impl_->value.index);
}
void RdmaAcceptor::begin_accept_batch(uint64_t ballot,
                                     const std::vector<Record>& records){
  std::lock_guard<std::mutex> l(impl_->mu);
  if(records.empty() || records.size()>Impl::kBatchCap)throw std::invalid_argument("bad RDMA batch size");
  if(impl_->pending||impl_->pending_batch||impl_->poisoned)
    throw std::logic_error("undrained/failed RDMA batch QP: recover with higher ballot");
  if(!impl_->qp || ballot!=impl_->prepared_ballot)
    throw std::runtime_error("RDMA batch submitted on unprepared ballot");
  for(size_t i=0;i<records.size();++i){
    const auto& r=records[i];
    if(r.index==0||r.index>impl_->remote.slots ||
       (i && r.index!=records[i-1].index+1))
      throw std::runtime_error("invalid RDMA batch index/contiguity");
    ValueWire w=pack(r);
    auto prev=impl_->posted.find(r.index);
    if(prev!=impl_->posted.end()&&std::memcmp(&prev->second,&w,sizeof(w))!=0)
      throw std::logic_error("same-ballot RDMA batch slot equivocation");
    impl_->batch_values[i]=w;
    impl_->batch_old[i]=0;
  }
  // All local buffers have stable addresses and remain registered/pinned until
  // every CAS CQE returns. RC QP ordering makes each slot's payload visible
  // before its atomic marker, and orders all slots as a contiguous prefix.
  for(size_t i=0;i<records.size();++i){
    const auto& r=records[i];
    impl_->posted[r.index]=impl_->batch_values[i];
    const uint64_t base=impl_->remote.addr+(r.index-1)*sizeof(AcceptedSlot);
    ibv_sge payload{};payload.addr=reinterpret_cast<uintptr_t>(&impl_->batch_values[i]);
    payload.length=sizeof(ValueWire);payload.lkey=impl_->batch_values_mr->lkey;
    ibv_sge result{};result.addr=reinterpret_cast<uintptr_t>(&impl_->batch_old[i]);
    result.length=8;result.lkey=impl_->batch_old_mr->lkey;
    ibv_send_wr write{};write.sg_list=&payload;write.num_sge=1;write.opcode=IBV_WR_RDMA_WRITE;
    write.wr.rdma.remote_addr=base;write.wr.rdma.rkey=impl_->remote.rkey;
    ibv_send_wr cas{};cas.sg_list=&result;cas.num_sge=1;
    cas.opcode=IBV_WR_ATOMIC_CMP_AND_SWP;cas.send_flags=IBV_SEND_SIGNALED;
    cas.wr.atomic.remote_addr=base+offsetof(AcceptedSlot,marker);
    cas.wr.atomic.rkey=impl_->remote.rkey;cas.wr.atomic.compare_add=0;
    cas.wr.atomic.swap=mark(r.index);
    write.next=&cas;
    ibv_send_wr* bad=nullptr;
    if(ibv_post_send(impl_->qp->qp,&write,&bad)){
      impl_->poisoned=true;
      throw std::runtime_error("posting RDMA batch failed; QP cannot be reused");
    }
  }
  impl_->pending_batch=records.size();
}
std::vector<bool> RdmaAcceptor::finish_accept_batch(){
  std::lock_guard<std::mutex> l(impl_->mu);
  const size_t n=impl_->pending_batch;
  if(!n)return {};
  try{
    for(size_t i=0;i<n;++i)impl_->qp->poll();
  }catch(const std::exception& e){
    impl_->pending_batch=0;impl_->poisoned=true;
    std::cerr<<"RDMA phase-2 batch completion failed: "<<e.what()<<'\n';
    return std::vector<bool>(n,false);
  }
  std::vector<bool> result;
  for(size_t i=0;i<n;++i){
    auto idx=impl_->batch_values[i].index;
    result.push_back(impl_->batch_old[i]==0||impl_->batch_old[i]==mark(idx));
  }
  impl_->pending_batch=0;
  return result;
}
void serve_rdma_paxos(uint16_t port,const RdmaPaxosOptions& o){
  Server server(o);int fd=listen_tcp(port);
  std::cout<<"RDMA Paxos acceptor listening TCP "<<port<<" on device "
           <<(o.device.empty()?"AUTO":o.device)<<"; slots="<<o.slots
           <<" takeover="<<(o.allow_experimental_takeover?"EXPERIMENTAL":"DISABLED")<<std::endl;
  for(;;){int c=::accept(fd,nullptr,nullptr);if(c<0){if(errno==EINTR)continue;fail("accept");}
    std::thread([&server,c]{server.serve(c);}).detach();}
}
} // namespace mucql
