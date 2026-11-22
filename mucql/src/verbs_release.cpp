#include "mucql/verbs_release.hpp"
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace mucql {
namespace {
[[noreturn]] void fail(const std::string& s) { throw std::runtime_error("ibverbs release: "+s); }
void send_exact(int fd,const void* p,size_t n) {
  auto b=static_cast<const char*>(p);
  while(n) { ssize_t k=::send(fd,b,n,MSG_NOSIGNAL); if(k<0 && errno==EINTR)continue;
    if(k<=0) fail("TCP send failed");
    b+=k;n-=static_cast<size_t>(k); }
}
void recv_exact(int fd,void* p,size_t n) {
  auto b=static_cast<char*>(p);
  while(n) { ssize_t k=::recv(fd,b,n,0); if(k<0 && errno==EINTR)continue;
    if(k<=0) fail("TCP receive failed");
    b+=k;n-=static_cast<size_t>(k); }
}
// This simple handshake is x86_64 little-endian specific. Both endpoints on
// Apt are x86-64; add byte-order conversion if running heterogeneous hosts.
struct QpWire {
  uint64_t region_addr;
  uint32_t rkey;
  uint32_t qpn;
  uint32_t psn;
  uint32_t slots;
  uint16_t lid;
  uint8_t port;
  uint8_t reserved;
};
static_assert(sizeof(QpWire)==32,"unexpected RDMA wire structure");
uint32_t psn() { auto n=std::chrono::steady_clock::now().time_since_epoch().count();
  return static_cast<uint32_t>(n)&0x00ffffff; }
struct Device {
  ibv_context* ctx{};
  ibv_pd* pd{};
  ibv_port_attr port_attr{};
  uint8_t port{1};
  explicit Device(const ReleaseEndpointOptions& opt):port(opt.port) {
    int n=0;ibv_device** list=ibv_get_device_list(&n);
    if(!list || n==0)fail("no RDMA devices: install mlx4/rdma-core, check ibv_devinfo");
    ibv_device* dev=nullptr;
    for(int i=0;i<n;i++){
      const char* name=ibv_get_device_name(list[i]);
      if(!opt.device.empty() && opt.device!=name)continue;
      // Do not silently select a RoCE NIC: this implementation uses IB LIDs.
      auto* temp=ibv_open_device(list[i]);
      if(!temp)continue;
      ibv_port_attr a{};
      bool good=ibv_query_port(temp,port,&a)==0 && a.link_layer==IBV_LINK_LAYER_INFINIBAND && a.state==IBV_PORT_ACTIVE;
      ibv_close_device(temp);
      if(good){dev=list[i];break;}
    }
    if(!dev){ibv_free_device_list(list);fail("requested active InfiniBand device/port not found");}
    ctx=ibv_open_device(dev);
    ibv_free_device_list(list);
    if(!ctx)fail("ibv_open_device");
    if(ibv_query_port(ctx,port,&port_attr))fail("ibv_query_port");
    if(port_attr.lid==0)fail("zero LID: Apt IB subnet manager/port unavailable");
    pd=ibv_alloc_pd(ctx);if(!pd)fail("ibv_alloc_pd");
  }
  ~Device(){if(pd)ibv_dealloc_pd(pd);if(ctx)ibv_close_device(ctx);}
};
struct Qp {
  Device& dev;
  ibv_cq* cq{};
  ibv_qp* qp{};
  uint32_t local_psn{};
  explicit Qp(Device& d):dev(d),local_psn(psn()){
    cq=ibv_create_cq(dev.ctx,64,nullptr,nullptr,0);if(!cq)fail("ibv_create_cq");
    ibv_qp_init_attr a{};a.send_cq=cq;a.recv_cq=cq;a.qp_type=IBV_QPT_RC;
    a.cap.max_send_wr=64;a.cap.max_recv_wr=4;a.cap.max_send_sge=1;a.cap.max_recv_sge=1;
    a.cap.max_inline_data=0; // works on old mlx4 too
    qp=ibv_create_qp(dev.pd,&a);if(!qp)fail("ibv_create_qp");
    ibv_qp_attr q{};q.qp_state=IBV_QPS_INIT;q.pkey_index=0;q.port_num=dev.port;
    q.qp_access_flags=IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_ATOMIC|IBV_ACCESS_REMOTE_READ;
    if(ibv_modify_qp(qp,&q,IBV_QP_STATE|IBV_QP_PKEY_INDEX|IBV_QP_PORT|IBV_QP_ACCESS_FLAGS))fail("QP INIT");
  }
  ~Qp(){if(qp)ibv_destroy_qp(qp);if(cq)ibv_destroy_cq(cq);}
  void connect(const QpWire& r){
    ibv_qp_attr a{};a.qp_state=IBV_QPS_RTR;a.path_mtu=IBV_MTU_1024;
    a.dest_qp_num=r.qpn;a.rq_psn=r.psn;a.max_dest_rd_atomic=1;a.min_rnr_timer=12;
    a.ah_attr.is_global=0;a.ah_attr.dlid=r.lid;a.ah_attr.sl=0;
    a.ah_attr.src_path_bits=0;a.ah_attr.port_num=dev.port;
    if(ibv_modify_qp(qp,&a,IBV_QP_STATE|IBV_QP_AV|IBV_QP_PATH_MTU|IBV_QP_DEST_QPN|
        IBV_QP_RQ_PSN|IBV_QP_MAX_DEST_RD_ATOMIC|IBV_QP_MIN_RNR_TIMER))fail("QP RTR");
    a={};a.qp_state=IBV_QPS_RTS;a.timeout=14;a.retry_cnt=7;a.rnr_retry=7;
    a.sq_psn=local_psn;a.max_rd_atomic=1;
    if(ibv_modify_qp(qp,&a,IBV_QP_STATE|IBV_QP_TIMEOUT|IBV_QP_RETRY_CNT|IBV_QP_RNR_RETRY|
        IBV_QP_SQ_PSN|IBV_QP_MAX_QP_RD_ATOMIC))fail("QP RTS");
  }
  QpWire info(uint64_t addr,uint32_t rkey,uint32_t slots)const {
    return {addr,rkey,qp->qp_num,local_psn,slots,dev.port_attr.lid,dev.port,0};
  }
  void poll_one(){
    auto start=std::chrono::steady_clock::now();
    for(;;){ibv_wc wc{};auto n=ibv_poll_cq(cq,1,&wc);
      if(n<0)fail("ibv_poll_cq");
      if(n==1){if(wc.status!=IBV_WC_SUCCESS)fail(std::string("work completion ")+ibv_wc_status_str(wc.status));return;}
      if(std::chrono::steady_clock::now()-start>std::chrono::seconds(3))fail("CQ timeout");
      std::this_thread::yield();
    }
  }
};
int connect_tcp(const std::string& host,uint16_t port){
  addrinfo hint{};hint.ai_socktype=SOCK_STREAM;hint.ai_family=AF_UNSPEC;
  addrinfo* ai=nullptr;auto ps=std::to_string(port);
  if(getaddrinfo(host.c_str(),ps.c_str(),&hint,&ai))fail("resolve "+host);
  int fd=-1;for(auto* p=ai;p;p=p->ai_next){fd=socket(p->ai_family,p->ai_socktype,p->ai_protocol);
    if(fd<0)continue;
    if(::connect(fd,p->ai_addr,p->ai_addrlen)==0)break;
    ::close(fd);fd=-1;}
  freeaddrinfo(ai);if(fd<0)fail("TCP connect "+host);return fd;
}
int listen_tcp(uint16_t port){
  int fd=socket(AF_INET,SOCK_STREAM,0);if(fd<0)fail("socket");
  int yes=1;setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));
  sockaddr_in addr{};addr.sin_family=AF_INET;addr.sin_port=htons(port);addr.sin_addr.s_addr=INADDR_ANY;
  if(bind(fd,reinterpret_cast<sockaddr*>(&addr),sizeof(addr))||listen(fd,32))fail("TCP bind/listen");
  return fd;
}
}
struct VerbsReleaseServer::Impl {
  Device dev;void* slots_mem{};ibv_mr* mr{};uint32_t slots{};
  explicit Impl(const ReleaseEndpointOptions& o):dev(o),slots(o.slots){
    if(slots==0 || slots>10'000'000)fail("invalid slots");
    if(posix_memalign(&slots_mem,4096,static_cast<size_t>(slots)*8))fail("allocation");
    std::memset(slots_mem,0,static_cast<size_t>(slots)*8);
    mr=ibv_reg_mr(dev.pd,slots_mem,static_cast<size_t>(slots)*8,
                IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|
                IBV_ACCESS_REMOTE_ATOMIC|IBV_ACCESS_REMOTE_READ);
    if(!mr)fail("ibv_reg_mr remote atomic/write");
  }
  ~Impl(){if(mr)ibv_dereg_mr(mr);std::free(slots_mem);}
};
VerbsReleaseServer::VerbsReleaseServer(const ReleaseEndpointOptions& o):impl_(new Impl(o)){}
VerbsReleaseServer::~VerbsReleaseServer()=default;
uint64_t VerbsReleaseServer::slot_value(size_t slot)const {
  if(slot>=impl_->slots)throw std::out_of_range("slot");
  return __atomic_load_n(static_cast<uint64_t*>(impl_->slots_mem)+slot,__ATOMIC_ACQUIRE);
}
void VerbsReleaseServer::listen_forever(uint16_t port){
  int fd=listen_tcp(port);
  std::cout<<"RDMA release server IB device active, slots="<<impl_->slots<<" TCP bootstrap="<<port<<std::endl;
  for(;;){int c=::accept(fd,nullptr,nullptr);if(c<0){if(errno==EINTR)continue;fail("accept");}
    auto* region=impl_.get();
    std::thread([region,c] {
      try{
        Qp qp(region->dev);
        const auto local=qp.info(reinterpret_cast<uintptr_t>(region->slots_mem),region->mr->rkey,region->slots);
        QpWire remote{};
        send_exact(c,&local,sizeof(local));recv_exact(c,&remote,sizeof(remote));qp.connect(remote);
        uint8_t ready=0x5a;send_exact(c,&ready,1);recv_exact(c,&ready,1);
        // Keep QP alive; RNIC services one-sided operations without CPU.
        while(::recv(c,&ready,1,0)>0){}
      }catch(const std::exception& e){std::cerr<<"RDMA client session: "<<e.what()<<'\n';}
      ::close(c);
    }).detach();
  }
}
struct VerbsReleaseClient::Impl {
  Device dev;Qp qp;int fd{-1};QpWire remote{};
  uint64_t local_buf{};ibv_mr* mr{};bool pending{};ReleaseMode mode{};uint64_t desired{};
  explicit Impl(const std::string& host,uint16_t port,const ReleaseEndpointOptions& o)
    :dev(o),qp(dev){
    mr=ibv_reg_mr(dev.pd,&local_buf,sizeof(local_buf),IBV_ACCESS_LOCAL_WRITE);
    if(!mr)fail("local MR");
    fd=connect_tcp(host,port);
    recv_exact(fd,&remote,sizeof(remote));
    QpWire local=qp.info(0,0,0);send_exact(fd,&local,sizeof(local));qp.connect(remote);
    uint8_t ready=0;recv_exact(fd,&ready,1);if(ready!=0x5a)fail("handshake");
    send_exact(fd,&ready,1);
  }
  ~Impl(){if(fd>=0)::close(fd);if(mr)ibv_dereg_mr(mr);}
};
VerbsReleaseClient::VerbsReleaseClient(const std::string& h,uint16_t p,const ReleaseEndpointOptions& o)
  :impl_(new Impl(h,p,o)){}
VerbsReleaseClient::~VerbsReleaseClient()=default;
uint32_t VerbsReleaseClient::slots()const{return impl_->remote.slots;}
void VerbsReleaseClient::post(size_t slot,uint64_t grant,ReleaseMode mode){
  if(impl_->pending)throw std::logic_error("must complete previous WR before posting next");
  if(slot>=impl_->remote.slots || !grant)throw std::invalid_argument("bad slot/grant");
  impl_->pending=true;impl_->mode=mode;impl_->desired=grant;impl_->local_buf=grant;
  ibv_sge sg{};sg.addr=reinterpret_cast<uintptr_t>(&impl_->local_buf);sg.length=8;sg.lkey=impl_->mr->lkey;
  ibv_send_wr wr{};wr.wr_id=grant;wr.sg_list=&sg;wr.num_sge=1;wr.send_flags=IBV_SEND_SIGNALED;
  if(mode==ReleaseMode::AtomicCas){
    wr.opcode=IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.wr.atomic.remote_addr=impl_->remote.region_addr+slot*8;
    wr.wr.atomic.rkey=impl_->remote.rkey;
    wr.wr.atomic.compare_add=0;wr.wr.atomic.swap=grant;
  }else{
    wr.opcode=IBV_WR_RDMA_WRITE;
    wr.wr.rdma.remote_addr=impl_->remote.region_addr+slot*8;
    wr.wr.rdma.rkey=impl_->remote.rkey;
  }
  ibv_send_wr* bad=nullptr;
  if(ibv_post_send(impl_->qp.qp,&wr,&bad)){
    impl_->pending=false;fail("ibv_post_send");
  }
}
bool VerbsReleaseClient::complete(){
  if(!impl_->pending)throw std::logic_error("no RDMA WR pending");
  impl_->qp.poll_one();impl_->pending=false;
  return impl_->mode==ReleaseMode::WriteHint || impl_->local_buf==0 || impl_->local_buf==impl_->desired;
}
} // namespace mucql

namespace mucql {
uint64_t VerbsReleaseClient::compare_exchange(size_t slot,uint64_t expected,uint64_t desired){
  if(impl_->pending)throw std::logic_error("pending WR: drain before CAS");
  if(slot>=impl_->remote.slots)throw std::out_of_range("CAS slot");
  impl_->local_buf=0;
  ibv_sge sg{};sg.addr=reinterpret_cast<uintptr_t>(&impl_->local_buf);
  sg.length=8;sg.lkey=impl_->mr->lkey;
  ibv_send_wr wr{};wr.wr_id=slot;wr.sg_list=&sg;wr.num_sge=1;
  wr.send_flags=IBV_SEND_SIGNALED;wr.opcode=IBV_WR_ATOMIC_CMP_AND_SWP;
  wr.wr.atomic.remote_addr=impl_->remote.region_addr+slot*8;
  wr.wr.atomic.rkey=impl_->remote.rkey;
  wr.wr.atomic.compare_add=expected;wr.wr.atomic.swap=desired;
  ibv_send_wr* bad=nullptr;
  if(ibv_post_send(impl_->qp.qp,&wr,&bad))fail("CAS post");
  impl_->qp.poll_one();return impl_->local_buf;
}
uint64_t VerbsReleaseClient::read_slot(size_t slot){
  if(impl_->pending)throw std::logic_error("pending WR: drain before READ");
  if(slot>=impl_->remote.slots)throw std::out_of_range("READ slot");
  impl_->local_buf=0;
  ibv_sge sg{};sg.addr=reinterpret_cast<uintptr_t>(&impl_->local_buf);
  sg.length=8;sg.lkey=impl_->mr->lkey;
  ibv_send_wr wr{};wr.wr_id=slot;wr.sg_list=&sg;wr.num_sge=1;
  wr.send_flags=IBV_SEND_SIGNALED;wr.opcode=IBV_WR_RDMA_READ;
  wr.wr.rdma.remote_addr=impl_->remote.region_addr+slot*8;
  wr.wr.rdma.rkey=impl_->remote.rkey;
  ibv_send_wr* bad=nullptr;
  if(ibv_post_send(impl_->qp.qp,&wr,&bad))fail("READ post");
  impl_->qp.poll_one();return impl_->local_buf;
}
void VerbsReleaseClient::write_slot(size_t slot,uint64_t value){
  if(impl_->pending)throw std::logic_error("pending WR: drain before WRITE");
  if(slot>=impl_->remote.slots)throw std::out_of_range("WRITE slot");
  impl_->local_buf=value;
  ibv_sge sg{};sg.addr=reinterpret_cast<uintptr_t>(&impl_->local_buf);
  sg.length=8;sg.lkey=impl_->mr->lkey;
  ibv_send_wr wr{};wr.wr_id=slot;wr.sg_list=&sg;wr.num_sge=1;
  wr.send_flags=IBV_SEND_SIGNALED;wr.opcode=IBV_WR_RDMA_WRITE;
  wr.wr.rdma.remote_addr=impl_->remote.region_addr+slot*8;
  wr.wr.rdma.rkey=impl_->remote.rkey;
  ibv_send_wr* bad=nullptr;
  if(ibv_post_send(impl_->qp.qp,&wr,&bad))fail("WRITE post");
  impl_->qp.poll_one();
}
} // namespace mucql
