#include "mucql/live_control.hpp"
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace mucql {
namespace {
constexpr uint64_t MAGIC=0x4D5543514C435432ULL; // MUCQLCT2
constexpr uint32_t VERSION=1, RECV_DEPTH=32;
struct Wire {
  uint64_t magic;
  uint32_t version, message_size, qpn, psn;
  uint16_t lid;
  uint8_t port, reserved;
  uint32_t recv_depth;
};
static_assert(sizeof(Wire)==32,"unexpected control QP wire layout");
[[noreturn]] void fail(const std::string& what){
  throw std::runtime_error("live control RDMA: "+what);
}
void send_all(int fd,const void* src,size_t n){
  const auto* p=static_cast<const uint8_t*>(src);
  while(n){
    ssize_t k=::send(fd,p,n,MSG_NOSIGNAL);
    if(k<0&&errno==EINTR)continue;
    if(k<=0)fail("TCP bootstrap send failed");
    p+=k;n-=size_t(k);
  }
}
void recv_all(int fd,void* dst,size_t n){
  auto* p=static_cast<uint8_t*>(dst);
  while(n){
    ssize_t k=::recv(fd,p,n,0);
    if(k<0&&errno==EINTR)continue;
    if(k<=0)fail("TCP bootstrap recv failed");
    p+=k;n-=size_t(k);
  }
}
uint32_t psn(){
  return uint32_t(std::chrono::steady_clock::now().time_since_epoch().count())&0xffffff;
}
// Optional control receive backoff: by default retain the original low-latency
// CQ poll. A bounded yield+sleep policy can release CPU on oversubscribed CNs.
uint32_t idle_recv_us(){
  const char* s=std::getenv("MUCQL_RX_IDLE_US");
  if(!s)return 0;
  if(!*s)fail("MUCQL_RX_IDLE_US must be 0..100");
  errno=0;char* end=nullptr;
  unsigned long n=std::strtoul(s,&end,10);
  if(errno||*end||n>100)fail("MUCQL_RX_IDLE_US must be 0..100");
  return uint32_t(n);
}
}

struct LiveControlChannel::Impl {
  int fd;
  bool rdma;
  size_t size;
  uint32_t rx_idle_us{};
  std::atomic<bool> stopping{false};
  ibv_context* ctx{};
  ibv_pd* pd{};
  ibv_cq* send_cq{};
  ibv_cq* recv_cq{};
  ibv_qp* qp{};
  ibv_mr* send_mr{};
  ibv_mr* recv_mr{};
  uint32_t local_psn{};
  ibv_port_attr attr{};
  uint8_t port;
  std::vector<uint8_t> send_buf,recv_buf;
  explicit Impl(int f,const std::string& device,uint8_t p,bool use_rdma,size_t message_size)
    :fd(f),rdma(use_rdma),size(message_size),rx_idle_us(idle_recv_us()),port(p){
    if(size==0||size>4096||port<1||port>2)fail("invalid control-frame size or port");
    if(!rdma)return;
    // Destructor is safe on every partial-construction failure.
    try{
      int count=0;
      ibv_device** list=ibv_get_device_list(&count);
      if(!list||count<=0)fail("no RNIC");
      ibv_device* selected=nullptr;
      for(int i=0;i<count;i++){
        if(device!=ibv_get_device_name(list[i]))continue;
        auto* test=ibv_open_device(list[i]);
        if(!test)continue;
        ibv_port_attr a{};
        bool active=ibv_query_port(test,port,&a)==0 &&
                    a.state==IBV_PORT_ACTIVE &&
                    a.link_layer==IBV_LINK_LAYER_INFINIBAND && a.lid!=0;
        ibv_close_device(test);
        if(active){selected=list[i];break;}
      }
      if(selected)ctx=ibv_open_device(selected);
      ibv_free_device_list(list);
      if(!ctx)fail("active InfiniBand port not found: "+device);
      if(ibv_query_port(ctx,port,&attr))fail("ibv_query_port");
      pd=ibv_alloc_pd(ctx);
      if(!pd)fail("ibv_alloc_pd");
      send_cq=ibv_create_cq(ctx,64,nullptr,nullptr,0);
      recv_cq=ibv_create_cq(ctx,64,nullptr,nullptr,0);
      if(!send_cq||!recv_cq)fail("ibv_create_cq");
      ibv_qp_init_attr init{};
      init.send_cq=send_cq;init.recv_cq=recv_cq;init.qp_type=IBV_QPT_RC;
      init.cap.max_send_wr=16;init.cap.max_recv_wr=RECV_DEPTH;
      init.cap.max_send_sge=1;init.cap.max_recv_sge=1;
      init.cap.max_inline_data=0; // mlx4 safe
      qp=ibv_create_qp(pd,&init);
      if(!qp)fail("ibv_create_qp");
      ibv_qp_attr q{};
      q.qp_state=IBV_QPS_INIT;q.pkey_index=0;q.port_num=port;
      q.qp_access_flags=0; // only two-sided SEND/RECV
      if(ibv_modify_qp(qp,&q,IBV_QP_STATE|IBV_QP_PKEY_INDEX|
                             IBV_QP_PORT|IBV_QP_ACCESS_FLAGS))fail("QP INIT");
      send_buf.resize(size);
      recv_buf.resize(RECV_DEPTH*size);
      send_mr=ibv_reg_mr(pd,send_buf.data(),send_buf.size(),IBV_ACCESS_LOCAL_WRITE);
      recv_mr=ibv_reg_mr(pd,recv_buf.data(),recv_buf.size(),IBV_ACCESS_LOCAL_WRITE);
      if(!send_mr||!recv_mr)fail("register control buffers");
      // The RQ is fully primed BEFORE either peer can send its first frame.
      for(uint32_t i=0;i<RECV_DEPTH;i++)post_recv(i);
      local_psn=psn();
      Wire local{MAGIC,VERSION,uint32_t(size),qp->qp_num,local_psn,attr.lid,port,0,RECV_DEPTH};
      Wire remote{};
      send_all(fd,&local,sizeof(local));
      recv_all(fd,&remote,sizeof(remote));
      if(remote.magic!=MAGIC||remote.version!=VERSION||
         remote.message_size!=size||remote.recv_depth!=RECV_DEPTH||remote.lid==0)
        fail("QP bootstrap version/frame-size mismatch");
      q={};q.qp_state=IBV_QPS_RTR;q.path_mtu=IBV_MTU_1024;
      q.dest_qp_num=remote.qpn;q.rq_psn=remote.psn;
      q.max_dest_rd_atomic=0;q.min_rnr_timer=12;
      q.ah_attr.is_global=0;q.ah_attr.dlid=remote.lid;
      q.ah_attr.sl=0;q.ah_attr.src_path_bits=0;q.ah_attr.port_num=port;
      if(ibv_modify_qp(qp,&q,IBV_QP_STATE|IBV_QP_AV|IBV_QP_PATH_MTU|
                             IBV_QP_DEST_QPN|IBV_QP_RQ_PSN|
                             IBV_QP_MAX_DEST_RD_ATOMIC|IBV_QP_MIN_RNR_TIMER))
        fail("QP RTR");
      q={};q.qp_state=IBV_QPS_RTS;q.sq_psn=local_psn;
      q.timeout=14;q.retry_cnt=7;q.rnr_retry=7;q.max_rd_atomic=0;
      if(ibv_modify_qp(qp,&q,IBV_QP_STATE|IBV_QP_SQ_PSN|IBV_QP_TIMEOUT|
                             IBV_QP_RETRY_CNT|IBV_QP_RNR_RETRY|IBV_QP_MAX_QP_RD_ATOMIC))
        fail("QP RTS");
      // A second bootstrap barrier makes all QPs RTS before first SEND.
      const char ready='R';char peer_ready=0;
      send_all(fd,&ready,1);recv_all(fd,&peer_ready,1);
      if(peer_ready!=ready)fail("QP ready barrier mismatch");
    }catch(...){cleanup();throw;}
  }
  void cleanup()noexcept {
    if(qp){ibv_destroy_qp(qp);qp=nullptr;}
    if(send_mr){ibv_dereg_mr(send_mr);send_mr=nullptr;}
    if(recv_mr){ibv_dereg_mr(recv_mr);recv_mr=nullptr;}
    if(send_cq){ibv_destroy_cq(send_cq);send_cq=nullptr;}
    if(recv_cq){ibv_destroy_cq(recv_cq);recv_cq=nullptr;}
    if(pd){ibv_dealloc_pd(pd);pd=nullptr;}
    if(ctx){ibv_close_device(ctx);ctx=nullptr;}
  }
  ~Impl(){cleanup();}
  void post_recv(uint32_t i){
    ibv_sge sg{};
    sg.addr=reinterpret_cast<uintptr_t>(recv_buf.data()+i*size);
    sg.length=uint32_t(size);sg.lkey=recv_mr->lkey;
    ibv_recv_wr wr{};wr.wr_id=i;wr.sg_list=&sg;wr.num_sge=1;
    ibv_recv_wr* bad=nullptr;
    if(ibv_post_recv(qp,&wr,&bad))fail("ibv_post_recv");
  }
  ibv_wc poll(ibv_cq* cq,const char* kind){
    auto start=std::chrono::steady_clock::now();
    uint32_t misses=0;
    for(;;){
      if(stopping.load(std::memory_order_relaxed))fail(std::string(kind)+" cancelled");
      ibv_wc wc{};
      int n=ibv_poll_cq(cq,1,&wc);
      if(n<0)fail(std::string(kind)+" poll failed");
      if(n==1){
        if(wc.status!=IBV_WC_SUCCESS)fail(std::string(kind)+" WC: "+ibv_wc_status_str(wc.status));
        return wc;
      }
      // Protect against hung QPs, but allow receiver to idle between requests.
      if(cq==send_cq&&std::chrono::steady_clock::now()-start>std::chrono::seconds(3))
        fail("SEND CQ timeout");
      if(cq==recv_cq && rx_idle_us>0 && (++misses%32)==0)
        std::this_thread::sleep_for(std::chrono::microseconds(rx_idle_us));
      else
        std::this_thread::yield();
    }
  }
};

LiveControlChannel::LiveControlChannel(int fd,const std::string& dev,uint8_t port,bool rdma,size_t n)
 :impl_(new Impl(fd,dev,port,rdma,n)){}
LiveControlChannel::~LiveControlChannel()=default;
bool LiveControlChannel::is_rdma()const noexcept{return impl_->rdma;}
void LiveControlChannel::stop()noexcept{impl_->stopping.store(true);}
void LiveControlChannel::send(const void* msg,size_t n){
  if(n!=impl_->size)fail("SEND frame-size mismatch");
  if(impl_->stopping.load())fail("SEND after stop");
  if(!impl_->rdma){send_all(impl_->fd,msg,n);return;}
  std::memcpy(impl_->send_buf.data(),msg,n);
  ibv_sge sg{};sg.addr=reinterpret_cast<uintptr_t>(impl_->send_buf.data());
  sg.length=uint32_t(n);sg.lkey=impl_->send_mr->lkey;
  ibv_send_wr wr{};wr.wr_id=0x4d5347;wr.sg_list=&sg;wr.num_sge=1;
  wr.opcode=IBV_WR_SEND;wr.send_flags=IBV_SEND_SIGNALED;
  ibv_send_wr* bad=nullptr;
  if(ibv_post_send(impl_->qp,&wr,&bad))fail("ibv_post_send SEND");
  const auto wc=impl_->poll(impl_->send_cq,"SEND");
  if(wc.wr_id!=0x4d5347)fail("unexpected SEND completion");
}
void LiveControlChannel::recv(void* msg,size_t n){
  if(n!=impl_->size)fail("RECV frame-size mismatch");
  if(!impl_->rdma){recv_all(impl_->fd,msg,n);return;}
  const auto wc=impl_->poll(impl_->recv_cq,"RECV");
  if(wc.wr_id>=RECV_DEPTH||wc.byte_len!=n)fail("bad RECV completion ID/size");
  std::memcpy(msg,impl_->recv_buf.data()+wc.wr_id*n,n);
  impl_->post_recv(uint32_t(wc.wr_id));
}
} // namespace mucql
