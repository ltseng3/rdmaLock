#include <infiniband/verbs.h>
#include <atomic>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
namespace {
std::mutex m;
ibv_device d{{'m','o','c','k','0','\0'}};
ibv_device* devlist[]={&d,nullptr};
ibv_context context{};ibv_pd pd{};
uint32_t next_qp=1,next_key=100;
std::unordered_map<uint32_t,ibv_mr*> regions;
std::unordered_map<uint32_t,ibv_qp*> qps;
}
extern "C" {
ibv_device** ibv_get_device_list(int*n){*n=1;return devlist;}
void ibv_free_device_list(ibv_device**){}
const char* ibv_get_device_name(ibv_device*v){return v->name;}
ibv_context* ibv_open_device(ibv_device*){return &context;}
int ibv_close_device(ibv_context*){return 0;}
int ibv_query_port(ibv_context*,uint8_t,ibv_port_attr*o){o->lid=1;o->state=IBV_PORT_ACTIVE;o->link_layer=IBV_LINK_LAYER_INFINIBAND;return 0;}
ibv_pd* ibv_alloc_pd(ibv_context*){return &pd;}
int ibv_dealloc_pd(ibv_pd*){return 0;}
ibv_cq* ibv_create_cq(ibv_context*,int,void*,void*,int){return new ibv_cq{0};}
int ibv_destroy_cq(ibv_cq*c){delete c;return 0;}
ibv_qp* ibv_create_qp(ibv_pd*,ibv_qp_init_attr* attr){std::lock_guard<std::mutex> l(m);auto* qp=new ibv_qp{next_qp++,attr->send_cq,0,IBV_QPS_INIT};qps[qp->qp_num]=qp;return qp;}
int ibv_destroy_qp(ibv_qp*q){std::lock_guard<std::mutex> l(m);qps.erase(q->qp_num);delete q;return 0;}
int ibv_modify_qp(ibv_qp*q,ibv_qp_attr*a,int flags){std::lock_guard<std::mutex> l(m);
  if(flags&IBV_QP_STATE)q->state=a->qp_state;
  if(flags&IBV_QP_DEST_QPN)q->remote_qpn=a->dest_qp_num;
  return 0;}
ibv_mr* ibv_reg_mr(ibv_pd*,void*addr,size_t len,int){std::lock_guard<std::mutex> l(m);auto* mr=new ibv_mr{addr,len,++next_key,++next_key};regions[mr->rkey]=mr;return mr;}
int ibv_dereg_mr(ibv_mr* mr){std::lock_guard<std::mutex> l(m);regions.erase(mr->rkey);delete mr;return 0;}
int ibv_post_send(ibv_qp* q,ibv_send_wr*wr,ibv_send_wr**bad){
  std::lock_guard<std::mutex> l(m);*bad=nullptr;
  auto peer=qps.find(q->remote_qpn);
  if(q->state!=IBV_QPS_RTS||peer==qps.end()||peer->second->state!=IBV_QPS_RTS)return -1;
  for(auto* w=wr;w;w=w->next){
    uint64_t addr= w->opcode==IBV_WR_RDMA_WRITE ? w->wr.rdma.remote_addr : w->wr.atomic.remote_addr;
    uint32_t rkey= w->opcode==IBV_WR_RDMA_WRITE ? w->wr.rdma.rkey : w->wr.atomic.rkey;
    auto it=regions.find(rkey);if(it==regions.end())return -1;
    auto* mr=it->second;auto low=reinterpret_cast<uintptr_t>(mr->addr);
    if(addr<low || addr+w->sg_list->length>low+mr->length)return -1;
    auto* target=reinterpret_cast<uint64_t*>(addr);
    if(w->opcode==IBV_WR_RDMA_WRITE)std::memcpy(target,reinterpret_cast<const void*>(w->sg_list->addr),w->sg_list->length);
    else{
      auto old=*target;
      std::memcpy(reinterpret_cast<void*>(w->sg_list->addr),&old,8);
      if(old==w->wr.atomic.compare_add)*target=w->wr.atomic.swap;
    }
    if(w->send_flags&IBV_SEND_SIGNALED)q->cq->completions++;
  }
  return 0;
}
int ibv_poll_cq(ibv_cq*c,int,ibv_wc*wc){if(c->completions>0){c->completions--;wc->status=IBV_WC_SUCCESS;return 1;}return 0;}
const char* ibv_wc_status_str(int){return "MOCK";}
}
