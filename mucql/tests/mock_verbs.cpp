#include <infiniband/verbs.h>
#include <atomic>
#include <deque>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <cerrno>
namespace {
std::mutex m;
ibv_device d{{'m','o','c','k','0','\0'}};
ibv_device* devlist[]={&d,nullptr};
ibv_context context{};ibv_pd pd{};
uint32_t next_qp=1,next_key=100;
std::unordered_map<uint32_t,ibv_mr*> regions;
std::unordered_map<uint32_t,ibv_qp*> qps;
struct Receive {uint64_t wr_id;ibv_sge sg;};
std::unordered_map<ibv_qp*,std::deque<Receive>> posted;
std::unordered_map<ibv_cq*,std::deque<ibv_wc>> completed;
// Preserve the SGE address, not its payload. Async Phase-2 DMA happens on
// progress, so overwriting a still-owned registered buffer corrupts tests.
struct DeferredWr {ibv_send_wr wr;ibv_sge sge;};
struct DeferredQueue {std::deque<DeferredWr> wrs;size_t polls{};};
std::unordered_map<ibv_qp*,DeferredQueue> deferred;
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
int ibv_destroy_cq(ibv_cq*c){std::lock_guard<std::mutex> l(m);completed.erase(c);delete c;return 0;}
ibv_qp* ibv_create_qp(ibv_pd*,ibv_qp_init_attr* attr){std::lock_guard<std::mutex> l(m);auto* qp=new ibv_qp{next_qp++,attr->send_cq,0,IBV_QPS_INIT,attr->recv_cq};qps[qp->qp_num]=qp;return qp;}
int ibv_destroy_qp(ibv_qp*q){std::lock_guard<std::mutex> l(m);qps.erase(q->qp_num);posted.erase(q);deferred.erase(q);delete q;return 0;}
int ibv_modify_qp(ibv_qp*q,ibv_qp_attr*a,int flags){std::lock_guard<std::mutex> l(m);
  if(flags&IBV_QP_STATE)q->state=a->qp_state;
  if(flags&IBV_QP_DEST_QPN)q->remote_qpn=a->dest_qp_num;
  return 0;}
ibv_mr* ibv_reg_mr(ibv_pd*,void*addr,size_t len,int){
  // Fault-injection is specific to the accepted-log MR, not the small
  // per-QP send buffers. Simulates expensive or rejected registration.
  if(len>=8192){
    if(const char* ms=std::getenv("MUCQL_MOCK_ACCEPTED_MR_DELAY_MS"))
      std::this_thread::sleep_for(std::chrono::milliseconds(std::stoul(ms)));
    if(std::getenv("MUCQL_MOCK_REJECT_ACCEPTED_MR")){
      errno=ENOMEM;
      return nullptr;
    }
  }
  std::lock_guard<std::mutex> l(m);
  auto* mr=new ibv_mr{addr,len,++next_key,++next_key};
  regions[mr->rkey]=mr;
  return mr;
}
int ibv_dereg_mr(ibv_mr* mr){std::lock_guard<std::mutex> l(m);regions.erase(mr->rkey);delete mr;return 0;}
int ibv_post_recv(ibv_qp* q,ibv_recv_wr*wr,ibv_recv_wr**bad){
  std::lock_guard<std::mutex> l(m);*bad=nullptr;
  if(q->state==IBV_QPS_ERR)return -1;
  for(auto* w=wr;w;w=w->next){
    if(w->num_sge!=1||!w->sg_list)return -1;
    posted[q].push_back({w->wr_id,*w->sg_list});
  }
  return 0;
}
int mock_execute_send(ibv_qp* q,ibv_send_wr*wr,ibv_send_wr**bad){
  *bad=nullptr;
  auto peer=qps.find(q->remote_qpn);
  if(q->state!=IBV_QPS_RTS||peer==qps.end()||peer->second->state!=IBV_QPS_RTS)return -1;
  for(auto* w=wr;w;w=w->next){
    if(w->opcode==IBV_WR_SEND){
      auto& queue=posted[peer->second];
      if(queue.empty())return -1; // test-model RNR
      auto target=queue.front();queue.pop_front();
      if(w->num_sge!=1||!w->sg_list||target.sg.length<w->sg_list->length)return -1;
      std::memcpy(reinterpret_cast<void*>(target.sg.addr),
                  reinterpret_cast<const void*>(w->sg_list->addr),w->sg_list->length);
      completed[peer->second->recv_cq].push_back({IBV_WC_SUCCESS,target.wr_id,w->sg_list->length});
      if(w->send_flags&IBV_SEND_SIGNALED)
        completed[q->cq].push_back({IBV_WC_SUCCESS,w->wr_id,w->sg_list->length});
      continue;
    }
    uint64_t addr= w->opcode!=IBV_WR_ATOMIC_CMP_AND_SWP ? w->wr.rdma.remote_addr : w->wr.atomic.remote_addr;
    uint32_t rkey= w->opcode!=IBV_WR_ATOMIC_CMP_AND_SWP ? w->wr.rdma.rkey : w->wr.atomic.rkey;
    auto it=regions.find(rkey);if(it==regions.end())return -1;
    auto* mr=it->second;auto low=reinterpret_cast<uintptr_t>(mr->addr);
    if(addr<low || addr+w->sg_list->length>low+mr->length)return -1;
    auto* target=reinterpret_cast<uint64_t*>(addr);
    if(w->opcode==IBV_WR_RDMA_WRITE){
      if(w->sg_list->length==8){uint64_t v;
        std::memcpy(&v,reinterpret_cast<const void*>(w->sg_list->addr),8);
        __atomic_store_n(target,v,__ATOMIC_RELEASE);
      }else std::memcpy(target,reinterpret_cast<const void*>(w->sg_list->addr),w->sg_list->length);
    }
    else if(w->opcode==IBV_WR_RDMA_READ){
      if(w->sg_list->length==8){auto v=__atomic_load_n(target,__ATOMIC_ACQUIRE);
        std::memcpy(reinterpret_cast<void*>(w->sg_list->addr),&v,8);
      }else std::memcpy(reinterpret_cast<void*>(w->sg_list->addr),target,w->sg_list->length);
    }
    else{
      auto old=__atomic_load_n(target,__ATOMIC_ACQUIRE);
      std::memcpy(reinterpret_cast<void*>(w->sg_list->addr),&old,8);
      if(old==w->wr.atomic.compare_add)__atomic_store_n(target,w->wr.atomic.swap,__ATOMIC_RELEASE);
    }
    if(w->send_flags&IBV_SEND_SIGNALED)completed[q->cq].push_back({IBV_WC_SUCCESS,w->wr_id,w->sg_list->length});
  }
  return 0;
}
int ibv_post_send(ibv_qp* q,ibv_send_wr*wr,ibv_send_wr**bad){
  std::lock_guard<std::mutex> l(m);*bad=nullptr;
  const bool phase2=wr->opcode==IBV_WR_RDMA_WRITE&&wr->next&&
    wr->next->opcode==IBV_WR_ATOMIC_CMP_AND_SWP&&wr->next->wr_id;
  if(!phase2)return mock_execute_send(q,wr,bad);
  auto peer=qps.find(q->remote_qpn);
  if(q->state!=IBV_QPS_RTS||peer==qps.end()||peer->second->state!=IBV_QPS_RTS)return -1;
  auto& queue=deferred[q];
  if(queue.wrs.empty()){
    const char* polls=std::getenv("MUCQL_MOCK_PHASE2_DELAY_POLLS");
    queue.polls=polls?std::stoul(polls):1;
  }
  for(auto* w=wr;w;w=w->next)queue.wrs.push_back({*w,*w->sg_list});
  return 0;
}
int ibv_poll_cq(ibv_cq*c,int,ibv_wc*wc){
  std::lock_guard<std::mutex> l(m);
  for(auto& entry:deferred){
    if(entry.first->cq!=c||entry.second.wrs.empty())continue;
    auto& queue=entry.second;
    if(queue.polls){--queue.polls;continue;}
    while(!queue.wrs.empty()){
      auto& next=queue.wrs.front();next.wr.next=nullptr;next.wr.sg_list=&next.sge;
      ibv_send_wr* bad=nullptr;
      if(mock_execute_send(entry.first,&next.wr,&bad))
        completed[c].push_back({1,next.wr.wr_id,0});
      queue.wrs.pop_front();
    }
  }
  auto& q=completed[c];if(q.empty())return 0;
  *wc=q.front();q.pop_front();return 1;
}
const char* ibv_wc_status_str(int){return "MOCK";}
}
