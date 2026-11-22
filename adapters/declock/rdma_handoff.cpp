#include "rdma_handoff.hpp"
#include <cstring>
namespace mucql { namespace declock {
void HandoffTransport::send_handoff(uint32_t tid,uint16_t mid,uint16_t dst_tid,
                                    const HandoffWire& h) {
  if(h.magic!=kHandoffMagic) throw std::invalid_argument("bad handoff payload");
  bool sync=false;
  char* buf=rdma_sendbuf(tid,sizeof(h),&sync);
  std::memcpy(buf,&h,sizeof(h));
  // Existing DecLock UD send path handles unsignaled sends and send-buffer wrap.
  rdma_send(TID_CORE(tid),mid,TID_CORE(dst_tid),buf,sizeof(h),sync);
}
void HandoffTransport::post_release(uint32_t tid,uint16_t mn,uint64_t addr,
                                    uint64_t grant) {
  if(tid>=MAX_CLIENT_NUM || pending_[tid].valid)
    throw std::logic_error("undrained RDMA release");
  auto* p=reinterpret_cast<uint64_t*>(rdma_rc_buf(tid));
  *p=grant;
  pending_[tid]={true,mn};
  // Signaled but not waited: overlaps concurrent direct UD handoff. The
  // buffer must not be reused until drain_release().
  rdma_write(tid,mn,reinterpret_cast<char*>(p),addr,sizeof(*p),false);
}
void HandoffTransport::drain_release(uint32_t tid) {
  if(tid>=MAX_CLIENT_NUM) throw std::out_of_range("bad tid");
  if(!pending_[tid].valid)return;
  rdma_wait_one(pending_[tid].mn,tid);
  pending_[tid].valid=false;
}
}} // namespace
