#include "../rdma_handoff.hpp"
#include <cassert>
#include <cstring>
#include <vector>
using namespace mucql::declock;
static char sendbuf[512];static char rcbuf[MAX_CLIENT_NUM][4096];
static int nsends=0, nwrites=0, ndrains=0;
char* rdma_sendbuf(uint32_t,uint32_t,bool* sync){*sync=false;return sendbuf;}
char* rdma_rc_buf(uint32_t t){return rcbuf[t];}
int rdma_send(uint16_t,uint16_t,uint16_t,char* b,size_t n,bool){assert(n==48);HandoffWire h;memcpy(&h,b,n);assert(h.magic==kHandoffMagic);nsends++;return 0;}
int rdma_one_sided_op(char,uint32_t,uint16_t,char*,uint64_t,uint32_t n,bool w,uint64_t,uint64_t){assert(n==8&&!w);nwrites++;return 0;}
void rdma_wait_one(uint16_t,uint32_t){ndrains++;}
int main(){
 HandoffTransport net;HandoffWire h{kHandoffMagic,0,10,2,1,2,1000};
 net.send_handoff(0,1,0,h);
 net.post_release(0,0,release_offset(10,1),1);
 net.drain_release(0);net.drain_release(0);
 assert(nsends==1&&nwrites==1&&ndrains==1);
}
