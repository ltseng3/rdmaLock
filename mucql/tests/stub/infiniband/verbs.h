#pragma once
// MINIMAL TEST DOUBLE for the subset of libibverbs used by mucql_verbs.
// Not an RDMA implementation; never install in a production include path.
#include <stdint.h>
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
struct ibv_context {int ignored;};
struct ibv_pd {int ignored;};
struct ibv_device {char name[32];};
enum ibv_port_state {IBV_PORT_ACTIVE=4};
enum ibv_mtu {IBV_MTU_1024=3};
enum ibv_link_layer {IBV_LINK_LAYER_INFINIBAND=1};
struct ibv_port_attr {uint16_t lid;uint8_t state;uint8_t link_layer;};
struct ibv_mr {void* addr;size_t length;uint32_t lkey;uint32_t rkey;};
struct ibv_cq {int completions;};
enum ibv_qp_type {IBV_QPT_RC=2};
enum ibv_qp_state {IBV_QPS_INIT=1,IBV_QPS_RTR=2,IBV_QPS_RTS=3,IBV_QPS_ERR=4};
struct ibv_qp_cap {uint32_t max_send_wr,max_recv_wr,max_send_sge,max_recv_sge,max_inline_data;};
struct ibv_qp_init_attr {ibv_cq* send_cq;ibv_cq* recv_cq;ibv_qp_cap cap;ibv_qp_type qp_type;};
struct ibv_global_route {uint8_t ignored;};
struct ibv_ah_attr {uint8_t is_global,sl,src_path_bits,port_num;uint16_t dlid;};
struct ibv_qp_attr {ibv_qp_state qp_state;int pkey_index;int port_num;int qp_access_flags;
   ibv_mtu path_mtu;uint32_t dest_qp_num,rq_psn, max_dest_rd_atomic,min_rnr_timer;
   ibv_ah_attr ah_attr;uint32_t timeout,retry_cnt,rnr_retry,sq_psn,max_rd_atomic;};
struct ibv_qp {uint32_t qp_num; ibv_cq* cq;uint32_t remote_qpn;ibv_qp_state state;};
struct ibv_sge {uint64_t addr;uint32_t length,lkey;};
enum ibv_wr_opcode {IBV_WR_RDMA_WRITE=0,IBV_WR_ATOMIC_CMP_AND_SWP=1};
struct ibv_send_wr {uint64_t wr_id; ibv_send_wr* next;ibv_sge* sg_list;int num_sge;ibv_wr_opcode opcode;unsigned send_flags;
  union { struct {uint64_t remote_addr;uint32_t rkey;} rdma;
          struct {uint64_t remote_addr;uint64_t compare_add,swap;uint32_t rkey;} atomic;} wr;
};
struct ibv_wc {int status;};
#define IBV_WC_SUCCESS 0
#define IBV_ACCESS_LOCAL_WRITE 1
#define IBV_ACCESS_REMOTE_WRITE 2
#define IBV_ACCESS_REMOTE_ATOMIC 4
#define IBV_ACCESS_REMOTE_READ 8
#define IBV_SEND_SIGNALED 2
#define IBV_QP_STATE 1
#define IBV_QP_PKEY_INDEX 2
#define IBV_QP_PORT 4
#define IBV_QP_ACCESS_FLAGS 8
#define IBV_QP_AV 16
#define IBV_QP_PATH_MTU 32
#define IBV_QP_DEST_QPN 64
#define IBV_QP_RQ_PSN 128
#define IBV_QP_MAX_DEST_RD_ATOMIC 256
#define IBV_QP_MIN_RNR_TIMER 512
#define IBV_QP_TIMEOUT 1024
#define IBV_QP_RETRY_CNT 2048
#define IBV_QP_RNR_RETRY 4096
#define IBV_QP_SQ_PSN 8192
#define IBV_QP_MAX_QP_RD_ATOMIC 16384
ibv_device** ibv_get_device_list(int*);
void ibv_free_device_list(ibv_device**);
const char* ibv_get_device_name(ibv_device*);
ibv_context* ibv_open_device(ibv_device*);
int ibv_close_device(ibv_context*);
int ibv_query_port(ibv_context*,uint8_t,ibv_port_attr*);
ibv_pd* ibv_alloc_pd(ibv_context*);
int ibv_dealloc_pd(ibv_pd*);
ibv_cq* ibv_create_cq(ibv_context*,int,void*,void*,int);
int ibv_destroy_cq(ibv_cq*);
ibv_qp* ibv_create_qp(ibv_pd*,ibv_qp_init_attr*);
int ibv_destroy_qp(ibv_qp*);
int ibv_modify_qp(ibv_qp*,ibv_qp_attr*,int);
ibv_mr* ibv_reg_mr(ibv_pd*,void*,size_t,int);
int ibv_dereg_mr(ibv_mr*);
int ibv_post_send(ibv_qp*,ibv_send_wr*,ibv_send_wr**);
int ibv_poll_cq(ibv_cq*,int,ibv_wc*);
const char* ibv_wc_status_str(int);
#ifdef __cplusplus
}
#endif
