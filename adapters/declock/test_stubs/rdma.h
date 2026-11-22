#pragma once
#include <cstdint>
#include <cstddef>
#define MAX_CLIENT_NUM 128
#define TID_CORE(tid) ((tid)/8)
char* rdma_sendbuf(uint32_t,uint32_t,bool*);
char* rdma_rc_buf(uint32_t);
int rdma_send(uint16_t,uint16_t,uint16_t,char*,size_t,bool);
int rdma_one_sided_op(char,uint32_t,uint16_t,char*,uint64_t,uint32_t,bool,uint64_t,uint64_t);
#define rdma_write(tid,dest,laddr,raddr,size,wait) rdma_one_sided_op(0,tid,dest,laddr,raddr,size,wait,0,0)
void rdma_wait_one(uint16_t,uint32_t);
