#include "mucql/tcp_transport.hpp"
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <vector>

namespace mucql {
namespace {
constexpr uint64_t kMagic=0x4d5543514c303031ULL;
constexpr uint64_t kPrepare=1,kAccept=2,kReply=3;
struct Request {
  uint64_t magic,op,ballot;
  uint64_t index,type,lock,client,request,first,last,deadline;
};
static_assert(sizeof(Request)==88,"wire request size");
struct Reply { uint64_t magic,op,ok,count; };
struct AcceptedWire { uint64_t ballot,index,type,lock,client,request,first,last,deadline; };
static_assert(sizeof(AcceptedWire)==72,"wire accepted size");
Request request(uint64_t op,uint64_t ballot,const Record* r=nullptr) {
  Request w{};w.magic=kMagic;w.op=op;w.ballot=ballot;
  if(r){w.index=r->index;w.type=static_cast<uint64_t>(r->type);
    w.lock=r->lock;w.client=r->client;w.request=r->request;
    w.first=r->first;w.last=r->last;w.deadline=r->deadline;}
  return w;
}
Record from_req(const Request& w) {
  Record r{};r.index=w.index;r.type=static_cast<Record::Type>(w.type);
  r.lock=w.lock;r.client=w.client;r.request=w.request;
  r.first=w.first;r.last=w.last;r.deadline=w.deadline;
  return r;
}
AcceptedWire to_wire(const Accepted& a) {
  AcceptedWire w{};w.ballot=a.ballot;w.index=a.value.index;
  w.type=static_cast<uint64_t>(a.value.type);w.lock=a.value.lock;
  w.client=a.value.client;w.request=a.value.request;
  w.first=a.value.first;w.last=a.value.last;w.deadline=a.value.deadline;
  return w;
}
Accepted from_wire(const AcceptedWire& w) {
  Record r{};r.index=w.index;r.type=static_cast<Record::Type>(w.type);
  r.lock=w.lock;r.client=w.client;r.request=w.request;
  r.first=w.first;r.last=w.last;r.deadline=w.deadline;
  return Accepted{w.ballot,r};
}
bool send_all(int fd,const void* d,size_t n) {
  auto p=reinterpret_cast<const uint8_t*>(d);
  while(n){ auto r=send(fd,p,n,MSG_NOSIGNAL);
    if(r<0&&errno==EINTR)continue;
    if(r<=0)return false;
    p+=r;n-=static_cast<size_t>(r);
  }
  return true;
}
bool recv_all(int fd,void* d,size_t n){
  auto p=reinterpret_cast<uint8_t*>(d);
  while(n){auto r=recv(fd,p,n,0);
    if(r<0&&errno==EINTR)continue;
    if(r<=0)return false;
    p+=r;n-=static_cast<size_t>(r);
  }
  return true;
}
void serve_client(int fd,Acceptor& acceptor) {
  // x86 little-endian same-host-class transport; add explicit endian
  // conversion and frame limits before deploying on mixed architectures.
  while(true){
    Request req{};
    if(!recv_all(fd,&req,sizeof(req)))break;
    if(req.magic!=kMagic)break;
    if(req.op==kPrepare){
      Promise p=acceptor.prepare(req.ballot);
      Reply rep{kMagic,kReply,p.granted?1ULL:0ULL,p.granted?p.accepted.size():0ULL};
      if(!send_all(fd,&rep,sizeof(rep)))break;
      bool ok=true;
      if(p.granted)for(const auto& kv:p.accepted){
        const auto w=to_wire(kv.second);
        if(!send_all(fd,&w,sizeof(w))){ok=false;break;}
      }
      if(!ok)break;
    }else if(req.op==kAccept){
      bool accepted=acceptor.accept(req.ballot,from_req(req));
      Reply rep{kMagic,kReply,accepted?1ULL:0ULL,0};
      if(!send_all(fd,&rep,sizeof(rep)))break;
    }else break;
  }
  ::close(fd);
}
}
TcpAcceptor::TcpAcceptor(std::string host,uint16_t port)
  :host_(std::move(host)),port_(port){}
TcpAcceptor::~TcpAcceptor(){std::lock_guard<std::mutex> l(mu_);close_locked();}
void TcpAcceptor::close_locked(){if(fd_>=0){::close(fd_);fd_=-1;}}
bool TcpAcceptor::connect_locked(){
  if(fd_>=0)return true;
  struct addrinfo hints{};hints.ai_family=AF_UNSPEC;hints.ai_socktype=SOCK_STREAM;
  struct addrinfo* result=nullptr;
  auto portstr=std::to_string(port_);
  if(getaddrinfo(host_.c_str(),portstr.c_str(),&hints,&result)!=0)return false;
  for(auto* ai=result;ai!=nullptr;ai=ai->ai_next){
    int s=socket(ai->ai_family,ai->ai_socktype,ai->ai_protocol);
    if(s<0)continue;
    struct timeval to{};to.tv_sec=1;
    setsockopt(s,SOL_SOCKET,SO_RCVTIMEO,&to,sizeof(to));
    setsockopt(s,SOL_SOCKET,SO_SNDTIMEO,&to,sizeof(to));
    if(::connect(s,ai->ai_addr,ai->ai_addrlen)==0){fd_=s;break;}
    ::close(s);
  }
  freeaddrinfo(result);
  return fd_>=0;
}
Promise TcpAcceptor::prepare(uint64_t ballot){
  std::lock_guard<std::mutex> l(mu_);
  if(!connect_locked())return {};
  const auto req=request(kPrepare,ballot);
  Reply rep{};
  if(!send_all(fd_,&req,sizeof(req))||!recv_all(fd_,&rep,sizeof(rep))||
     rep.magic!=kMagic||rep.op!=kReply||rep.count>1'000'000){
    close_locked();return {};
  }
  Promise p;p.granted=rep.ok!=0;
  if(!p.granted && rep.count){close_locked();return {};}
  for(uint64_t i=0;i<rep.count;i++){
    AcceptedWire w{};
    if(!recv_all(fd_,&w,sizeof(w))){close_locked();return {};}
    if(!w.index || w.type>1){close_locked();return {};}
    p.accepted[w.index]=from_wire(w);
  }
  return p;
}
bool TcpAcceptor::accept(uint64_t ballot,const Record& r){
  std::lock_guard<std::mutex> l(mu_);
  if(!connect_locked())return false;
  const auto req=request(kAccept,ballot,&r);
  Reply rep{};
  if(!send_all(fd_,&req,sizeof(req)) || !recv_all(fd_,&rep,sizeof(rep)) ||
     rep.magic!=kMagic || rep.op!=kReply || rep.count!=0){
    close_locked();return false;
  }
  return rep.ok!=0;
}
int run_tcp_acceptor(uint16_t port){
  int s=socket(AF_INET,SOCK_STREAM,0);
  if(s<0)throw std::runtime_error("socket failed");
  int yes=1;setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));
  sockaddr_in addr{};addr.sin_family=AF_INET;
  addr.sin_addr.s_addr=htonl(INADDR_ANY);addr.sin_port=htons(port);
  if(bind(s,reinterpret_cast<sockaddr*>(&addr),sizeof(addr))<0 ||
     listen(s,128)<0){::close(s);throw std::runtime_error("bind/listen failed");}
  Acceptor replica;
  for(;;){
    int c=::accept(s,nullptr,nullptr);
    if(c<0){if(errno==EINTR)continue;throw std::runtime_error("accept failed");}
    std::thread([c,&replica] {serve_client(c,replica);}).detach();
  }
}
} // namespace mucql
