#include "mucql/paxos.hpp"
#include <algorithm>
#include <stdexcept>
#include <utility>
namespace mucql {
Promise Acceptor::prepare(uint64_t b) {
  std::lock_guard<std::mutex> l(mu_);
  if(down_ || b<promised_)return {};
  promised_=b;
  return Promise{true,accepted_};
}
bool Acceptor::accept(uint64_t b,const Record& value) {
  std::lock_guard<std::mutex> l(mu_);
  if(down_ || !b || b<promised_ || !value.index)return false;
  auto it=accepted_.find(value.index);
  // Multiple accept attempts at same ballot/slot cannot use different values.
  if(it!=accepted_.end() && it->second.ballot==b) {
    const auto& v=it->second.value;
    if(v.index!=value.index || v.type!=value.type || v.lock!=value.lock ||
       v.first!=value.first || v.last!=value.last || v.client!=value.client ||
       v.request!=value.request || v.deadline!=value.deadline)
      throw std::logic_error("same-ballot slot equivocation");
  }
  promised_=b;
  accepted_[value.index]=Accepted{b,value};
  return true;
}
void Acceptor::crash(bool d) { std::lock_guard<std::mutex> l(mu_);down_=d; }
LocalQuorum::LocalQuorum(size_t n) {
  if(n<3 || n%2==0)throw std::invalid_argument("odd quorum >= 3 required");
  for(size_t i=0;i<n;i++)nodes_.push_back(std::make_shared<Acceptor>());
}
LocalQuorum::LocalQuorum(std::vector<std::shared_ptr<Acceptor>> replicas)
  : nodes_(std::move(replicas)) {
  if(nodes_.size()<3 || nodes_.size()%2==0)
    throw std::invalid_argument("odd quorum >= 3 required");
}
PaxosJournal::PaxosJournal(LocalQuorum& q,uint64_t b):quorum_(q),ballot_(b) {
  if(!ballot_)throw std::invalid_argument("zero ballot");
}
bool PaxosJournal::accept_to_majority(const Record& r) {
  // Post all phase-2 acceptances before waiting for any single follower.
  // In the RDMA adapter this overlaps follower WRITEs; in TCP/local adapters
  // begin_accept() conservatively runs synchronously.
  const auto& replicas=quorum_.nodes();
  std::vector<bool> started(replicas.size(),false);
  for(size_t i=0;i<replicas.size();++i){
    try{replicas[i]->begin_accept(ballot_,r);started[i]=true;}
    catch(const std::exception&){ /* unavailable follower */ }
  }
  size_t ok=0;
  for(size_t i=0;i<replicas.size();++i){
    if(!started[i])continue;
    try{if(replicas[i]->finish_accept())ok++;}
    catch(const std::exception&){ /* timeout, failed QP or replica */ }
  }
  return ok>=quorum_.majority();
}
void PaxosJournal::take_over() {
  std::lock_guard<std::mutex> l(mu_);
  // Phase-1 response contains all accepted slots, including uncommitted.
  std::map<uint64_t,Accepted> merged;
  size_t promised=0;
  for(const auto& a:quorum_.nodes()){
    auto p=a->prepare(ballot_);
    if(!p.granted)continue;
    promised++;
    for(const auto& kv:p.accepted){
      auto it=merged.find(kv.first);
      if(it==merged.end() || it->second.ballot<kv.second.ballot)
        merged[kv.first]=kv.second;
    }
  }
  if(promised<quorum_.majority()){
    leader_=false;
    throw std::runtime_error("phase 1 quorum unavailable/promised to higher ballot");
  }
  // Re-proposing all earlier accepted values, including an incomplete tail,
  // ensures no client-visible JOIN can be contradicted after takeover.
  // The queue protocol issues serial appends, so holes cannot be generated.
  std::vector<Record> restored;
  uint64_t expected=1;
  for(const auto& kv:merged){
    if(kv.first!=expected || kv.second.value.index!=expected)
      throw std::logic_error("accepted log contains a hole");
    const auto& value=kv.second.value;
    if(!accept_to_majority(value)){
      leader_=false;
      throw std::runtime_error("could not re-accept adopted value");
    }
    restored.push_back(value);
    expected++;
  }
  decided_=std::move(restored);
  leader_=true;
}
Record PaxosJournal::commit(Record proposal) {
  std::lock_guard<std::mutex> l(mu_);
  if(!leader_)throw std::runtime_error("not a prepared leader");
  proposal.index=decided_.size()+1;
  if(!accept_to_majority(proposal)){
    leader_=false; // cannot safely reuse same slot for a different command
    throw std::runtime_error("phase 2 quorum failed; recover before retry");
  }
  decided_.push_back(proposal);
  return proposal;
}
std::vector<Record> PaxosJournal::snapshot() const {
  std::lock_guard<std::mutex> l(mu_);
  return decided_;
}
} // namespace mucql
