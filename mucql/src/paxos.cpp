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
  leader_=false;
  if(!pending_.empty())throw std::logic_error("recover pending pipeline with a new journal/ballot");
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
  submitted_=decided_.size();
  leader_=true;
}
Record PaxosJournal::commit(Record proposal) {
  auto records=commit_batch({proposal});
  return records.front();
}
bool PaxosJournal::accept_batch_to_majority(const std::vector<Record>& records) {
  const auto& replicas=quorum_.nodes();
  std::vector<bool> started(replicas.size(),false);
  for(size_t i=0;i<replicas.size();++i){
    try{replicas[i]->begin_accept_batch(ballot_,records);started[i]=true;}
    catch(const std::exception&){/* unavailable follower */}
  }
  std::vector<size_t> ack(records.size(),0);
  for(size_t i=0;i<replicas.size();++i){
    if(!started[i])continue;
    try{
      auto result=replicas[i]->finish_accept_batch();
      if(result.size()!=records.size())continue;
      for(size_t j=0;j<result.size();++j)if(result[j])ack[j]++;
    }catch(const std::exception&){/* uncertain follower */}
  }
  return std::all_of(ack.begin(),ack.end(),[&](size_t n){return n>=quorum_.majority();});
}
std::vector<Record> PaxosJournal::commit_batch(std::vector<Record> proposals) {
  std::lock_guard<std::mutex> l(mu_);
  if(!leader_)throw std::runtime_error("not a prepared leader");
  if(!pending_.empty())throw std::logic_error("synchronous commit during async pipeline");
  if(proposals.empty())return {};
  if(proposals.size()>32)throw std::invalid_argument("batch must be 1..32");
  for(size_t i=0;i<proposals.size();++i)proposals[i].index=decided_.size()+i+1;
  // Keep JOIN aggregation and RNIC phase-2 pipelining independently tunable.
  // Failed partial prefixes are NOT exposed: a new ballot must adopt them.
  for(size_t off=0;off<proposals.size();off+=phase2_window_){
    auto stop=std::min(proposals.size(),off+phase2_window_);
    std::vector<Record> window(proposals.begin()+off,proposals.begin()+stop);
    ++phase2_stats_.windows;
    phase2_stats_.records+=window.size();
    if(window.size()>1)++phase2_stats_.multi_record_windows;
    phase2_stats_.max_window=std::max(phase2_stats_.max_window,window.size());
    if(!accept_batch_to_majority(window)){
      leader_=false;
      throw std::runtime_error("phase 2 window quorum failed; recover before retry");
    }
  }
  decided_.insert(decided_.end(),proposals.begin(),proposals.end());
  submitted_=decided_.size();
  return proposals;
}
uint64_t PaxosJournal::submit_batch(std::vector<Record> proposals) {
  std::lock_guard<std::mutex> l(mu_);
  if(!leader_)throw std::runtime_error("not a prepared leader");
  if(proposals.empty()||proposals.size()>phase2_window_)
    throw std::invalid_argument("async batch exceeds phase2 window");
  if(proposals.size()>32-inflight_records_)
    throw std::logic_error("async pipeline has no credits");
  Pending p;p.id=next_batch_++;p.records=std::move(proposals);
  for(auto& r:p.records)r.index=++submitted_;
  const auto& replicas=quorum_.nodes();
  p.finished.resize(replicas.size(),false);p.votes.resize(p.records.size(),0);
  const bool overlapping=std::any_of(pending_.begin(),pending_.end(),
      [](const Pending& old){return !old.published;});
  pending_.push_back(std::move(p));
  auto& posted=pending_.back();
  inflight_records_+=posted.records.size();
  ++phase2_stats_.windows;phase2_stats_.records+=posted.records.size();
  if(posted.records.size()>1)++phase2_stats_.multi_record_windows;
  phase2_stats_.max_window=std::max(phase2_stats_.max_window,posted.records.size());
  phase2_stats_.max_inflight_batches=std::max(phase2_stats_.max_inflight_batches,pending_.size());
  phase2_stats_.max_inflight_records=std::max(phase2_stats_.max_inflight_records,inflight_records_);
  if(overlapping)++phase2_stats_.overlap_submissions;
  for(size_t i=0;i<replicas.size();++i){
    try{replicas[i]->submit_batch(posted.id,ballot_,posted.records);}
    catch(const std::exception&){posted.finished[i]=true;}
  }
  return posted.id;
}
std::vector<PaxosJournal::Completion> PaxosJournal::poll_commits() {
  std::lock_guard<std::mutex> l(mu_);
  if(!leader_)throw std::runtime_error("not a prepared leader");
  const auto& replicas=quorum_.nodes();
  // Poll every replica and every outstanding batch, including a slow tail
  // of an already chosen batch. No wait on one follower blocks another.
  for(auto& p:pending_){
    for(size_t i=0;i<replicas.size();++i){
      if(p.finished[i])continue;
      try{
        auto result=replicas[i]->poll_batch(p.id);
        if(!result)continue;
        p.finished[i]=true;
        if(result->size()==p.records.size())
          for(size_t j=0;j<result->size();++j)if((*result)[j])++p.votes[j];
      }catch(const std::exception&){p.finished[i]=true;}
    }
    const auto remaining=std::count(p.finished.begin(),p.finished.end(),false);
    for(auto votes:p.votes)if(votes+remaining<quorum_.majority()){
      leader_=false;
      throw std::runtime_error("async phase 2 quorum failed; recover at a higher ballot");
    }
  }
  std::vector<Completion> result;
  for(auto& p:pending_){
    if(p.published)continue;
    if(!std::all_of(p.votes.begin(),p.votes.end(),[&](size_t n){return n>=quorum_.majority();}))break;
    decided_.insert(decided_.end(),p.records.begin(),p.records.end());
    p.published=true;result.push_back({p.id,p.records});
  }
  // Credits protect registered buffers until ALL replicas have completed
  // or failed. Publication itself only needs a quorum.
  for(auto it=pending_.begin();it!=pending_.end();){
    if(it->published&&std::all_of(it->finished.begin(),it->finished.end(),[](bool b){return b;})){
      inflight_records_-=it->records.size();it=pending_.erase(it);
    }else ++it;
  }
  return result;
}
size_t PaxosJournal::available_slots() const {
  std::lock_guard<std::mutex> l(mu_);return 32-inflight_records_;
}
size_t PaxosJournal::submitted_count() const {
  std::lock_guard<std::mutex> l(mu_);return submitted_;
}
bool PaxosJournal::pipeline_idle() const {
  std::lock_guard<std::mutex> l(mu_);return pending_.empty();
}
void PaxosJournal::set_phase2_window(size_t n){
  if(n<1||n>32)throw std::invalid_argument("phase2 window must be 1..32");
  std::lock_guard<std::mutex> l(mu_);
  if(!pending_.empty())throw std::logic_error("changing window with pending pipeline");
  phase2_window_=n;
}
PaxosJournal::Phase2Stats PaxosJournal::phase2_stats() const {
  std::lock_guard<std::mutex> l(mu_);return phase2_stats_;
}
size_t PaxosJournal::decided_count() const {
  std::lock_guard<std::mutex> l(mu_);return decided_.size();
}
std::vector<Record> PaxosJournal::snapshot() const {
  std::lock_guard<std::mutex> l(mu_);
  return decided_;
}
} // namespace mucql
