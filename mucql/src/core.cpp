#include "mucql/core.hpp"
#include <algorithm>
#include <limits>
#include <set>

namespace mucql {
Record MemoryJournal::commit(Record r) {
  std::lock_guard<std::mutex> g(mu_);
  if (log_.size() == std::numeric_limits<uint64_t>::max()-1)
    throw std::overflow_error("journal full");
  r.index = static_cast<uint64_t>(log_.size()+1);
  log_.push_back(r);
  return r;
}
std::vector<Record> MemoryJournal::snapshot() const {
  std::lock_guard<std::mutex> g(mu_);
  return log_;
}
Leader::Leader(Journal& journal, const Clock& clock, Config config)
  : journal_(journal), clock_(clock), config_(config) {
  if (!config_.horizon_ns || !config_.prepare_batch || !config_.prepared_ahead)
    throw std::invalid_argument("bad config");
  recover();
}
void Leader::replay(const Record& r) {
  auto& s=state_[r.lock];
  if (r.type==Record::Type::Join) {
    if (r.index==0 || s.queue.size()!=r.first ||
        s.requests.count({r.client,r.request}))
      throw std::logic_error("invalid decided JOIN");
    Ticket t{r.lock,r.client,r.request,r.first,r.index,
             s.queue.empty()?0:s.queue.back().grant_id,0};
    s.requests[{r.client,r.request}]=r.first;
    s.queue.push_back(t);
  } else if (r.type==Record::Type::Prepare) {
    if (r.first!=s.prepared || r.last<r.first || r.last>=s.queue.size()
        || r.deadline==0 ||
        (s.prepared && r.deadline<s.queue[s.prepared-1].deadline))
      throw std::logic_error("invalid decided PREPARE");
    for (uint64_t i=r.first;i<=r.last;i++) s.queue[i].deadline=r.deadline;
    s.prepared=static_cast<size_t>(r.last)+1;
  } else throw std::logic_error("unknown journal record");
}
void Leader::recover() {
  std::lock_guard<std::mutex> g(mu_);
  state_.clear();
  auto records=journal_.snapshot();
  uint64_t last_index=0;
  for (auto& r: records) {
    if (r.index<=last_index) throw std::logic_error("nonmonotone decided log");
    replay(r);last_index=r.index;
  }
}
Ticket Leader::join(LockId lock, ClientId client, RequestId request) {
  std::lock_guard<std::mutex> g(mu_);
  auto& s=state_[lock];
  auto key=std::make_pair(client,request);
  auto it=s.requests.find(key);
  if(it!=s.requests.end()) return s.queue[it->second];
  Record r{};r.type=Record::Type::Join;r.lock=lock;r.client=client;
  r.request=request;r.first=s.queue.size();
  // commit() may throw, in which case caller must re-read/recover and retry.
  // Production adapter must not acknowledge an uncommitted proposal.
  r=journal_.commit(r);
  replay(r);
  return state_[lock].queue.back();
}
std::vector<Ticket> Leader::join_batch(LockId lock,
    const std::vector<std::pair<ClientId,RequestId>>& requests) {
  std::lock_guard<std::mutex> g(mu_);
  if(requests.empty())return {};
  if(requests.size()>32)throw std::invalid_argument("join batch must be <=32");
  auto& s=state_[lock];
  std::set<std::pair<ClientId,RequestId>> seen;
  std::vector<Record> records;
  for(const auto& req:requests){
    if(!req.first || !req.second || s.requests.count(req) || !seen.insert(req).second)
      throw std::invalid_argument("duplicate or invalid batched client request");
    Record r{};r.type=Record::Type::Join;r.lock=lock;r.client=req.first;
    r.request=req.second;r.first=s.queue.size()+records.size();
    records.push_back(r);
  }
  auto committed=journal_.commit_many(std::move(records));
  for(const auto& record:committed)replay(record);
  std::vector<Ticket> tickets;
  for(size_t i=s.queue.size()-committed.size();i<s.queue.size();++i)
    tickets.push_back(s.queue[i]);
  return tickets;
}
bool Leader::add_overflows(uint64_t a,uint64_t b){
  return a>std::numeric_limits<uint64_t>::max()-b;
}
std::vector<Ticket> Leader::prepare(LockId lock, size_t max_count) {
  std::lock_guard<std::mutex> g(mu_);
  auto& s=state_[lock];
  if(!max_count || s.prepared>=s.queue.size())return {};
  size_t count=std::min({max_count,config_.prepare_batch,
                          s.queue.size()-s.prepared});
  // Conservative contiguous progress: a completed release marker or a
  // *certain* deadline expiration retires a prepared position. Without a
  // release observation recovery remains conservative.
  Position progress=s.progress;
  const auto now=clock_.now();
  if(now.lower>now.upper)throw std::logic_error("invalid clock bounds");
  while(progress<s.prepared &&
        (s.released.count(progress) ||
         now.lower>=s.queue[progress].deadline)) ++progress;
  s.progress=progress;
  if(s.prepared-progress>=config_.prepared_ahead)return {};
  count=std::min(count, config_.prepared_ahead-(s.prepared-progress));
  if(count==0)return {};
  const auto b=clock_.now();
  if(b.lower>b.upper || add_overflows(b.upper,config_.horizon_ns))
    throw std::logic_error("clock bounds invalid/overflow");
  Deadline deadline=b.upper+config_.horizon_ns;
  if(s.prepared)deadline=std::max(deadline,s.queue[s.prepared-1].deadline);
  Record r{};r.type=Record::Type::Prepare;r.lock=lock;
  r.first=s.prepared;r.last=s.prepared+count-1;r.deadline=deadline;
  r=journal_.commit(r);
  replay(r);
  auto& q=state_[lock].queue;
  return std::vector<Ticket>(q.begin()+r.first,q.begin()+r.last+1);
}
std::optional<Ticket> Leader::ticket(LockId lock, Position pos) const {
  std::lock_guard<std::mutex> g(mu_);
  auto it=state_.find(lock);
  if(it==state_.end() || pos>=it->second.queue.size())return std::nullopt;
  return it->second.queue[pos];
}
std::vector<Ticket> Leader::ordered(LockId lock) const {
  std::lock_guard<std::mutex> g(mu_);
  auto it=state_.find(lock);
  return it==state_.end()?std::vector<Ticket>{}:it->second.queue;
}
Position Leader::prepared_count(LockId lock) const {
  std::lock_guard<std::mutex> g(mu_);
  auto it=state_.find(lock);return it==state_.end()?0:it->second.prepared;
}
void Leader::observe(const ReleaseMark& mark) {
  std::lock_guard<std::mutex> g(mu_);
  auto it=state_.find(mark.lock);
  if(it==state_.end() || mark.position>=it->second.prepared)return;
  const auto& t=it->second.queue[mark.position];
  if(t.grant_id!=mark.grant_id)return;
  it->second.released[mark.position]=mark.grant_id;
}
bool Leader::may_start(LockId lock, Position pos) const {
  std::lock_guard<std::mutex> g(mu_);
  auto it=state_.find(lock);
  if(it==state_.end() || pos>=it->second.prepared)return false;
  auto& s=it->second;
  if(pos==0)return true;
  const auto& pred=s.queue[pos-1];
  auto released=s.released.find(pos-1);
  if(released!=s.released.end()&&released->second==pred.grant_id)return true;
  Bounds b=clock_.now();
  return b.lower>=pred.deadline;
}
void Client::on_prepared(const Ticket& t) {
  if(t.client!=id_ || t.grant_id==0 || t.deadline==0)
    throw std::invalid_argument("invalid PREPARED ticket");
  auto it=grants_.find(t.grant_id);
  if(it==grants_.end())grants_.emplace(t.grant_id,Local{t,Status::Waiting});
  else if(it->second.ticket.lock!=t.lock || it->second.ticket.position!=t.position
          || it->second.ticket.predecessor_id!=t.predecessor_id
          || it->second.ticket.deadline!=t.deadline)
    throw std::logic_error("changed prepared ticket");
}
bool Client::activate(const Ticket& t,const Clock& clock) {
  auto it=grants_.find(t.grant_id);
  if(it==grants_.end() || it->second.status!=Status::Waiting)return false;
  const Ticket& known=it->second.ticket;
  if(known.lock!=t.lock || known.position!=t.position ||
     known.predecessor_id!=t.predecessor_id ||
     known.deadline!=t.deadline || known.client!=id_)return false;
  auto b=clock.now();
  if(b.lower>b.upper || b.upper>=t.deadline)return false;
  // Even if malformed notifications arrive, a client never acquires two
  // positions of the same lock concurrently.
  for(const auto& kv:grants_){
    if(kv.first!=t.grant_id && kv.second.ticket.lock==t.lock &&
       kv.second.status==Status::Holding)return false;
  }
  it->second.status=Status::Holding;return true;
}
bool Client::activate_handoff(const Handoff& h,const Clock& clock) {
  if(h.successor.position==0 || h.successor.position-1!=h.predecessor ||
     !h.predecessor_id || h.successor.predecessor_id!=h.predecessor_id)return false;
  // In crash-stop model the predecessor sends only after irrevocable retire.
  // Production must authenticate the sender's identity and validate that
  // predecessor_id is the committed prior queue entry for the same lock.
  return activate(h.successor,clock);
}
bool Client::activate_start(const Ticket& t,bool leader_authorized,
                            const Clock& clock) {
  if(!leader_authorized)return false;
  return activate(t,clock);
}
Client::Release Client::retire(const Ticket& current,
                              const std::optional<Ticket>& next) {
  auto it=grants_.find(current.grant_id);
  if(it==grants_.end() || it->second.status!=Status::Holding ||
     it->second.ticket.lock!=current.lock ||
     it->second.ticket.position!=current.position ||
     it->second.ticket.client!=current.client ||
     it->second.ticket.request!=current.request ||
     it->second.ticket.deadline!=current.deadline ||
     it->second.ticket.predecessor_id!=current.predecessor_id)
    throw std::logic_error("release without ownership");
  // Crucial ordering: all protected effects must have completed BEFORE this.
  it->second.status=Status::Retired;
  Release result{ReleaseMark{current.lock,current.position,current.grant_id},
                 std::nullopt};
  if(next && next->lock==current.lock && next->position==current.position+1
     && next->deadline!=0 && next->predecessor_id==current.grant_id)
    result.handoff=Handoff{*next,current.grant_id,current.position};
  return result;
}
Client::Status Client::status(GrantId id) const {
  auto it=grants_.find(id);return it==grants_.end()?Status::Waiting:it->second.status;
}
bool Client::may_use(const Ticket& t,const Clock& clock) const {
  auto it=grants_.find(t.grant_id);
  return it!=grants_.end() && it->second.status==Status::Holding &&
         clock.now().upper<t.deadline;
}
} // namespace mucql
