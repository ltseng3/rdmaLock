#include "mucql/core.hpp"
#include <cassert>
#include <iostream>
#include <random>
#include <memory>
using namespace mucql;
struct ManualClock: Clock {
  uint64_t t=1000;
  Bounds now() const override {return {t-3,t+3};}
};
int main(){
  constexpr int runs=2000, n=12;
  for(int seed=0;seed<runs;seed++){
    std::mt19937 rng(seed);
    MemoryJournal journal;
    ManualClock clock;
    Config config;config.horizon_ns=60;config.prepared_ahead=16;config.prepare_batch=16;
    auto l=std::make_unique<Leader>(journal,clock,config);
    std::vector<Client> cs;
    for(int i=0;i<n;i++)cs.emplace_back(static_cast<ClientId>(i+100));
    for(int i=0;i<n;i++)l->join(1,static_cast<ClientId>(i+100),i+1);
    auto tickets=l->prepare(1,n);assert(tickets.size()==n);
    for(int i=0;i<n;i++)cs[i].on_prepared(tickets[i]);
    cs[0].activate_start(tickets[0],true,clock);
    std::vector<std::optional<Handoff>> messages;
    std::vector<ReleaseMark> markers;
    for(int step=0;step<120;step++){
      int act=rng()%7;
      if(act==0){ // release random currently holding client
        int i=rng()%n;
        if(cs[i].status(tickets[i].grant_id)==Client::Status::Holding){
          auto r=cs[i].retire(tickets[i],i+1<n?std::optional<Ticket>(tickets[i+1]):std::nullopt);
          markers.push_back(r.mark);
          if(r.handoff)messages.push_back(r.handoff);
        }
      }else if(act==1 && !messages.empty()){
        int k=rng()%messages.size();
        if(messages[k]){
          auto& h=*messages[k];
          cs[h.successor.position].activate_handoff(h,clock);
          if(rng()%2)messages[k].reset();
        }
      }else if(act==2 && !markers.empty()){
        auto mark=markers[rng()%markers.size()];
        l->observe(mark);
      }else if(act==3){
        int i=rng()%n;
        if(l->may_start(1,i))cs[i].activate_start(tickets[i],true,clock);
      }else if(act==4){
        l=std::make_unique<Leader>(journal,clock,config); // lose all release observations
      }else{
        clock.t+=rng()%5;
      }
      int live=0;
      for(int i=0;i<n;i++)if(cs[i].may_use(tickets[i],clock))live++;
      if(live>1){std::cerr<<"seed "<<seed<<" step "<<step<<" live "<<live<<"\n";return 1;}
    }
  }
  std::cout<<"PASS: 2000 randomized schedules, lost markers, delayed handoffs, failover\n";
}
