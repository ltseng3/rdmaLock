#include "mucql/paxos.hpp"
#include <cassert>
#include <iostream>
#include <stdexcept>
using namespace mucql;
struct ClockImpl : Clock { Bounds now() const override{return {99,101};} };
int main(){
 ClockImpl clock;
 LocalQuorum q(3);
 PaxosJournal j1(q,10);j1.take_over();
 Leader l1(j1,clock);auto a=l1.join(2,100,1);
 auto p=l1.prepare(2,1);
 assert(p[0].grant_id==a.grant_id && p[0].deadline>0);
 PaxosJournal j2(q,20);j2.take_over();Leader l2(j2,clock);
 assert(l2.ordered(2).size()==1 && l2.ordered(2)[0].grant_id==a.grant_id);
 // Old leader cannot choose after a higher promised ballot on a quorum.
 bool rejected=false;
 try{ l1.join(2,200,1); }catch(const std::runtime_error&){rejected=true;}
 assert(rejected);
 // New leader advances ordered prefix.
 auto b=l2.join(2,200,1);assert(b.position==1);
 assert(l2.join(2,200,1).grant_id==b.grant_id);
 // One replica can be unavailable; quorum still survives.
 q.nodes()[2]->crash(true);
 auto c=l2.join(2,300,1);assert(c.position==2);
 // Losing two replicas prevents a decision. The old leader must stop.
 q.nodes()[1]->crash(true);
 bool no_quorum=false;try{l2.join(2,400,1);}catch(const std::runtime_error&){no_quorum=true;}
 assert(no_quorum);
 // Partially accepted tail may be adopted and decided during recovery.
 q.nodes()[1]->crash(false);
 PaxosJournal j3(q,30);j3.take_over();Leader l3(j3,clock);
 assert(l3.ordered(2).size()==4); // adopted formerly unacknowledged tail
 assert(l3.ordered(2)[0].grant_id==a.grant_id);
 assert(l3.ordered(2)[1].grant_id==b.grant_id);
 // All existing entries have one log-index order across leader switches.
 for(size_t i=0;i<l3.ordered(2).size();i++)
   assert(l3.ordered(2)[i].position==i);
 std::cout<<"PASS: Paxos phase1 adoption, phase2 quorum, one crash, old leader fenced\n";
}
