#include "mucql/tcp_transport.hpp"
#include <iostream>
#include <stdexcept>
#include <cstdlib>
int main(int argc,char** argv){
  if(argc!=2){std::cerr<<"Usage: mucql_acceptor <port>\n";return 2;}
  try { return mucql::run_tcp_acceptor(static_cast<uint16_t>(std::stoul(argv[1]))); }
  catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 1;}
}
