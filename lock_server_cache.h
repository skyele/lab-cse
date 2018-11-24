#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <map>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
using namespace std;

class lock_server_state{
	public:
	 int hold;
	 string hold_client;
	 queue<string> waiting_client;
	 lock_server_state(){
	 	hold = 0;
		hold_client = "";
		vector<string> waiting_client;
	 }
};

class lock_server_cache {
 private:
  int nacquire;
  map<lock_protocol::lockid_t,lock_server_state> lock_state;
  map<lock_protocol::lockid_t,pthread_cond_t> server_cond;
  map<lock_protocol::lockid_t,pthread_mutex_t> server_mutex;
  pthread_mutex_t global_mutex;
  pthread_cond_t global_cond;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
