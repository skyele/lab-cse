// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"
using namespace std;

class lock_client_state{
	public:
	 enum xxstatus { NONE, FREE, LOCKED, ACQUIRE, RELEASE };
	 int status;
	 int retry;
	 int revoke;
	 lock_client_state(){
	 	status = lock_client_state::NONE;
		retry = 0;
		revoke = 0;
	 };
};

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  map<lock_protocol::lockid_t,lock_client_state> lock_state;
  map<lock_protocol::lockid_t,pthread_mutex_t> client_mutex;
  map<lock_protocol::lockid_t,pthread_cond_t> client_cond;
  map<lock_protocol::lockid_t,pthread_cond_t> retry_cond;
  map<lock_protocol::lockid_t,int> retry_flag;
  map<lock_protocol::lockid_t,int> revoke_flag;
  pthread_mutex_t global_mutex;
  pthread_cond_t global_cond;
 public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
};


#endif
