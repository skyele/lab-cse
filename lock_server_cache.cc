// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"
using namespace std;

lock_server_cache::lock_server_cache():
 nacquire(0)
{
	pthread_mutex_init(&global_mutex,NULL);
	pthread_cond_init(&global_cond,NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &)
{
	pthread_mutex_lock(&global_mutex);
	lock_server_state lock_info;
  	lock_protocol::status ret = lock_protocol::OK;
	
	// no exist
	if(lock_state.find(lid) == lock_state.end()){
		lock_info = lock_server_state();
		lock_info.hold = 1;
		lock_info.hold_client = id;
		lock_state[lid] = lock_info;
		pthread_mutex_init(&server_mutex[lid],NULL);
		pthread_cond_init(&server_cond[lid],NULL);
		pthread_mutex_unlock(&global_mutex);
		return lock_protocol::OK;
	}
	else{
		pthread_mutex_unlock(&global_mutex);
		pthread_mutex_lock(&server_mutex[lid]);
		lock_info = lock_state[lid];
		// no hold && no waiting
		if(lock_info.hold == 0 && lock_info.waiting_client.size()==0){
			lock_info.hold = 1;	
			lock_info.hold_client = id;
			lock_state[lid] = lock_info;
  			ret = lock_protocol::OK;
		}
		// hold && no waiting
		else if(lock_info.hold == 1 && lock_info.waiting_client.size()==0){
			lock_state[lid].waiting_client.push(id);
			handle client_to_revoke = handle(lock_state[lid].hold_client);
			rpcc *cl = client_to_revoke.safebind();
			if(cl){
				int r;
				pthread_mutex_unlock(&server_mutex[lid]);
				ret = cl->call(rlock_protocol::revoke,lid,r);
				pthread_mutex_lock(&server_mutex[lid]);
				ret = lock_protocol::RETRY;
			} else{ //failure
				tprintf("there is an error in safe bind 1\n");//just test
			}
		}
		else if(lock_info.hold == 0 && lock_info.waiting_client.size() != 0){
			cout<<id<<": warning"<<endl;//just test
		}
		// hold && yes waiting
		else{
			lock_state[lid].waiting_client.push(id);
			ret = lock_protocol::RETRY;
		}
	}
	pthread_mutex_unlock(&server_mutex[lid]);
	return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
	lock_protocol::status ret = lock_protocol::OK;
	pthread_mutex_lock(&server_mutex[lid]);
	lock_server_state server_state = lock_state[lid];
	if(server_state.hold == 0)
		;
	//	cout<<id<<": no one hold how to release!"<<endl;//just test
	else{
		if(server_state.waiting_client.size() == 0){
			lock_state[lid].hold = 0;
			lock_state[lid].hold_client ="";
			pthread_mutex_unlock(&server_mutex[lid]);
			return lock_protocol::OK;
		}
		else{
			handle client_to_retry = handle(lock_state[lid].waiting_client.front());
			rpcc *cl = client_to_retry.safebind();
			if(cl){
				int r;
				lock_state[lid].hold=1;
				lock_state[lid].hold_client=lock_state[lid].waiting_client.front();
				lock_state[lid].waiting_client.pop();
				if(lock_state[lid].waiting_client.size()!=0){
					pthread_mutex_unlock(&server_mutex[lid]);
					ret = cl->call(rlock_protocol::revoke,lid,r);
					pthread_mutex_lock(&server_mutex[lid]);
				}
				pthread_mutex_unlock(&server_mutex[lid]);
				ret = cl->call(rlock_protocol::retry,lid,r);
				pthread_mutex_lock(&server_mutex[lid]);
			} else{ //failure
				printf("bind failure\n");//just test
			}
		}
	}
	pthread_mutex_unlock(&server_mutex[lid]);

  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

