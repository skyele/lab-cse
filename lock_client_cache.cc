// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <string>
#include <fstream>
#include "tprintf.h"
#include <pthread.h>
using namespace std;

//string longtostr(unsigned long i);

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  //VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
	pthread_cond_init(&global_cond,NULL);
	pthread_mutex_init(&global_mutex,NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
	int r;
	lock_protocol::status ret;
	lock_client_state lock_item;
	pthread_mutex_lock(&global_mutex);
	if(lock_state.find(lid) == lock_state.end()){
		lock_item = lock_client_state();
		lock_item.status = lock_client_state::NONE;
		lock_state[lid] = lock_item;
		pthread_cond_init(&client_cond[lid],NULL);
		pthread_cond_init(&retry_cond[lid],NULL);
		pthread_mutex_init(&client_mutex[lid],NULL);
	}
	pthread_mutex_unlock(&global_mutex);
	pthread_mutex_lock(&client_mutex[lid]);
	while(1){
		if(lock_state[lid].status == lock_client_state::NONE || lock_state[lid].status == lock_client_state::RELEASE){
			lock_state[lid].status = lock_client_state::ACQUIRE;
			pthread_mutex_unlock(&client_mutex[lid]);

			ret = cl->call(lock_protocol::acquire, lid, id, r);

			pthread_mutex_lock(&client_mutex[lid]);
			if(ret == lock_protocol::OK){
				lock_state[lid].status = lock_client_state::LOCKED;
				pthread_mutex_unlock(&client_mutex[lid]);
				return lock_protocol::OK;
			}
			else if(ret == lock_protocol::RETRY){
				while(lock_state[lid].retry == 0)
					pthread_cond_wait(&retry_cond[lid],&client_mutex[lid]);
				lock_state[lid].retry = 0;
				lock_state[lid].status = lock_client_state::LOCKED;
				pthread_mutex_unlock(&client_mutex[lid]);
				return lock_protocol::OK;
			}
		}
		else if(lock_state[lid].status == lock_client_state::FREE){
			lock_state[lid].status = lock_client_state::LOCKED;
			pthread_mutex_unlock(&client_mutex[lid]);
			return lock_protocol::OK;
		}
		else if(lock_state[lid].status == lock_client_state::LOCKED ){
			pthread_cond_wait(&client_cond[lid],&client_mutex[lid]);
			/* attention!!!!!!!!*/
			while(lock_state[lid].status == lock_client_state::ACQUIRE && lock_state[lid].retry==1){
				pthread_cond_signal(&client_cond[lid]);
				pthread_cond_wait(&client_cond[lid],&client_mutex[lid]);
			}
		}
		else{
			pthread_cond_wait(&client_cond[lid],&client_mutex[lid]);	
			/*if(lock_state[lid].status == lock_client_state::ACQUIRE&&lock_state[lid].retry==1)
				pthread_cond_signal(&client_cond[lid]);*/
		}
	}
	pthread_mutex_unlock(&client_mutex[lid]);
  	return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
	pthread_mutex_lock(&client_mutex[lid]);
	lock_protocol::status ret;
	int r;
	if(lock_state[lid].revoke){
		lock_state[lid].status = lock_client_state::RELEASE;
		lock_state[lid].revoke = 0;
		pthread_mutex_unlock(&client_mutex[lid]);
		ret = cl->call(lock_protocol::release, lid, id, r);
		pthread_mutex_lock(&client_mutex[lid]);
		
		if(lock_state[lid].status != lock_client_state::ACQUIRE && lock_state[lid].status != lock_client_state::FREE)
			lock_state[lid].status = lock_client_state::NONE;
		pthread_cond_signal(&client_cond[lid]);
	}
	else{
		lock_state[lid].status = lock_client_state::FREE;
		pthread_cond_signal(&client_cond[lid]);
	}
	pthread_mutex_unlock(&client_mutex[lid]);
	return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
	lock_protocol::status ret;
	int r;
	pthread_mutex_lock(&client_mutex[lid]);
	lock_client_state client_state = lock_state[lid];
	lock_state[lid].revoke = 1;
	switch(client_state.status){
		case lock_client_state::LOCKED:{
			lock_state[lid].revoke = 1;
			pthread_mutex_unlock(&client_mutex[lid]);
			return rlock_protocol::OK;
		}
		case lock_client_state::FREE:{
			lock_state[lid].status = lock_client_state::RELEASE;
			pthread_mutex_unlock(&client_mutex[lid]);
			ret = cl->call(lock_protocol::release, lid, id, r);
			pthread_mutex_lock(&client_mutex[lid]);
			if(lock_state[lid].status != lock_client_state::ACQUIRE && lock_state[lid].status != lock_client_state::FREE)
				lock_state[lid].status = lock_client_state::NONE;
			lock_state[lid].revoke = 0;
			pthread_mutex_unlock(&client_mutex[lid]);
			return ret;
		}
		default:{
			lock_state[lid].revoke = 1;
			pthread_mutex_unlock(&client_mutex[lid]);
			return rlock_protocol::OK;
		}
	}
	pthread_mutex_unlock(&client_mutex[lid]);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
	pthread_mutex_lock(&client_mutex[lid]);
	lock_state[lid].retry = 1;
	pthread_cond_signal(&retry_cond[lid]);
	pthread_mutex_unlock(&client_mutex[lid]);
	return rlock_protocol::OK;
}
