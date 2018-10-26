// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

using namespace std;

static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex_ac;
pthread_mutex_t mutex_re;

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	//if(lock_map.find(lid) == lock_map.end())
	//	lock_map.insert(pair(lock_protocol::lockid_t,int>)(i,1));
  	printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
	pthread_mutex_lock(&mutex_ac);
	while(lock_map[lid] != 0)
		pthread_cond_wait(&cond, &mutex_ac);
	lock_map[lid]++;
	pthread_mutex_unlock(&mutex_ac);
  	lock_protocol::status ret = lock_protocol::OK;
  	r = lock_map[lid];
	// Your lab2 part2 code goes here
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  	pthread_mutex_lock(&mutex_re);
	/*
	if(lock_map[lid] == 0){
  		r = lock_map[lid];
		pthread_mutex_unlock(&mutex_re);
		return lock_protocol::RPCERR; //no one acquire but release
	}*/
	lock_map[lid]--;
	pthread_cond_signal(&cond);
	pthread_mutex_unlock(&mutex_re);
	lock_protocol::status ret = lock_protocol::OK;
  	r = lock_map[lid];
	// Your lab2 part2 code goes here
  return ret;
}
