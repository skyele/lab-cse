// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

// a demo to show how to use RPC
extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
	printf("in create in extent_client\n");//just test
	printf("the create id : %d\n",(int)id);//just test
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::create, type, id);
  // Your lab2 part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
	printf("in get in extent_client\n");//just test
	printf("the get eid : %d\n",(int)eid);//just test
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::get, eid, buf);
  // Your lab2 part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
	printf("in getattr in extent_client\n");//just test
	printf("the getattr eid : %d\n",(int)eid);//just test
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
	printf("in put in extent_client\n");//just test
	printf("the put eid : %d\n",(int)eid);//just test
  extent_protocol::status ret = extent_protocol::OK;
	int r;
  ret = cl->call(extent_protocol::put, eid, buf, r);
  // Your lab2 part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
	printf("in remove in extent_client\n");//just test
	printf("the remove eid : %d\n",(int)eid);//just test
  extent_protocol::status ret = extent_protocol::OK;
	int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  // Your lab2 part1 code goes here
  return ret;
}


