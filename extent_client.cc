// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
using namespace std;

extent_client::extent_client(std::string dst)
{
	cout<<"in extext_client::init"<<endl;//just test
	fflush(stdout);//just test
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
	fflush(stdout);//just test
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
	fflush(stdout);//just test
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
	fflush(stdout);//just test
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
	printf("in put in extent_client\n");//just test
	printf("the put eid : %d\n",(int)eid);//just test
	cout<<"the input string is "<<buf<<endl;//just test
	fflush(stdout);//just test
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
	fflush(stdout);//just test
  extent_protocol::status ret = extent_protocol::OK;
	int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  // Your lab2 part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::get_block_ids(extent_protocol::extentid_t eid, std::list<blockid_t> &block_ids)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::get_block_ids, eid, block_ids);
  return ret;
}

extent_protocol::status
extent_client::read_block(blockid_t bid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::read_block, bid, buf);
  return ret;
}

extent_protocol::status
extent_client::write_block(blockid_t bid, const std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::write_block, bid, buf, r);
  return ret;
}

extent_protocol::status
extent_client::append_block(extent_protocol::extentid_t eid, blockid_t &bid)
{
	printf("in extent_client::append_block\n");//just test
	fflush(stdout);//just test
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::append_block, eid, bid);
  return ret;
}

extent_protocol::status
extent_client::complete(extent_protocol::extentid_t eid, uint32_t size)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::complete, eid, size, r);
  return ret;
}

