#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
  ec = new extent_client(extent_dst);
  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

	cout<<"the host name is "<<GetHostname()<<endl;//just test
	fflush(stdout);//just test

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);
  if (!ConnectToNN()) {
	printf("datanode error 1\n");//just test
	fflush(stdout);//just test
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode()) {
	printf("datanode error 2\n");//just test
	fflush(stdout);//just test

    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }
  /* Add your initialization here */
	NewThread(this,&DataNode::InvokeSendHeartBeat,0);

	printf("finish datanode init\n");//just test
	fflush(stdout);//just test
  return 0;
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
  /* Your lab4 part 2 code */
	cout<<"in DataNode::ReadBlock"<<endl;//just test
	fflush(stdout);//just test
	string raw;
	int res = ec->read_block(bid,raw);
	if(res != extent_protocol::OK){	
		printf("in ReadBlock readblock fail???\n");//just test
		fflush(stdout);//just test
		return false;
	}
	
	if(len == 0){
		buf = "";
	}
	else{
		if(offset >= BLOCK_SIZE){
			buf = "";
		}
		else
			buf = raw.substr(offset,len);
	}
	return true;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
  /* Your lab4 part 2 code */
	cout<<"in DataNode::WriteBlock"<<endl;//just test
	cout<<"the offset is : "<<offset<<" the len is : "<<len<<endl;//just test
	fflush(stdout);//just test
	string raw;
	int res = ec->read_block(bid,raw);
	if(res != extent_protocol::OK){
		printf("in WriteBlock readblock fail???\n");//just test
		fflush(stdout);//just test
		return false;
	}
	cout<<"the raw string size is : "<<raw.length()<<endl;//just test
	if(offset == 0){
		if(offset+len >= BLOCK_SIZE)
			raw = buf.substr(0,len);
		else
			raw = buf.substr(0,len) + raw.substr(len,BLOCK_SIZE - len);
	}
	else if(offset < BLOCK_SIZE){
		if(offset+len >= BLOCK_SIZE)
			raw = raw.substr(0,offset) + buf.substr(0,len);
		else
			raw = raw.substr(0,offset) + buf.substr(0,len) + raw.substr(offset+len);
	}
	else
		raw = raw;
	raw = raw.substr(0,BLOCK_SIZE);
	cout<<"after operation the size is : "<<raw.length()<<endl;//just test
	res = ec->write_block(bid,raw);	
	if(res != extent_protocol::OK){
		printf("in WriteBlock writeblock fail???\n");//just test
		fflush(stdout);//just test
		return false;
	}
	return true;
}

void DataNode::InvokeSendHeartBeat(int r){
	cout<<"in DataNode::InvokeSendHeartBeat"<<endl;//just test
	fflush(stdout);//just test
	while(true){
		if(SendHeartbeat() == 0){
			printf("there is an error in send heart beat\n");//just test
			fflush(stdout);//just test
			sleep(1);
			continue;
		}
		printf("in while infinit loop\n");//just test
		fflush(stdout);//just test
		sleep(1);
	}
}
