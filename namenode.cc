#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"
#include <time.h>

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
	cout<<"in NameNode::init"<<endl;//just test
	fflush(stdout);//just test
	ec = new extent_client(extent_dst);
	lc = new lock_client_cache(lock_dst);
	yfs = new yfs_client(extent_dst, lock_dst);
  /* Add your init logic here */
	pthread_mutex_init(&mutex_map,NULL);
	pthread_mutex_init(&mutex_list,NULL);

	cout<<"finish NameNode::init"<<endl;//just test
	fflush(stdout);//just test
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
	cout<<"in NameNode::GetBlockLocations"<<endl;//just test
	fflush(stdout);//just test
	extent_protocol::attr a;
	ec->getattr(ino,a);
	
	list<blockid_t> block_ids;
	ec->get_block_ids(ino,block_ids);
	list<blockid_t>::iterator itor;
	itor = block_ids.begin();
	list<NameNode::LocatedBlock> res;
	blockid_t tmp;
	int m = 0;
	while(itor != block_ids.end()){
		tmp = *itor;
		if( a.size - m >= BLOCK_SIZE)
			res.push_back(LocatedBlock(tmp,m,BLOCK_SIZE,GetDatanodes()));
		else
			res.push_back(LocatedBlock(tmp,m,a.size%BLOCK_SIZE,GetDatanodes()));
		itor++;
		m += BLOCK_SIZE;
	} 
 	return res;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
	cout<<"in NameNode::Complete"<<endl;//just test
	cout<<"the ino : "<<ino<<" the new_size : "<<new_size<<endl;//just test
	fflush(stdout);//just test
	if(ec->complete(ino,new_size) == extent_protocol::OK){
		lc->release(ino);
		return true;
	}
	else{
		lc->release(ino);
		return false;
	}
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
	pthread_mutex_lock(&mutex_list);
	cout<<"in NameNode::AppendBlock"<<endl;//just test
	cout<<"the inum is : "<<(int)ino<<endl;//just test
	fflush(stdout);//just test
	blockid_t bid;
	extent_protocol::attr a;
	ec->getattr(ino,a);
	int offset = a.size%BLOCK_SIZE==0?(a.size/BLOCK_SIZE-1)*BLOCK_SIZE:(a.size/BLOCK_SIZE)*BLOCK_SIZE;
	int res = ec->append_block(ino,bid);
	if(res == extent_protocol::OK){
		blockToRep.push_back(bid);
		pthread_mutex_unlock(&mutex_list);
		return LocatedBlock(bid,offset,(a.size%16384)==0?16384:(a.size%16384),GetDatanodes());
	}
	cout<<"in NameNode return is not ok?"<<endl;
	fflush(stdout);//just test
	pthread_mutex_unlock(&mutex_list);
 	throw HdfsException("Not implemented");
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
	cout<<"in NameNode::Rename"<<endl;//just test
	cout<<"the src_dir_ino : "<<src_dir_ino<<endl;//just test
	cout<<"the dst_dir_ino : "<<dst_dir_ino<<endl;//just test
	fflush(stdout);//just test
	if(!yfs->isdir(src_dir_ino) || !yfs->isdir(dst_dir_ino))
		return false;
	return yfs->rename(src_dir_ino,src_name,dst_dir_ino,dst_name);
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
	cout<<"in NameNode::Mkdir"<<endl;//just test
	cout<<"parent : "<<parent<<" name : "<<name<<endl;//just test
	fflush(stdout);//just test
	char *charName = (char *)name.data();
	if(yfs->_mkdir(parent,charName,mode,ino_out) == yfs_client::OK){
		return true;
	}
	cout<<"in Mkdir return is not ok?"<<endl;
 	return false;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
	cout<<"in NameNode::Create"<<endl;//just test
	cout<<"parent : "<<parent<<" name : "<<name<<endl;//just test
	fflush(stdout);//just test
	char *charName = (char *)name.data();
	if(yfs->_create(parent,charName,mode,ino_out) == yfs_client::OK){
		cout<<"the yfs_client::OK is : "<<yfs_client::OK<<endl;//just test
		lc->acquire(ino_out);
		return true;
	}
	cout<<"in Create return is not ok?"<<endl;//just test
	fflush(stdout);//just test
  	return false;
}

bool NameNode::Isfile(yfs_client::inum ino) {
	cout<<"in NameNode::Isfile, inum : "<<ino<<endl;//just test
	fflush(stdout);//just test
 	return yfs->isfile(ino);
}

bool NameNode::Isdir(yfs_client::inum ino) {
	cout<<"in NameNode::Isdir, inum : "<<ino<<endl;//just test
	fflush(stdout);//just test
	return yfs->isdir(ino);
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
	cout<<"in NameNode::Getfile"<<endl;//just test
	cout<<"the inum is : "<<(int)ino<<endl;//just test
	fflush(stdout);//just test
	return !yfs->getfile(ino,info);
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
	cout<<"in NameNode::Getdir, inum : "<<ino<<endl;//just test
	fflush(stdout);//just test
	return !yfs->getdir(ino,info);
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
	cout<<"in NameNode::Readdir, inum : "<<ino<<endl;//just test
	fflush(stdout);//just test
	return !yfs->readdir(ino,dir);
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
	cout<<"in NameNode::Unlink"<<endl;//just test
	cout<<"the parent : "<<parent<<" the name : "<<name<<endl;//just test
	fflush(stdout);//just test
	int res = !yfs->_unlink(parent,(char*)name.data());
	return res;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
	pthread_mutex_lock(&mutex_map);
	cout<<"in NameNode::DatanodeHeartbeat"<<endl;//just test
	printf("the id is : %lx\n",(unsigned long)&id);//just test
	fflush(stdout);//just test
	DatanodeInManage res = liveDatanode[id];
	switch(res.s){
		case LIVING: case REGISTER:{
			printf("in living or register\n");//just test
			fflush(stdout);//just test
			liveDatanode[id].t = time((time_t*)NULL);
			break;
		}
		case DEATH:{
			printf("its already dead!\n");//just test
			fflush(stdout);//just test
			liveDatanode[id].s = REGISTER;
			liveDatanode[id].t = time((time_t *)NULL);
			pthread_mutex_unlock(&mutex_map);
			NewThread(this,&NameNode::Monitor,id);
			NewThread(this,&NameNode::Replicate,id);
			return;
		}
	}
	pthread_mutex_unlock(&mutex_map);
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
	pthread_mutex_lock(&mutex_map);
/*	if(liveDatanode.find(id) != liveDatanode.end()){
		printf("hahahahahahhahaha?????\n");//just test
		fflush(stdout);//just test
		pthread_mutex_unlock(&mutex_map);
		return;
	}*/
	cout<<"in NameNode::RegisterDatanode"<<endl;//just test
	printf("the id is : %lx\n",(unsigned long)&id);//just test
	fflush(stdout);//just test
	liveDatanode[id] = DatanodeInManage(REGISTER,time((time_t *)NULL));
	pthread_mutex_unlock(&mutex_map);
	NewThread(this,&NameNode::Monitor,id);
	NewThread(this,&NameNode::Replicate,id);
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
	pthread_mutex_lock(&mutex_map);//just test
	cout<<"in NameNode::GetDatanodes"<<endl;//just test
	fflush(stdout);//just test
	list<DatanodeIDProto> res;
	map<DatanodeIDProto,DatanodeInManage>::iterator itor;
	itor = liveDatanode.begin();
	while(itor != liveDatanode.end()){
		if((itor->second).s == LIVING){
			res.push_back(itor->first);
		}
		itor++;
	}
	pthread_mutex_unlock(&mutex_map);//just test
	return res;
}

void NameNode::Monitor(DatanodeIDProto id){	
	pthread_mutex_lock(&mutex_map);
	cout<<"in NameNode::Monitor"<<endl;//just test
	printf("the id is : %lx\n",(unsigned long)&id);//just test
	fflush(stdout);//just test
	
	DatanodeInManage inMo = liveDatanode[id];
	while(true){
		time_t now = time((time_t *)NULL);
		if((now - inMo.t) >= 4){
			liveDatanode[id].s = DEATH;
			printf("dead!\n");//just test
			fflush(stdout);//just test
			pthread_mutex_unlock(&mutex_map);
			return;
		}
		printf("in monitor living\n");//just test
		fflush(stdout);//just test
		pthread_mutex_unlock(&mutex_map);
		sleep(1);
		pthread_mutex_lock(&mutex_map);
	}
}

void NameNode::Replicate(DatanodeIDProto id){	
	pthread_mutex_lock(&mutex_list);
	cout<<"in NameNode::Replicate"<<endl;//just test
	printf("the id is : %lx\n",(unsigned long)&id);//just test
	fflush(stdout);//just test

	list<blockid_t>::iterator itor;
	itor = blockToRep.begin();
	while(itor != blockToRep.end()){
		blockid_t bid = *itor;
		ReplicateBlock(bid,master_datanode,id);
		itor++;
		printf("in replicate while\n");//just test
		fflush(stdout);//just test
	}	
	printf("after Replicate\n");//just test
	fflush(stdout);//just test
	pthread_mutex_lock(&mutex_map);
	liveDatanode[id].s = LIVING;
	pthread_mutex_unlock(&mutex_map);
	pthread_mutex_unlock(&mutex_list);
}


