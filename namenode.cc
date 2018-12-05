#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
	cout<<"in NameNode::init"<<endl;//just test
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);
	cout<<"finish NameNode::init"<<endl;//just test
  /* Add your init logic here */
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
	cout<<"in NameNode::GetBlockLocations"<<endl;//just test
  return list<LocatedBlock>();
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
	cout<<"in NameNode::Complete"<<endl;//just test
	if(ec->complete(ino,new_size) == extent_protocol::OK)
		return true;
	else
		return false;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
	cout<<"in NameNode::AppendBlock"<<endl;//just test
	blockid_t bid;
	if(ec->append_block(ino,bid) == extent_protocol::OK){
		return LocatedBlock(bid,0,BLOCK_SIZE,master_datanode);
	}
	cout<<"in NameNode return is not ok?"<<endl;
  throw HdfsException("Not implemented");
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
	cout<<"in NameNode::Rename"<<endl;//just test
	yfs_client::inum dst_ino;
	yfs_client::inum src_ino;
	extent_protocol::attr a;

	mode_t mode;
	bool found;
	yfs->lookup(src_dir_ino,(char *)src_name.data(),found,src_ino);
	if(found == false)
		return false;
	ec->getattr(src_ino,a);
	unsigned int src_size = a.size;
	yfs->create(dst_dir_ino,(char*)dst_name.data(),mode,dst_ino);
	
	string data;
	size_t bytes_written;
	off_t m = 0;

	for(unsigned int i = src_size; i >= 0 ; i -= BLOCK_SIZE, m += BLOCK_SIZE){
		int actual_size;
		if(i <= BLOCK_SIZE)
			actual_size = BLOCK_SIZE;
		else
			actual_size = i;
		yfs->read(src_ino,actual_size,m,data);
		yfs->write(dst_ino,actual_size,m,(char*)data.data(),bytes_written);
		m += BLOCK_SIZE;
	}
	yfs->unlink(src_dir_ino,(char*)src_name.data());
	return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
	cout<<"in NameNode::Mkdir"<<endl;//just test
	char *charName = (char *)name.data();
	if(yfs->mkdir(parent,charName,mode,ino_out) == yfs_client::OK)
		return true;
	cout<<"in Mkdir return is not ok?"<<endl;
  return false;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
	cout<<"in NameNode::Create"<<endl;//just test
	char *charName = (char *)name.data();
	if(yfs->create(parent,charName,mode,ino_out) == yfs_client::OK)
		return true;
	cout<<"in Create return is not ok?"<<endl;
  return false;
}

bool NameNode::Isfile(yfs_client::inum ino) {
	cout<<"in NameNode::Isfile"<<endl;//just test
  return yfs->isfile(ino);
}

bool NameNode::Isdir(yfs_client::inum ino) {
	cout<<"in NameNode::Isdir"<<endl;//just test
  return yfs->isdir(ino);
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
	cout<<"in NameNode::Getfile"<<endl;//just test
	cout<<"the inum is : "<<(int)ino<<endl;//just test
  return !yfs->getfile(ino,info);
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
	cout<<"in NameNode::Getdir"<<endl;//just test
  return !yfs->getdir(ino,info);
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
	cout<<"in NameNode::Readdir"<<endl;//just test
  return !yfs->readdir(ino,dir);
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
	cout<<"in NameNode::Unlink"<<endl;//just test
  return !yfs->unlink(parent,(char*)name.data());
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
	cout<<"in NameNode::DatanodeHeartbeat"<<endl;//just test
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
	cout<<"in NameNode::RegisterDatanode"<<endl;//just test
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
	cout<<"in NameNode::GetDatanodes"<<endl;//just test
  return list<DatanodeIDProto>();
}
