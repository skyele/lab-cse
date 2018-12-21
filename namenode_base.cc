#include <sys/socket.h>
#include <stdio.h>
#include <errno.h>
#include <netinet/ip.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <namenode.pb.h>
#include "namenode.h"
#include "hrpc.h"
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>

using namespace std;
using namespace google::protobuf::io;

NameNode namenode;

int ReadHeader(int fd) {
    fprintf(stderr, "in namenode_base.cc::ReadHeader\n");//just test
    fflush(stderr);//just test

  char buf[7];

  if (read(fd, buf, sizeof(buf)) != sizeof(buf)) {
    fprintf(stderr, "%s:%d Read header failed: %s\n", __func__, __LINE__, strerror(errno));
    fflush(stderr);
    return -1;
  }
  if (buf[0] != 'h' || buf[1] != 'r' || buf[2] != 'p' || buf[3] != 'c') {
    fprintf(stderr, "%s:%d Bad header\n", __func__, __LINE__);
    fflush(stderr);
    return -1;
  }
  if (buf[4] != 9) {
    fprintf(stderr, "%s:%d Unsupported version %d\n", __func__, __LINE__, buf[4]);
    fflush(stderr);
    return -1;
  }
  if (buf[5] != 0) {
    fprintf(stderr, "%s:%d Unsupported service class %d\n", __func__, __LINE__, buf[5]);
    fflush(stderr);
    return -1;
  }
  if (buf[6] != 0) {
    fprintf(stderr, "%s:%d Unsupported auth protocol %d\n", __func__, __LINE__, buf[6]);
    fflush(stderr);
    return -1;
  }
  return 0;
}

int HandleRpc(int fd) {
  RpcRequestHeaderProto rpc_header;
  RequestHeaderProto header;
  string buf;
  if (!HrpcRead(fd, rpc_header, buf)) {
    fprintf(stderr, "%s:%d read rpc request failed\n", __func__, __LINE__);
    fflush(stderr);
    return -1;
  }
  if (rpc_header.callid() < 0)
    return 0;
  if (ReadDelimited(buf, header) != 0) {
    fprintf(stderr, "%s:%d read request header failed\n", __func__, __LINE__);
    fflush(stderr);
    return -1;
  }
#define TRY(name) \
  if (strcasecmp(header.methodname().c_str(), #name) == 0) { \
    name##RequestProto req; \
    if (ReadDelimited(buf, req) != 0) { \
      fprintf(stderr, "%s:%d %s read request param failed\n", __func__, __LINE__, #name); \
      fflush(stderr); \
      return -1; \
    } \
    name##ResponseProto resp; \
    try { \
      namenode.PB##name(req, resp); \
      RpcResponseHeaderProto rpc_resp; \
      rpc_resp.set_callid(rpc_header.callid()); \
      rpc_resp.set_status(RpcResponseHeaderProto_RpcStatusProto_SUCCESS); \
      rpc_resp.set_serveripcversionnum(9); \
      rpc_resp.set_clientid(rpc_header.clientid()); \
      rpc_resp.set_retrycount(rpc_header.retrycount()); \
      HrpcWrite(fd, rpc_resp, resp); \
    } catch (HdfsException &e) { \
      RpcResponseHeaderProto rpc_resp; \
      rpc_resp.set_callid(rpc_header.callid()); \
      rpc_resp.set_status(RpcResponseHeaderProto_RpcStatusProto_ERROR); \
      rpc_resp.set_serveripcversionnum(9); \
      rpc_resp.set_clientid(rpc_header.clientid()); \
      rpc_resp.set_retrycount(rpc_header.retrycount()); \
      rpc_resp.set_errormsg(e.what()); \
      HrpcWrite(fd, rpc_resp); \
    } \
    return 0; \
  }
  TRY(GetFileInfo)
  TRY(GetListing)
  TRY(GetBlockLocations)
  TRY(RegisterDatanode)
  TRY(GetServerDefaults)
  TRY(Create)
  TRY(Complete)
  TRY(AddBlock)
  TRY(RenewLease)
  TRY(Rename)
  TRY(Delete)
  TRY(Mkdirs)
  TRY(GetFsStats)
  TRY(SetSafeMode)
  TRY(GetDatanodeReport)
  TRY(DatanodeHeartbeat)
  fprintf(stderr, "%s:%d Unknown namenode method %s\n", __func__, __LINE__, header.methodname().c_str());
  fflush(stderr);
  RpcResponseHeaderProto rpc_resp;
  rpc_resp.set_callid(rpc_header.callid());
  rpc_resp.set_status(RpcResponseHeaderProto_RpcStatusProto_ERROR);
  rpc_resp.set_serveripcversionnum(9);
  rpc_resp.set_clientid(rpc_header.clientid());
  rpc_resp.set_retrycount(rpc_header.retrycount());
  rpc_resp.set_errordetail(RpcResponseHeaderProto_RpcErrorCodeProto_ERROR_NO_SUCH_METHOD);
  rpc_resp.set_errormsg("Method not supported");
  HrpcWrite(fd, rpc_resp);

  return -1;
}

void *worker(void *_arg) {
  int clientfd = (int) (uintptr_t) _arg;

  if (ReadHeader(clientfd) != 0) {
	printf("worker error1\n");///just test
	fflush(stdout);//just test
    close(clientfd);
    return NULL;
  }

  for (;;) {
    if (HandleRpc(clientfd) != 0)
      break;
  }
	printf("worker error2\n");///just test
	fflush(stdout);//just test

  close(clientfd);
  return NULL;
}

bool NameNode::RecursiveLookup(const string &path, yfs_client::inum &ino, yfs_client::inum &last) {
    fprintf(stderr, "in namenode_base.cc::RecursiveLookup1\n");//just test
    fflush(stderr);//just test

  size_t pos = 1, lastpos = 1;
  bool found;
  if (path[0] != '/') {
    fprintf(stderr, "%s:%d Only absolute path allowed\n", __func__, __LINE__); fflush(stderr);	
	printf("recursivelookup error1\n");///just test
	fflush(stdout);//just test

    return false;
  }
  last = 1;
  ino = 1;
  while ((pos = path.find('/', pos)) != string::npos) {
    if (pos != lastpos) {
      string component = path.substr(lastpos, pos - lastpos);
      last = ino;
      if (yfs->lookup(ino, component.c_str(), found, ino) != yfs_client::OK || !found) {
        fprintf(stderr, "%s:%d Lookup %s in %llu failed and yfs_client::OK is : %d\n", __func__, __LINE__, component.c_str(), ino,yfs_client::OK); //just test
        //fprintf(stderr, "%s:%d Lookup %s in %llu failed\n", __func__, __LINE__, component.c_str(), ino); 
	fflush(stderr);
	printf("recursivelookup error2\n");///just test
	fflush(stdout);//just test

        return false;
      }
    }
    pos++;
    lastpos = pos;
  }

  if (lastpos != path.size()) {
    last = ino;
    string component = path.substr(lastpos);
    if (yfs->lookup(ino, component.c_str(), found, ino) != yfs_client::OK || !found) {
      fprintf(stderr, "%s:%d Lookup %s in %llu failed\n", __func__, __LINE__, component.c_str(), ino); fflush(stderr);
	printf("recursivelookup error3\n");///just test
	fflush(stdout);//just test

      return false;
    }
  }

  return true;
}

bool NameNode::RecursiveLookup(const string &path, yfs_client::inum &ino) {
    fprintf(stderr, "in namenode_base.cc::RecursiveLookup2\n");//just test
    fflush(stderr);//just test

  yfs_client::inum last;
  return RecursiveLookup(path, ino, last);
}

bool NameNode::RecursiveLookupParent(const string &path, yfs_client::inum &ino) {
    fprintf(stderr, "in namenode_base.cc::RecursiveLookupParent\n");//just test
    fflush(stderr);//just test

  string _path = path;
  while (_path.size() != 0 && _path[_path.size() - 1] == '/')
    _path.resize(_path.size() - 1);
  if (_path.rfind('/') != string::npos)
    _path = _path.substr(0, _path.rfind('/') + 1);
  return RecursiveLookup(_path, ino);
}

bool operator<(const DatanodeIDProto &a, const DatanodeIDProto &b) {
  if (a.ipaddr() < b.ipaddr())
    return true;
  if (a.ipaddr() > b.ipaddr())
    return false;
  if (a.hostname() < b.hostname())
    return true;
  if (a.hostname() > b.hostname())
    return false;
  if (a.xferport() < b.xferport())
    return true;
  if (a.xferport() > b.xferport())
    return false;
  return false;
}

bool operator==(const DatanodeIDProto &a, const DatanodeIDProto &b) {

  if (a.ipaddr() != b.ipaddr())
    return false;
  if (a.hostname() != b.hostname())
    return false;
  if (a.xferport() != b.xferport())
    return false;
  return true;
}

bool NameNode::ReplicateBlock(blockid_t bid, DatanodeIDProto from, DatanodeIDProto to) {
    fprintf(stderr, "in namenode_base.cc::ReplicateBlock\n");//just test
    fflush(stderr);//just test

  unique_ptr<FileInputStream> pfis;
  unique_ptr<FileOutputStream> pfos;
  if (!Connect(from.ipaddr(), from.xferport(), pfis, pfos)) {
    fprintf(stderr, "%s:%d Connect to %s:%d failed\n", __func__, __LINE__, from.hostname().c_str(), from.xferport()); fflush(stderr);
	printf("replicateblock error1\n");///just test
	fflush(stdout);//just test

    return false;
  }
  CodedOutputStream cos(&*pfos);
  uint8_t hdr[3] = { 0, 28, 86 };
  OpTransferBlockProto req;
  req.mutable_header()->mutable_baseheader()->mutable_block()->set_poolid("yfs");
  req.mutable_header()->mutable_baseheader()->mutable_block()->set_blockid(bid);
  req.mutable_header()->mutable_baseheader()->mutable_block()->set_generationstamp(0);
  req.mutable_header()->mutable_baseheader()->mutable_block()->set_numbytes(BLOCK_SIZE);
  req.mutable_header()->set_clientname("");
  req.add_targets()->mutable_id()->CopyFrom(to);
  req.add_targetstoragetypes(RAM_DISK);
  cos.WriteRaw(hdr, sizeof(hdr));
  cos.WriteVarint32(req.ByteSize());
  req.SerializeWithCachedSizes(&cos);
  cos.Trim();
  pfos->Flush();
  if (cos.HadError()) {
    fprintf(stderr, "%s:%d send transfer request failed\n", __func__, __LINE__); fflush(stderr);
	printf("replicateblock error2\n");///just test
	fflush(stdout);//just test

    return false;
  }

  BlockOpResponseProto resp;
  CodedInputStream cis(&*pfis);
  auto limit = cis.ReadLengthAndPushLimit();
  if (!resp.ParseFromCodedStream(&cis)) {
    fprintf(stderr, "%s:%d bad response\n", __func__, __LINE__); fflush(stderr);
	printf("replicateblock error3\n");///just test
	fflush(stdout);//just test

    return false;
  }
  if (!cis.CheckEntireMessageConsumedAndPopLimit(limit)) {
    fprintf(stderr, "%s:%d bad response\n", __func__, __LINE__); fflush(stderr);
	printf("replicateblock error4\n");///just test
	fflush(stdout);//just test

    return false;
  }
  if (resp.status() != SUCCESS) {
    fprintf(stderr, "%s:%d transfer block from %s to %s failed\n", __func__, __LINE__, from.hostname().c_str(), to.hostname().c_str()); fflush(stderr);
	printf("replicateblock error5\n");///just test
	fflush(stdout);//just test

    return false;
  }
  return true;
}

// Translators

bool NameNode::PBGetFileInfoFromInum(yfs_client::inum ino, HdfsFileStatusProto &info) {
    fprintf(stderr, "in namenode_base.cc::PBGetFileInfoFromInum\n");//just test
    fflush(stderr);//just test

  info.set_filetype(HdfsFileStatusProto_FileType_IS_FILE);
  info.set_path("");
  info.set_length(0);
  info.set_owner("cse");
  info.set_group("supergroup");
  info.set_blocksize(BLOCK_SIZE);
  if (Isfile(ino)) {
    yfs_client::fileinfo yfs_info;
    if (!Getfile(ino, yfs_info)) {
      fprintf(stderr, "%s:%d Getfile(%llu) failed\n", __func__, __LINE__, ino); fflush(stderr);
	printf("pbgetfileinfofrominum error1\n");///just test
	fflush(stdout);//just test

      return false;
    }
    info.set_length(yfs_info.size);
    info.mutable_permission()->set_perm(0666);
    info.set_modification_time(((uint64_t) yfs_info.mtime) * 1000);
    info.set_access_time(((uint64_t) yfs_info.atime) * 1000);
    return true;
  } else if (Isdir(ino)) {
    yfs_client::dirinfo yfs_info;
    if (!Getdir(ino, yfs_info)) {
      fprintf(stderr, "%s:%d Getdir(%llu) failed\n", __func__, __LINE__, ino); fflush(stderr);	
	printf("pbgetfileinfofrominum error2\n");///just test
	fflush(stdout);//just test

      return false;
    }
    info.set_filetype(HdfsFileStatusProto_FileType_IS_DIR);
    info.mutable_permission()->set_perm(0777);
    info.set_modification_time(((uint64_t) yfs_info.mtime) * 1000);
    info.set_access_time(((uint64_t) yfs_info.atime) * 1000);
    return true;
  }

  return false;
}

void NameNode::PBGetFileInfo(const GetFileInfoRequestProto &req, GetFileInfoResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBGetFileInfo\n");//just test
    fflush(stderr);//just test

  yfs_client::inum ino;
  if (!RecursiveLookup(req.src(), ino)){
	printf("pbgetfileinfo error2\n");///just test
	fflush(stdout);//just test

    return;
  }
  if (!PBGetFileInfoFromInum(ino, *resp.mutable_fs())){
	printf("pbgetfileinfo error2\n");///just test
	fflush(stdout);//just test

    resp.clear_fs();
  }
}

void NameNode::PBGetListing(const GetListingRequestProto &req, GetListingResponseProto &resp) { 
    fprintf(stderr, "in namenode_base.cc::PBGetListing\n");//just test
    fflush(stderr);//just test

  yfs_client::inum ino;
  if (!RecursiveLookup(req.src(), ino)){
	printf("pbgetlisting error1\n");///just test
	fflush(stdout);//just test

    return;
  }
  string start_after(req.startafter());
  list<yfs_client::dirent> dir;
  if (yfs->readdir(ino, dir) != yfs_client::OK){
	printf("pbgetlisting error2\n");///just test
	fflush(stdout);//just test

    throw HdfsException("read directory failed");
  }
  auto it = dir.begin();
  if (start_after.size() != 0) {
    while (it != dir.end() && it->name != start_after)
      it++;
  }
  for (; it != dir.end(); it++) {
    if (!PBGetFileInfoFromInum(it->inum, *resp.mutable_dirlist()->add_partiallisting())){
	printf("pbgetlisting error3\n");///just test
	fflush(stdout);//just test

      throw HdfsException("get dirent info failed");
    }
    resp.mutable_dirlist()->mutable_partiallisting()->rbegin()->set_path(it->name);
    if (req.needlocation() && yfs->isfile(it->inum)) {
      yfs_client::fileinfo info;
      if (yfs->getfile(it->inum, info) != yfs_client::OK)
        return;
      list<LocatedBlock> blocks = GetBlockLocations(it->inum);
      LocatedBlocksProto &locations = *resp.mutable_dirlist()->mutable_partiallisting()->rbegin()->mutable_locations();
      locations.set_filelength(info.size);
      locations.set_underconstruction(false);
      locations.set_islastblockcomplete(true);
      int i = 0;
      for (auto it = blocks.begin(); it != blocks.end(); it++) {
        LocatedBlockProto *block = locations.add_blocks();
        if (!ConvertLocatedBlock(*it, *block)){
		printf("pbgetlisting error4\n");///just test
		fflush(stdout);//just test

          throw HdfsException("Failed to convert located block");
   	}
        if (next(it) == blocks.end())
          locations.mutable_lastblock()->CopyFrom(*block);
        i++;
      }
    }
  }
  resp.mutable_dirlist()->set_remainingentries(0);
}

bool NameNode::ConvertLocatedBlock(const LocatedBlock &src, LocatedBlockProto &dst) {
    fprintf(stderr, "in namenode_base.cc::ConvertLocatedBlock\n");//just test
    fflush(stderr);//just test

  dst.mutable_b()->set_poolid("yfs");
  dst.mutable_b()->set_blockid(src.block_id);
  dst.mutable_b()->set_generationstamp(0);
  dst.mutable_b()->set_numbytes(src.size);
  dst.mutable_blocktoken()->set_identifier("");
  dst.mutable_blocktoken()->set_password("");
  dst.mutable_blocktoken()->set_kind("");
  dst.mutable_blocktoken()->set_service("");
  dst.set_offset(src.offset);
  for (auto it = src.locs.begin(); it != src.locs.end(); it++) {
    DatanodeInfoProto *loc = dst.add_locs();
    loc->mutable_id()->CopyFrom(*it);
    loc->set_location("/default-rack");
    dst.add_iscached(false);
    dst.add_storagetypes(StorageTypeProto::RAM_DISK);
    dst.add_storageids(it->hostname());
  }
  dst.set_corrupt(false);
  dst.mutable_blocktoken();
  return true;
}

void NameNode::PBGetBlockLocations(const GetBlockLocationsRequestProto &req, GetBlockLocationsResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBGetBlockLocations\n");//just test
    fflush(stderr);//just test

  yfs_client::inum ino;
  if (!RecursiveLookup(req.src(), ino)){
	printf("pbgetblocklocations error1\n");///just test
	fflush(stdout);//just test

    return;
  }
  if (!yfs->isfile(ino)){
	printf("pbgetblocklocations error2\n");///just test
	fflush(stdout);//just test

    return;
  }
  yfs_client::fileinfo info;
  if (yfs->getfile(ino, info) != yfs_client::OK){
	printf("pbgetblocklocations error3\n");///just test
	fflush(stdout);//just test

    return;
   }
  list<LocatedBlock> blocks = GetBlockLocations(ino);
  LocatedBlocksProto &locations = *resp.mutable_locations();
  locations.set_filelength(info.size);
  locations.set_underconstruction(false);
  locations.set_islastblockcomplete(true);
  int i = 0;
  for (auto it = blocks.begin(); it != blocks.end(); it++) {
    LocatedBlockProto *block = locations.add_blocks();
    if (!ConvertLocatedBlock(*it, *block)){
	printf("pbgetblocklocations error4\n");///just test
	fflush(stdout);//just test

      throw HdfsException("Failed to convert located block");
    }
    if (next(it) == blocks.end())
      locations.mutable_lastblock()->CopyFrom(*block);
    i++;
  }
}

struct DatanodeRegistration {
  NameNode *namenode;
  DatanodeIDProto id;
};

void *NameNode::_RegisterDatanode(void *arg) {
    fprintf(stderr, "in namenode_base.cc::_RegisterDatanode\n");//just test
    fflush(stderr);//just test

  DatanodeRegistration *registration = (DatanodeRegistration *) arg;
  registration->namenode->RegisterDatanode(registration->id);
  delete registration;
  return NULL;
}

void NameNode::PBRegisterDatanode(const RegisterDatanodeRequestProto &req, RegisterDatanodeResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBRegisterDatanode\n");//just test
    fflush(stderr);//just test

  if (req.registration().master())
    master_datanode = req.registration().datanodeid();
  pthread_t thread;
  DatanodeRegistration *registration = new DatanodeRegistration();
  registration->namenode = this;
  registration->id.CopyFrom(req.registration().datanodeid());
  if (pthread_create(&thread, NULL, NameNode::_RegisterDatanode, registration) != 0){
	printf("pbregisterdatanode error1\n");///just test
	fflush(stdout);//just test

    throw HdfsException("Failed to register datanode");
  }
  resp.mutable_registration()->CopyFrom(req.registration());
}

void NameNode::PBGetServerDefaults(const GetServerDefaultsRequestProto &req, GetServerDefaultsResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBGetServerDefaults\n");//just test
    fflush(stderr);//just test

  FsServerDefaultsProto &defaults = *resp.mutable_serverdefaults();
  defaults.set_blocksize(BLOCK_SIZE);
  defaults.set_bytesperchecksum(1);
  defaults.set_writepacketsize(BLOCK_SIZE);
  defaults.set_replication(1);
  defaults.set_filebuffersize(4096);
  defaults.set_checksumtype(CHECKSUM_NULL);
}

void NameNode::PBCreate(const CreateRequestProto &req, CreateResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBCreate\n");//just test
    fflush(stderr);//just test

  size_t pos = 1, lastpos = 1;
  bool found;
  string path = req.src();
  if (path[0] != '/'){
	cout<<"create error 1"<<endl;//just test
    throw HdfsException("Not absolute path");
  }
	cout<<"in pbcreate 1"<<endl;//just test
  yfs_client::inum ino = 1;
  while ((pos = path.find('/', pos)) != string::npos) {
    if (pos != lastpos) {
      string component = path.substr(lastpos, pos - lastpos);
      if (yfs->lookup(ino, component.c_str(), found, ino) != yfs_client::OK){
	cout<<"create error 2"<<endl;//just test
        throw HdfsException("Traverse failed");
    }
      if (!found) {
        if (req.createparent()) {
          if (!Mkdir(ino, component.c_str(), 0777, ino)){
		cout<<"create error 3"<<endl;//just test
            throw HdfsException("Create parent failed");
 	 }
        } else{
		cout<<"create error 4"<<endl;//just test
          throw HdfsException("Parent not exists");
 	}
      }
    }
    pos++;
    lastpos = pos;
  }

	cout<<"in pbcreate 2"<<endl;//just test
  if (lastpos != path.size()) {
    string component = path.substr(lastpos);
    if (!Create(ino, component.c_str(), 0666, ino)){
		cout<<"create error 5"<<endl;//just test
      throw HdfsException("Create file failed");
    }
    pendingWrite.insert(make_pair(ino, 0));
    if (!PBGetFileInfoFromInum(ino, *resp.mutable_fs())){
		cout<<"create error 6"<<endl;//just test
      resp.clear_fs();
	}
  } else{
		cout<<"create error 7"<<endl;//just test
    throw HdfsException("No file name given");
 }
	cout<<"in pbcreate 3"<<endl;//just test
}

void NameNode::PBComplete(const CompleteRequestProto &req, CompleteResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBComplete\n");//just test
    fflush(stderr);//just test

  yfs_client::inum ino;
  if (!RecursiveLookup(req.src(), ino)) {
	printf("pbcomplete error1\n");//just test
	fflush(stdout);//just test
    resp.set_result(false);
    return;
  }
  if (pendingWrite.count(ino) == 0){
	printf("pbcomplete error2\n");//just test
	fflush(stdout);//just test

    throw HdfsException("No such pending write");
  }
  if (req.has_last()) {
    pendingWrite[ino] -= BLOCK_SIZE;
    pendingWrite[ino] += req.last().numbytes();
  }
  uint32_t new_size = pendingWrite[ino];
  pendingWrite.erase(ino);
  bool r = Complete(ino, new_size);
  if (!r) {
    pendingWrite.insert(make_pair(ino, new_size));
	printf("pbcomplete error3\n");//just test
	fflush(stdout);//just test

    throw HdfsException("Complete pending write failed");
  }
	printf("pbcomplete finish!!\n");//just test
	fflush(stdout);//just test
  resp.set_result(r);
}

void NameNode::PBAddBlock(const AddBlockRequestProto &req, AddBlockResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBAddBlock\n");//just test
    fflush(stderr);//just test

  yfs_client::inum ino;
  if (!RecursiveLookup(req.src(), ino)){
	printf("addBlock error1\n");//just test
	fflush(stdout);
    return;
  }
  if (pendingWrite.count(ino) == 0){
	printf("addBlock error2\n");//just test
	fflush(stdout);
    throw HdfsException("File not locked");
  }
  LocatedBlock new_block = AppendBlock(ino);
  if (!ConvertLocatedBlock(new_block, *resp.mutable_block())){
	printf("addBlock error3\n");//just test
	fflush(stdout);
    throw HdfsException("Convert LocatedBlock failed");
  }
	printf("addBlock finish\n");//just test
	fflush(stdout);
  pendingWrite[ino] += BLOCK_SIZE;
}

void NameNode::PBRenewLease(const RenewLeaseRequestProto &req, RenewLeaseResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBRenewLease\n");//just test
    fflush(stderr);//just test

}

void NameNode::PBRename(const RenameRequestProto &req, RenameResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBRename\n");//just test
    fflush(stderr);//just test

  yfs_client::inum src_dir_ino, dst_dir_ino;
  string src_name, dst_name;
  if (!RecursiveLookupParent(req.src(), src_dir_ino)) {
	printf("rename error1\n");///just test
	fflush(stdout);//just test

    resp.set_result(false);
    return;
  }
  if (!RecursiveLookupParent(req.dst(), dst_dir_ino)) {
	printf("rename error2\n");///just test
	fflush(stdout);//just test

    resp.set_result(false);
    return;
  }
  src_name = req.src().substr(req.src().rfind('/') + 1);
  dst_name = req.dst().substr(req.dst().rfind('/') + 1);
  resp.set_result(Rename(src_dir_ino, src_name, dst_dir_ino, dst_name));
}

void NameNode::DualLock(lock_protocol::lockid_t a, lock_protocol::lockid_t b) { 
    fprintf(stderr, "in namenode_base.cc::DualLock\n");//just test
    fflush(stderr);//just test
    cout<<"the a : "<< a << " the b : "<<b<<endl;//just test
    fflush(stdout);//just test

  if (a < b) {
    lc->acquire(a);
    lc->acquire(b);
  } else if (a > b) {
    lc->acquire(b);
	cout<<"finish acquire "<<b<<endl;//just test
    lc->acquire(a);
	cout<<"finish acquire "<<a<<endl;//just test
  } else
    lc->acquire(a);
   cout<<"finish DualLock"<<endl;//just test
   fflush(stdout);//just test
}

void NameNode::DualUnlock(lock_protocol::lockid_t a, lock_protocol::lockid_t b) {
    fprintf(stderr, "in namenode_base.cc::DualUnlock\n");//just test
    fflush(stderr);//just test

  if (a == b)
    lc->release(a);
  else {
    lc->release(a);
    lc->release(b);
  }
}

bool NameNode::RecursiveDelete(yfs_client::inum ino) {
    fprintf(stderr, "in namenode_base.cc::RecursiveDelete\n");//just test
    fflush(stderr);//just test

  list<yfs_client::dirent> dir;
  if (!Isdir(ino)){
	printf("recursivedelete error1\n");///just test
	fflush(stdout);//just test

    return true;
  }
  if (!Readdir(ino, dir)) {
    fprintf(stderr, "%s:%d Readdir(%llu) failed\n", __func__, __LINE__, ino); fflush(stderr);
	printf("recursivedelete error2\n");///just test
	fflush(stdout);//just test

    return false;
  }
  for (auto it = dir.begin(); it != dir.end(); it++) {
    lc->acquire(it->inum);
    if (!RecursiveDelete(it->inum)) {
      lc->release(it->inum);
	printf("recursivedelete error3\n");///just test
	fflush(stdout);//just test

      fprintf(stderr, "%s:%d recursively delete %s failed\n", __func__, __LINE__, it->name.c_str()); fflush(stderr);
      return false;
    }
    if (!Unlink(ino, it->name, it->inum)) {
      lc->release(it->inum);
	printf("recursivedelete error4\n");///just test
	fflush(stdout);//just test

      fprintf(stderr, "%s:%d delete %s failed\n", __func__, __LINE__, it->name.c_str()); fflush(stderr);
      return false;
    }
    lc->release(it->inum);
  }
  return true;
}

void NameNode::PBDelete(const DeleteRequestProto &req, DeleteResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBDelete\n");//just test
    fflush(stderr);//just test

  yfs_client::inum ino, parent;
  if (!RecursiveLookup(req.src(), ino, parent)) {
    resp.set_result(false);
	printf("delete error1\n");///just test
	fflush(stdout);//just test

    return;
  }
  cout<<"the ino : "<<ino<<" the parent : "<<parent<<endl;//just test
  fflush(stdout);//just test
  DualLock(ino, parent);
  if (req.recursive()) {
    if (!RecursiveDelete(ino)) {
      resp.set_result(false);
      DualUnlock(ino, parent);
	printf("delete error2\n");///just test
      fflush(stdout);//just test
;
      return;
    }
  }
  if (Isdir(ino)) {
    list<yfs_client::dirent> dir;
    if (!Readdir(ino, dir)) {
      resp.set_result(false);
      DualUnlock(ino, parent);
	printf("delete error3\n");///just test
      fflush(stdout);//just test

      return;
    }
    if (dir.size() != 0) {
      resp.set_result(false);
      DualUnlock(ino, parent);
	printf("delete error4\n");///just test
      fflush(stdout);//just test

      return;
    }
  }
  if (!Unlink(parent, req.src().substr(req.src().rfind('/') + 1), ino)) {
    resp.set_result(false);
    DualUnlock(ino, parent);
	printf("delete error5\n");///just test
      fflush(stdout);//just test

    return;
  }
  resp.set_result(true);
  DualUnlock(ino, parent);
}

void NameNode::PBMkdirs(const MkdirsRequestProto &req, MkdirsResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBMkdirs\n");//just test
    fflush(stderr);//just test

  size_t pos = 1, lastpos = 1;
  bool found;
  string path = req.src();
  if (path[0] != '/'){
	printf("mkdir error1\n");///just test
      fflush(stdout);//just test

    throw HdfsException("Not absolute path");
  }
  yfs_client::inum ino = 1;
  while ((pos = path.find('/', pos)) != string::npos) {
    if (pos != lastpos) {
      string component = path.substr(lastpos, pos - lastpos);
      if (yfs->lookup(ino, component.c_str(), found, ino) != yfs_client::OK){
	printf("mkdir error2\n");///just test
      fflush(stdout);//just test

        throw HdfsException("Traverse failed");
	}
      if (!found) {
        if (req.createparent()) {
          if (!Mkdir(ino, component.c_str(), 0777, ino)){
		printf("mkdir error3\n");///just test
      		fflush(stdout);//just test

            throw HdfsException("Create parent failed");
  	  }
        } else{	
		printf("mkdir error4\n");///just test
      		fflush(stdout);//just test

          throw HdfsException("Parent not exists");
	}
      }
    }
    pos++;
    lastpos = pos;
  }

  if (lastpos != path.size()) {
    string component = path.substr(lastpos);
    if (!Mkdir(ino, component.c_str(), 0777, ino)){	
		printf("mkdir error5\n");///just test
      		fflush(stdout);//just test

      throw HdfsException("Mkdirs failed");
    }
  } else{	
		printf("mkdir error5\n");///just test
      		fflush(stdout);//just test

    throw HdfsException("No file name given");
  }

  resp.set_result(true);
}

void NameNode::PBGetFsStats(const GetFsStatsRequestProto &req, GetFsStatsResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBGetFsStats\n");//just test
    fflush(stderr);//just test

  resp.set_capacity(0);
  resp.set_used(0);
  resp.set_remaining(0);
  resp.set_under_replicated(0);
  resp.set_corrupt_blocks(0);
  resp.set_missing_blocks(0);
}

void NameNode::PBSetSafeMode(const SetSafeModeRequestProto &req, SetSafeModeResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBSetSafeMode\n");//just test
    fflush(stderr);//just test

  resp.set_result(false);
}

void NameNode::PBGetDatanodeReport(const GetDatanodeReportRequestProto &req, GetDatanodeReportResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBGetDatanodeReport\n");//just test
    fflush(stderr);//just test

  switch (req.type()) {
  case ALL:
  case LIVE: {
    list<DatanodeIDProto> datanodes = GetDatanodes();
    for (auto it = datanodes.begin(); it != datanodes.end(); it++)
      resp.add_di()->mutable_id()->CopyFrom(*it);
    break; }
  case DEAD:
  case DECOMMISSIONING:
    break;
  }
}

void NameNode::PBDatanodeHeartbeat(const DatanodeHeartbeatRequestProto &req, DatanodeHeartbeatResponseProto &resp) {
    fprintf(stderr, "in namenode_base.cc::PBDatanodeHeartbeat\n");//just test
    fflush(stderr);//just test

  DatanodeHeartbeat(req.id());
}

// Main

int main(int argc, char *argv[]) {
    fprintf(stderr, "in namenode_base.cc::main\n");//just test
    fflush(stderr);//just test

  if(argc != 3){
    fprintf(stderr, "Usage: %s <extent_server> <lock_server>\n", argv[0]);
    fflush(stderr);
    exit(1);
  }

  int acceptfd = socket(AF_INET, SOCK_STREAM, 0);
  if (acceptfd < 0) {
    fprintf(stderr, "Create listen socket failed\n");
    fflush(stderr);
    return 1;
  }

  int yes = 1;
  if (setsockopt(acceptfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) != 0) {
    fprintf(stderr, "Enable SO_REUSEADDR failed: %s\n", strerror(errno));
    fflush(stderr);
    return 1;
  }

  struct sockaddr_in bindaddr;
  bindaddr.sin_family = AF_INET;
  bindaddr.sin_port = htons(9000);
  bindaddr.sin_addr.s_addr = 0;
  if (bind(acceptfd, (struct sockaddr *) &bindaddr, sizeof(bindaddr)) != 0) {
    fprintf(stderr, "Bind listen socket failed: %s\n", strerror(errno));
    fflush(stderr);
    return 1;
  }
  if (listen(acceptfd, 1) != 0) {
    fprintf(stderr, "Listen on :9000 failed: %s\n", strerror(errno));
    fflush(stderr);
    return 1;
  }

  namenode.init(argv[1], argv[2]);

  for (;;) {
    int clientfd = accept(acceptfd, NULL, 0);
    pthread_t thread;
    if (pthread_create(&thread, NULL, worker, (void *) (uintptr_t) clientfd) != 0) {
      fprintf(stderr, "Create handler thread failed: %s\n", strerror(errno));
      fflush(stderr);
    }
  }

  return 0;
}
