// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client_cache.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

using namespace std;

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
	cout<<"in yfs_client::init"<<endl;//just test
  ec = new extent_client(extent_dst);
  //lc = new lock_client(lock_dst); //lab2
  lc = new lock_client_cache(lock_dst); //lab3
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
	cout<<"in yfs_client::isfile"<<endl;//just test
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("%lld is not a dir\n", inum);
    return false;
}

/**
 * issymlink(inum) to judge 
 * whether a file's type is a symbolic link or not
 **/
bool
yfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    } 
    printf("%lld is not a symlink\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
	printf("in getfile error\n");//just test
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

/**
 * getsymlink(inum, symlinkinfo)
 * get a symbolic link file's information into a 'symlinkinfo' struct
 **/
int
yfs_client::getsymlink(inum inum, symlinkinfo &slin)
{
    int r = OK;

    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    slin.atime = a.atime;
    slin.mtime = a.mtime;
    slin.ctime = a.ctime;
    slin.size = a.size;
	
release:
    return r;
}

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
	lc->acquire(ino);
	cout<<"in yfs_client::setattr"<<endl;//just test
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
	extent_protocol::attr attr;
	ec->getattr(ino,attr);
	string origin;
	ec->get(ino,origin);
	if(attr.size>=size){
		origin=origin.substr(0,size);
	}
	else{
		char *append = (char *)malloc(sizeof(char)*(size-attr.size));
		memset(append,0,size-attr.size);
		string appendStr(append,size-attr.size);
		origin = origin+appendStr;
	}
	ec->put(ino,origin);
	lc->release(ino);
   	return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
	lc->acquire(parent);
	cout<<"in yfs_client::create"<<endl;//just test
    	int r = OK; 
	bool found;
	if(lookup(parent,name,found,ino_out) == OK){
		lc->release(parent);
		return EXIST;
	}
	ec->create(extent_protocol::T_FILE,ino_out);
	
	string parentDir;
	ec->get(parent, parentDir);
	cout<<"before append parentDir in create in yfs_client parentDir : "<<parentDir<<endl;
	parentDir = parentDir + name + ":" + filename(ino_out) + ";";
	
	cout<<"after append parentDir in create in yfs_client parentDir : "<<parentDir<<endl;
	ec->put(parent, parentDir);
	lc->release(parent);
	return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
	lc->acquire(parent);
	cout<<"in yfs_client::mkdir"<<endl;//just test
	int r = OK;
	
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
	bool found;
	if(lookup(parent,name,found,ino_out) == OK){
		lc->release(parent);
		return EXIST;
	}
	ec->create(extent_protocol::T_DIR,ino_out);

	string parentDir;
	ec->get(parent,parentDir);
	parentDir = parentDir + name + ":" + filename(ino_out) + ";";
	ec->put(parent, parentDir);
	lc->release(parent);
	return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
	cout<<"in yfs_client::lookup"<<endl;//just test
	printf("the parent inum is %d\n",(int)parent);//just test
	printf("the name is %s and length : %d\n",name,(int)strlen(name));//just test
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
	std::list<dirent> list;
	std::list<dirent>::iterator itor;

	readdir(parent, list);

	itor = list.begin();
	while(itor != list.end()){
		dirent tmpDir = *itor;
		cout<<"int lookup while the tmpdiren filename : "<<tmpDir.name<<" and length :"<<tmpDir.name.length()<<endl;//just test
		if(tmpDir.name.compare(std::string(name)) == 0){
			printf("yes is true!\n");//just test
			found = true;
			ino_out = tmpDir.inum;
			return OK;
		}
		itor++;
	}
	printf("why is false?\n");//just test
	found = false;
   	return NOENT;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
	int r = OK;
	cout<<"in yfs_client::readdir"<<endl;//just test
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
	std::string dirCont;//quan bu de nei rong
	std::string tmpDirStr;

	ec->get(dir,dirCont);	
	cout << "the dir content : "<<dirCont<<endl;
    	while(dirCont.find(";") != dirCont.npos){
		tmpDirStr = dirCont.substr(0,dirCont.find(";"));
		dirCont = dirCont.substr(dirCont.find(";")+1);
		dirent tmpDirent;
		tmpDirent.name = tmpDirStr.substr(0,tmpDirStr.find(":"));//filename
		tmpDirent.inum = n2i(tmpDirStr.substr(tmpDirStr.find(":")+1));
		cout<<"the dir file name: "<<tmpDirent.name<<endl;//just test
		cout<<"the dir file inum: "<<tmpDirent.inum<<endl;//just test
		list.push_back(tmpDirent);
	}
	return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
	lc->acquire(ino);
	cout<<"in yfs_client::read"<<endl;//just test
    int r = OK;

    /*
     * your code goes here
     * note: read using ec->get().
     */
	extent_protocol::attr attr;
	ec->getattr(ino,attr);
	if(attr.size == 0 || off >= attr.size){
		data = "";
		lc->release(ino);
		return r;
	}
	
	string origin;
	ec->get(ino,origin);
	if((off+size) > attr.size){
		data = origin.substr(off);
	}
	else{
		data = origin.substr(off,size);
	}
	lc->release(ino);
	return r;
}

/**
 * readsymlink(inym, string)
 * read the content of a symbolic link file
 **/
int
yfs_client::readsymlink(inum ino, std::string &data)
{
	lc->acquire(ino);
	int r = OK;
	
	ec->get(ino,data);
	lc->release(ino);
	return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
	lc->acquire(ino);
	cout<<"in yfs_client::write"<<endl;//just test
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
	extent_protocol::attr attr;
	ec->getattr(ino,attr);
	string newFile(data,size);
	if(attr.size == 0){
		char *blank = (char *)malloc(off*sizeof(char));
		memset(blank,0,off);
		string offBlank(blank,off);
		newFile = offBlank+newFile;
		ec->put(ino,newFile);
		lc->release(ino);
		return r;
	}
	string origin;
	ec->get(ino,origin);
	
	if(off <= (long)attr.size){
		if((off+size)<attr.size){
			newFile = origin.substr(0,off)+newFile+origin.substr(off+size);
			ec->put(ino,newFile);
			lc->release(ino);
			return r;
		}
		else{
			newFile = origin.substr(0,off)+newFile;
			ec->put(ino,newFile);
			lc->release(ino);
			return r;
		}
	}
	else{
		char *blank = (char *)malloc(sizeof(char)*(off-attr.size));
		memset(blank,0,off-attr.size);
		string offblank(blank,off-attr.size);
		newFile = origin+offblank+newFile;
		ec->put(ino,newFile);
		lc->release(ino);
		return r;
	}
	lc->release(ino);
	return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
	cout<<"in yfs_client::unlink"<<endl;//just test
    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
	bool found;
	inum ino_out;

	list<dirent> list;
	std::list<dirent>::iterator itor;

	lookup(parent,name,found,ino_out);
	if(found == false){
		return NOENT;
	}
	ec->remove(ino_out);
	
	string newEnt;
	readdir(parent,list);
	itor = list.begin();
	while(itor != list.end()){
		dirent tmpDir = *itor;
		if(tmpDir.name.compare(std::string(name)) == 0){
			itor++;
			continue;
		}
		newEnt = newEnt + tmpDir.name+":"+filename(tmpDir.inum)+";";
		itor++;
	}
	ec->put(parent,newEnt);
    	return r;
}

/**
 * symlink(inum, const char*, const char*, inum&)
 * create a symbolic link file in to parent dir,
 * the 'link' parameter indicates the file name which is linked to
 **/
int
yfs_client::symlink(inum parent,const char *link, const char *name, inum &ino_out){
	cout<<"in yfs_client::symlink"<<endl;//just test
	lc->acquire(parent);
	int r = OK;
	bool found;
	if(lookup(parent,name,found,ino_out) == OK){
		lc->release(parent);
		return EXIST;
	}

	ec->create(extent_protocol::T_SYMLINK,ino_out);
	
	ec->put(ino_out,string(link));	

	string parentDir;
	ec->get(parent, parentDir);
	parentDir = parentDir + name + ":" + filename(ino_out) + ";";
	ec->put(parent, parentDir);
	lc->release(parent);
	return r;
}

