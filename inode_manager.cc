#include "inode_manager.h"
#include <math.h>
// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
	if(buf == NULL || id<0 || id>=BLOCK_NUM)
		return;
	memcpy(buf,blocks[id],BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{	
	if(buf == NULL || id<0 || id>=BLOCK_NUM)
		return;
	memcpy(blocks[id],buf,BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
	for(int i = 1036;i<BLOCK_NUM;i++){//1034 modify in lab2
		if(using_blocks.find(i)==using_blocks.end()){
			using_blocks.insert(std::pair<uint32_t,int>(i,1));
			return i;
		}
	}
	return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
	if(id<0 || id>=BLOCK_NUM){
		return;
	}
  	char* buf=(char *)malloc(sizeof(char)*BLOCK_SIZE);
	memset(buf,0,BLOCK_SIZE);
	d->write_block(id, buf);
	using_blocks.erase(id);
	
	free(buf); 
  	return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
 	inode *ino;  	
	for(int i=1;i<INODE_NUM;i++){
		ino = get_inode(i);
		if(ino==NULL){
			ino = (inode *)malloc(sizeof(inode));
			ino->type = type;
			ino->size = 0;
			ino->atime= 0;
			ino->ctime= time(0);
			ino->mtime= time(0);
			put_inode(i,ino);
			free(ino); 
			return i;
		}
	}
	return 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  	inode *ino = get_inode(inum);
	if(ino == NULL)
		return;
	ino->type = 0;
	ino->size = 0;
	ino->atime = 0;
	ino->mtime = 0;
	ino->ctime = 0;
	if(ino->blocks[NDIRECT] != 0){
		bm->free_block(ino->blocks[NDIRECT]);
		ino->blocks[NDIRECT]=0;
	}

	memset(ino->blocks,0,(NDIRECT+1)*sizeof(blockid_t)); 
	put_inode(inum,ino);
	return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);  

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;
	
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))
#define MAX(a,b) ((a)<(b) ? (b) : (b))
#define DIVID_CEIL(a,b) ((a)%(b)==0?(a/b):((a/b)+1))
/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */	
	inode *ino = get_inode(inum);
	if((ino == NULL) | (ino->type == 0))
		return;
	*size = ino->size;
	char *buf_tmp = (char *)malloc(sizeof(char)*(DIVID_CEIL(ino->size,BLOCK_SIZE)*BLOCK_SIZE));
	int newCeilIndex = DIVID_CEIL(ino->size,BLOCK_SIZE);
	for(int i=0;i<newCeilIndex;i++){
		uint32_t block_id = get_blockid_by_blocks_offset(ino,i);
		bm->read_block(block_id,&buf_tmp[i*BLOCK_SIZE]);
	}
	char *buf_exact = (char *)malloc(sizeof(char)*ino->size);
	memcpy(buf_exact,buf_tmp,ino->size);
	free(buf_tmp);
	*buf_out = buf_exact;
	ino->atime = time(0);
	put_inode(inum, ino);
  	return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
	inode *ino = get_inode(inum);
	if(ino == NULL){
		printf("ino is null in write file \n");
		return;
	}
	char *bufToFile = (char *)malloc(sizeof(char)*BLOCK_SIZE);
	//new larger than origin
	if((int)ino->size < size){
		int oldCeilIndex = DIVID_CEIL(ino->size,BLOCK_SIZE);
		for(int i=0; i < DIVID_CEIL(size,BLOCK_SIZE); i++){
			memset(bufToFile,0,BLOCK_SIZE);
			if(i == (DIVID_CEIL(size,BLOCK_SIZE))-1&&size%BLOCK_SIZE!=0)
				memcpy(bufToFile,&buf[i*BLOCK_SIZE],size%BLOCK_SIZE);
			else
				memcpy(bufToFile,&buf[i*BLOCK_SIZE],BLOCK_SIZE);
		
			uint32_t blockid = 0;
			if(oldCeilIndex==0||i>=oldCeilIndex){
				blockid = bm->alloc_block();
				set_blockid_by_blocks_offset(ino,i,blockid);
			}
			else{
				blockid = get_blockid_by_blocks_offset(ino,i);
			}
			printf("the bufToFile : %s\n",bufToFile);//just est
			bm->write_block(blockid,bufToFile);
		}	
	}
	//new smaller than origin
	else{
		int oldCeilIndex = DIVID_CEIL(ino->size,BLOCK_SIZE);
		int newCeilIndex = DIVID_CEIL(size,BLOCK_SIZE);
		for(int i=0; i<newCeilIndex; i++){
			memset(bufToFile,0,BLOCK_SIZE);
			memcpy(bufToFile,&buf[i*BLOCK_SIZE],BLOCK_SIZE);
			uint32_t blockid = get_blockid_by_blocks_offset(ino,i);
			bm->write_block(blockid,bufToFile);
		}
		for(int i=newCeilIndex;i<oldCeilIndex;i++){
			uint32_t blockid = get_blockid_by_blocks_offset(ino,i);
			bm->free_block(blockid);
			ino->blocks[i]=0;
		}
	}
	ino->size = size;
	ino->mtime = time(0);
	ino->ctime = time(0);
	put_inode(inum,ino);
	return;
}

/* if inum->type=0 ino=null!!!!*/
void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
 	inode *ino = get_inode(inum);
	if(ino == NULL){
		a.type = 0;
		return;
	}
	a.type = ino->type;
	a.atime = ino->atime;
	a.mtime = ino->mtime;
	a.ctime = ino->ctime;
	a.size = ino->size;
  	return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
	inode *ino = get_inode(inum);
	if(ino == NULL)
		return;
  	int newCeilIndex = DIVID_CEIL(ino->size,BLOCK_SIZE);
	for(int i=0; i<newCeilIndex; i++){
		int blockid = get_blockid_by_blocks_offset(ino,i);
		bm->free_block(blockid);
	}
	free_inode(inum);
	return;
}

/* 
*  get the blockid in inode->blocks
*  the i is the offset	
*/
uint32_t
inode_manager::get_blockid_by_blocks_offset(inode *ino, int i)
{
	uint32_t block_id = 0;
	if(i < NDIRECT){
		block_id = ino->blocks[i];
	}
	else{
		char *blockBuf = (char *)malloc(sizeof(char)*BLOCK_SIZE);
		bm->read_block(ino->blocks[NDIRECT],blockBuf);
		block_id = *((uint32_t *)blockBuf+(i-NDIRECT));
		free(blockBuf); 
	}
	return block_id;
}

/*
* set the blockid in inode->blocks
* the i is the offset
*/
void
inode_manager::set_blockid_by_blocks_offset(inode *ino, int i, uint32_t block_id)
{
	if(i < NDIRECT){
		ino->blocks[i] = block_id;
	}
	else{
		if(ino->blocks[NDIRECT] == 0){
			uint32_t new_indiret_blockid = bm->alloc_block();
			ino->blocks[NDIRECT] = new_indiret_blockid;
			char *blockBuf = (char *)malloc(sizeof(char)*BLOCK_SIZE);
			memset(blockBuf,0,BLOCK_SIZE);
			*((uint32_t *)blockBuf) = block_id;
			bm->write_block(new_indiret_blockid,blockBuf);
			free(blockBuf); 
		}
		else{
			char *blockBuf = (char *)malloc(sizeof(char)*BLOCK_SIZE);
			bm->read_block(ino->blocks[NDIRECT],blockBuf);
			i = i-NDIRECT;
			*((uint32_t *)blockBuf+i) = block_id;
			bm->write_block(ino->blocks[NDIRECT],blockBuf);
			free(blockBuf); 
		}
	}
}

void
inode_manager::append_block(uint32_t inum, blockid_t &bid)
{
  /*
   * your code goes here.
   */

}

void
inode_manager::get_block_ids(uint32_t inum, std::list<blockid_t> &block_ids)
{
  /*
   * your code goes here.
   */

}

void
inode_manager::read_block(blockid_t id, char buf[BLOCK_SIZE])
{
  /*
   * your code goes here.
   */

}

void
inode_manager::write_block(blockid_t id, const char buf[BLOCK_SIZE])
{
  /*
   * your code goes here.
   */

}

void
inode_manager::complete(uint32_t inum, uint32_t size)
{
  /*
   * your code goes here.
   */

}
