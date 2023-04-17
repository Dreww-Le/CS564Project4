#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
    // Ask about BufDesc and BufMgr
    // allocates free frame using clock algorithm
    // first step is to check if validBit is false
    // Use -> for a pointer, and . for not a pointer (ex: tempbuf->valid vs. tempbuf.valid)
    for(int i = 0; i < (numBufs * 2); i++) {
        //clockHand++;
        //if (clockHand > numBufs) {
        //       clockHand = 0;
        //}
//	cout << "iteration " << i << " ";
	advanceClock();
	BufDesc *tempbuf = &(bufTable[clockHand]);
        if (tempbuf->valid) {
            if (tempbuf->refbit) {
                tempbuf->refbit = false;
		
            }
            else {
                if (tempbuf->pinCnt > 0) {
                    continue;
                }
                else {
                    if (tempbuf->dirty) {
                        // flush page
                        Status flush = tempbuf->file->writePage(tempbuf->pageNo, &(bufPool[clockHand]));
                        if (flush != OK) {
                            return flush;
                        }
                    }
                    //tempbuf->Set(tempbuf->file, tempbuf->pageNo);
                    frame = clockHand;
//		    cout << "in loop";
		    hashTable->remove(tempbuf->file, tempbuf->pageNo);
                    return OK;
                }
            }
        }
        else {
            frame = clockHand;
            //tempbuf->Set(tempbuf->file, tempbuf->pageNo);
//            hashTable->remove(tempbuf.file, tempbuf.pageNo);
	    return OK;
        }   
    }
    return BUFFEREXCEEDED;
    
    // Call the hash table in here somewhere (Ask TA how to do that)
    // how to call to the I/O layer? (Ask TA)
    // how to check if the buffer frame allocated has valid page in it, remove the appropriate entry if it is
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    // invoke lookup() method on hash table to get frame number
    // if page is not in buffer pool (lookup is false): call allocBuf() to allocate buffer frame then call file->readPage() to read page from disk into buffer pool frame. Then insert page into hashtable and invoke Set() on frame to set it up properly, which will leave the Pincnt for the page set to 1. Return pointer to frame containing page via page parameter
    // If page is in buffer pool (lookup is OK) set refbit, increment pincnt and return pointer to frame containing page via page parameter
    Status inPool = OK;
    int frameNo = 0;
    inPool = hashTable->lookup(file, PageNo, frameNo);
    if (inPool == OK) {
	BufDesc *tempbuf = &(bufTable[frameNo]);
        tempbuf->refbit = true;
        tempbuf->pinCnt += 1;
        page = &bufPool[frameNo];
    }
    else {
        Status allocated = allocBuf(frameNo);
	BufDesc *tempbuf = &(bufTable[frameNo]);
        if(allocated != OK) {
            return allocated;
        }
        Status readPage = file->readPage(PageNo, &bufPool[frameNo]);
        if(readPage != OK) {
		return readPage;
	}
	Status insert = hashTable->insert(file, PageNo, frameNo);
        if (insert != OK) {
           return insert;
        }
        tempbuf->Set(file, PageNo);
        page = &bufPool[frameNo];
        
    }
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    // decrement pincount of frame containing (file, PageNo) 
    // if dirty == True set the dirty bit
    
    // Ask TA about HASHNOTFOUND if the page is not in the buffer pool hash table
    int frameNo = 0;
    Status inPool = OK;
    inPool = hashTable->lookup(file, PageNo, frameNo);
    BufDesc *tempbuf = &(bufTable[frameNo]);
    if(inPool != OK) {
        return inPool;
    }
    if(tempbuf->pinCnt == 0) {
        return PAGENOTPINNED;
    }
//    bufTable[frameNo].refbit = false;
    tempbuf->pinCnt -= 1;
    if (dirty) {
        tempbuf->dirty = true;
    }
    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    // allocate empty page in specified file by invoking file->allocatePage() which returns page
    // then allocBuf() is called to obtain a buffer pool frame
    // you then have to insert an entry into the hash table and call Set() to set it up on the frame properly
    // this method returns the page number of the newly allocated page to the caller via pageNo parameter and a pointer to the buffer frame allocated for the page via page parameter
    Status allocPage = file->allocatePage(pageNo);
    if (allocPage != OK) {
        return allocPage;
    }
    int frameNo = 0;
    Status allocated = allocBuf(frameNo);
    if (allocated != OK) {
       return allocated;
    }
    page = &bufPool[frameNo];
    Status insert = hashTable->insert(file, pageNo, frameNo);
    if (insert != OK) {
       return insert;
    }
    BufDesc *tempbuf = &(bufTable[frameNo]);
    tempbuf->Set(file, pageNo);
    return OK;
    
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


