#ifndef PAGE_REC_ITER_C
#define PAGE_REC_ITER_C

#include "MyDB_PageRecordIterator.h"
#include "MyDB_PageReaderWriter.h"

using namespace std;

// professor's in class guide
// void* pos = myPage->getByres() headerSize;
// void* nextRec = myRec->fromBinary(pos);
// int btytesConsumed = (char*)nextRec - (char*)pos;

// PageMeta * pagePtr = (pageHeader*)myPage->getBytes();
// while(bytesRead <= pagePtr->numBytesUsed) {
// void* nextPos = myRec->fromBinary((void*)&pagePtr->recs[bytesRead)];
// bytesRead (char*)nextRps = &pagePtr->recs[bytesRead];
//}

void MyDB_PageRecordIterator::getNext()
{
  if (this->hasNext())
  {
    PageMeta *pageHeader = castPageHeader(this->page);
    // do the pointer arrithmetic directly, no need to deref
    void *pos = (void *)(pageHeader->recs + this->numBytesUsed);
    this->rec->fromBinary(pos);
    this->numBytesUsed += this->rec->getBinarySize();
  }
}

bool MyDB_PageRecordIterator::hasNext()
{
  PageMeta *pageHeader = castPageHeader(this->page);
  // compare it with info on header
  return (this->numBytesUsed < pageHeader->numBytesUsed);
}

MyDB_PageRecordIterator::MyDB_PageRecordIterator(MyDB_PageHandle whichPage, MyDB_RecordPtr iterateIntoMe) : page(whichPage), rec(iterateIntoMe), numBytesUsed(0){};

MyDB_PageRecordIterator::~MyDB_PageRecordIterator(){};

#endif