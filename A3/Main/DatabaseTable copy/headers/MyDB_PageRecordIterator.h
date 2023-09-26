#ifndef PAGE_REC_ITER_H
#define PAGE_REC_ITER_H

#include <memory>
#include "MyDB_RecordIterator.h"
#include "MyDB_Record.h"
#include "MyDB_PageHandle.h"

using namespace std;

class MyDB_PageRecordIterator;

typedef shared_ptr<MyDB_PageRecordIterator> MyDB_PageRecordIteratorPtr;

class MyDB_PageRecordIterator : public MyDB_RecordIterator
{

public:
  // put the contents of the next record in the file/page into the iterator record
  // this should be called BEFORE the iterator record is first examined
  void getNext();

  // return true iff there is another record in the file/page
  bool hasNext();

  // BEFORE a call to getNext (), a call to getCurrentPointer () will get the address
  // of the record.  At a later time, it is then possible to reconstitute the record
  // by calling MyDB_Record.fromBinary (obtainedPointer)... ASSUMING that the page
  // that the record is located on has not been swapped out
  virtual void *getCurrentPointer();

  // destructor and contructor
  MyDB_PageRecordIterator(MyDB_PageHandle whichPage, MyDB_RecordPtr whichRec);

  ~MyDB_PageRecordIterator();

private:
  friend class MyDB_TableRecordIterator;
  // current record in use
  MyDB_RecordPtr rec;
  // current page
  MyDB_PageHandle page;
  // how far has this iterator reached?
  // the logic is the same as the "numBytesUsed" in pageMeta
  size_t numBytesUsed;
};

#endif