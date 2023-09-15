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

  // destructor and contructor
  MyDB_PageRecordIterator(MyDB_PageHandle whichPage){};
  ~MyDB_PageRecordIterator(){};

private:
  // current page
  MyDB_PageHandle page;
  // current record
  MyDB_RecordPtr rec;
  // index of current record
  size_t recIndex;
};

#endif