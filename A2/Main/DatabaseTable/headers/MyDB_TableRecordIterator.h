#ifndef TABLE_REC_ITER_H
#define TABLE_REC_ITER_H

#include <memory>
#include "MyDB_RecordIterator.h"
#include "MyDB_Record.h"
#include "MyDB_Table.h"
#include "MyDB_PageRecordIterator.h"

class MyDB_TableRecordIterator;

typedef shared_ptr<MyDB_TableRecordIterator> MyDB_TableRecordIteratorPtr;

class MyDB_TableRecordIterator : public MyDB_RecordIterator
{

public:
  // put the contents of the next record in the file/page into the iterator record
  // this should be called BEFORE the iterator record is first examined
  void getNext();

  // return true iff there is another record in the file/page
  bool hasNext();

  // destructor and contructor
  MyDB_TableRecordIterator(MyDB_TablePtr whichTable, MyDB_RecordPtr iterateIntoMe, MyDB_TableReaderWriter *tableReaderWriter);

  ~MyDB_TableRecordIterator();

private:
  // current record in use
  MyDB_RecordPtr rec;
  // current table
  MyDB_TablePtr table;
  // iterator of the current page
  // page reader writer's interface cannot be changed
  // so cannot use more specific type
  MyDB_RecordIteratorPtr pageIter;
  // reference to the table reader wrtier
  MyDB_TableReaderWriter *tableReaderWriter;
  // current index of page
  int pageIndex;

  // get record iterator of the target page
  MyDB_RecordIteratorPtr getPageRecordIterator(int index);
};

#endif