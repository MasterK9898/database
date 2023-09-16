#ifndef TABLE_REC_ITER_C
#define TABLE_REC_ITER_C

#include "MyDB_PageRecordIterator.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecordIterator.h"
#include "MyDB_TableReaderWriter.h"

void MyDB_TableRecordIterator::getNext()
{
  // shift the page iterator to the place first
  if (this->hasNext())
  {
    this->pageIter->getNext();
  }
}

bool MyDB_TableRecordIterator::hasNext()
{
  if (this->pageIter->hasNext())
  {
    return true;
  }

  if (this->pageIndex == this->table->lastPage())
  {
    return false;
  }

  pageIter = tableReaderWriter[++this->pageIndex].getIterator(this->rec);

  return this->hasNext();
}

MyDB_TableRecordIterator::MyDB_TableRecordIterator(MyDB_TablePtr whichTable, MyDB_RecordPtr iterateIntoMe, MyDB_TableReaderWriter *tableReaderWriter)
    : table(whichTable), rec(iterateIntoMe), pageIndex(0), tableReaderWriter(tableReaderWriter)
{
  this->pageIter = tableReaderWriter[0].getIterator(iterateIntoMe);
}
MyDB_TableRecordIterator::~MyDB_TableRecordIterator(){};

#endif