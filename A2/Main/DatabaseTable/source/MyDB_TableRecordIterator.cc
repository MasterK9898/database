#ifndef TABLE_REC_ITER_C
#define TABLE_REC_ITER_C

#include "MyDB_PageRecordIterator.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecordIterator.h"
#include "MyDB_TableReaderWriter.h"

using namespace std;

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

  pageIter = this->tableReaderWriter->operator[](++this->pageIndex).getIterator(this->rec);

  return this->hasNext();
}

MyDB_TableRecordIterator::MyDB_TableRecordIterator(MyDB_TablePtr whichTable, MyDB_RecordPtr iterateIntoMe, MyDB_TableReaderWriter *tableReaderWriter)
    : rec(iterateIntoMe), table(whichTable), tableReaderWriter(tableReaderWriter), pageIndex(0)
{
  this->pageIter = tableReaderWriter->operator[](0).getIterator(iterateIntoMe);
}
MyDB_TableRecordIterator::~MyDB_TableRecordIterator(){};

#endif