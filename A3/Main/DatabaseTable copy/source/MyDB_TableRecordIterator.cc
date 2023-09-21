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

  this->pageIter = this->getPageRecordIterator(++this->pageIndex);

  return this->hasNext();
}

MyDB_TableRecordIterator::MyDB_TableRecordIterator(MyDB_TablePtr whichTable, MyDB_RecordPtr iterateIntoMe, MyDB_TableReaderWriter *tableReaderWriter)
    : rec(iterateIntoMe), table(whichTable), tableReaderWriter(tableReaderWriter), pageIndex(0)
{
  this->pageIter = this->getPageRecordIterator(0);
}

MyDB_TableRecordIterator::~MyDB_TableRecordIterator(){};

MyDB_RecordIteratorPtr MyDB_TableRecordIterator::getPageRecordIterator(int index)
{
  return tableReaderWriter->operator[](index).getIterator(this->rec);
}

#endif