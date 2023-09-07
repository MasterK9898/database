
#ifndef PAGE_H
#define PAGE_H

#include <iostream>
#include "MyDB_BufferManager.h"

using namespace std;
// a page object that stores the meat
class MyDB_Page
{

public:
  // when first created, the page is guaranteened for the area of space
  MyDB_Page(bool pinned, long index, void *pointer) : pinned(pinned), pageIndex(index), bytes(pointer) {}

  // whipe out the data when destroyed
  ~MyDB_Page()
  {
  }

  // DATA METHODS

  // get the data
  void *getBytes()
  {
    return this->bytes;
  }

  // REFERENCE COUNT METHODS

  // increment the reference count
  void addRef()
  {
    this->ref++;
  }

  // decrement the reference count
  void removeRef()
  {
    this->ref--;
  }

  // get the reference count
  int getRef()
  {
    return this->ref;
  }

  // PINNED METHODS

  // set the pinned status
  void setPinned(bool pinned)
  {
    this->pinned = pinned;
  }

  // get the pinned status
  bool getPinned()
  {
    return this->pinned;
  }

  // DIRTY METHODS

  // set dirty bit
  void setDirty(bool dirty)
  {
    this->dirty = dirty;
  }

  // get dirty bit
  bool getDirty()
  {
    return this->dirty;
  }

  // BUFFERED METHOD

  // check if this page is buffered or not
  bool getBuffered()
  {
    return this->buffered;
  }

  // set this page is buffered or not
  void setBuffered(bool buffered)
  {
    this->buffered = buffered;
  }

  // TIME STAMP METHODS

  // get the last accessed time
  size_t getTimeStamp()
  {
    return this->timeStamp;
  }

  // set the last modified time
  void setTimeStamp(size_t timeStamp)
  {
    this->timeStamp = timeStamp;
  }

  // PAGE INDEX METHOD

  // get current page index
  long getPageIndex()
  {
    return this->pageIndex;
  }

  // set the page index
  void setPageIndex(long index)
  {
    this->pageIndex = index;
  }

private:
  // the index of the page, to calculate offset
  long pageIndex;
  // pinned or not
  bool pinned;
  // dirty or not
  bool dirty;
  // the meat
  void *bytes;
  // reference count
  int ref;
  // last modifed
  size_t timeStamp;
  // buffered or not, if not then need to retrive from disk
  bool buffered;
};

#endif