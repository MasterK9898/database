
#ifndef PAGE_H
#define PAGE_H

#include <iostream>
#include "MyDB_Table.h"
#include "MyDB_BufferManager.h"

using namespace std;
// a page object that stores the info
class MyDB_Page
{

public:
  MyDB_Page(MyDB_TablePtr whichTable, bool pinned, long page_id) : table(whichTable), pinned(pinned), page_id(page_id)
  {
  }

  ~MyDB_Page();

  void *getBytes();

  void wroteBytes()
  {
    this->dirty = true;
  }

  void addRef()
  {
    this->refCount++;
  }

  void removeRef()
  {
    this->refCount--;
  }

  void setPinned(bool pinned)
  {
    this->pinned = pinned;
  }

  bool getPinned()
  {
    return this->pinned;
  }

  long getTimeStamp()
  {
    return this->timeStamp;
  }

  void setTimeStamp(long timeStamp)
  {
    this->timeStamp = timeStamp;
  }

  MyDB_TablePtr getTable()
  {
    return this->table;
  }

  long getPageID()
  {
    return this->page_id;
  }

  bool getBuffered()
  {
    return this->buffered;
  }

  void setBuffered(bool buffered)
  {
    this->buffered = buffered;
  }

  void setDirty(bool dirty)
  {
    this->dirty = dirty;
  }

  bool getDirty()
  {
    return this->dirty;
  }

private:
  friend class MyDB_BufferManager; // solve the problem of inaccessible

  // MyDB_BufferManager &bufferManager;
  MyDB_TablePtr table;
  long page_id;
  //
  bool pinned;
  // dirty or not
  bool dirty;
  void *bytes;
  // reference count
  int refCount;
  // last modifed
  long timeStamp;
  // buffered
  bool buffered;
};

#endif