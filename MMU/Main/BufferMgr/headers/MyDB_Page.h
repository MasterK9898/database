
#ifndef PAGE_H
#define PAGE_H

#include <memory>
// #include "MyDB_BufferManager.h"
// #include "MyDB_Page.h"
#include "MyDB_Table.h"

using namespace std;
class MyDB_Page;
class MyDB_BufferManager;
class MyDB_PageHandleBase;

typedef shared_ptr<MyDB_Page> MyDB_PagePtr;
// a page object that stores the meat
class MyDB_Page
{

public:
  ~MyDB_Page();

  MyDB_Page(MyDB_TablePtr table, size_t pageIndex, MyDB_BufferManager *manager, bool pinned);

  // decrements the ref count
  void removeRef();

  // increments the ref count
  void addRef();

private:
  friend class MyDB_BufferManager;
  friend class MyDB_PageHandleBase;
  // the meat
  void *bytes;

  // second chance
  bool doNotKill;

  // dirty or not
  bool dirty;

  // pinned?
  bool pinned;

  // pointer to the parent buffer manager
  MyDB_BufferManager *manager;

  // reference to the table, could be nullptr for annonymous pages
  MyDB_TablePtr table;

  // this is the position of the page in the relation
  size_t pageIndex;

  // the number of references
  int ref;
};

#endif