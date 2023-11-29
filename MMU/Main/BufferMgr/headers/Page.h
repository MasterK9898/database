
#ifndef PAGE_H
#define PAGE_H

#include <memory>
// #include "BufferManager.h"
// #include "Page.h"
#include "Table.h"

using namespace std;
class Page;
class BufferManager;
class PageHandleBase;

typedef shared_ptr<Page> PagePtr;
// a page object that stores the meat
class Page
{

public:
  ~Page();

  Page(TablePtr table, size_t pageIndex, BufferManager *manager, bool pinned);

  // decrements the ref count
  void removeRef();

  // increments the ref count
  void addRef();

private:
  friend class BufferManager;
  friend class PageHandleBase;
  // the meat
  void *bytes;

  // second chance
  bool doNotKill;

  // dirty or not
  bool dirty;

  // pinned?
  bool pinned;

  // pointer to the parent buffer manager
  BufferManager *manager;

  // reference to the table, could be nullptr for annonymous pages
  TablePtr table;

  // this is the position of the page in the relation
  size_t pageIndex;

  // the number of references
  int ref;
};

#endif