
#ifndef PAGE_C
#define PAGE_C

#include "MyDB_Page.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include "MyDB_BufferManager.h"

using namespace std;

MyDB_Page::MyDB_Page(MyDB_TablePtr table, size_t pageIndex, MyDB_BufferManager *manager, bool pinned)
    : bytes(nullptr), doNotKill(false), dirty(false), pinned(pinned), manager(manager), table(table), pageIndex(pageIndex), ref(0) {}

MyDB_Page::~MyDB_Page()
{
  // no need to write back at this stage
  // because the clock has a pointer to the page
  // and in order to destruct the clock will need to lose the page
  // which means an evict happens, and at that time write back already happened
}

void MyDB_Page::removeRef()
{
  this->ref--;
  if (ref == 0)
  {
    // no longer pinned
    this->pinned = false;

    if (this->table == nullptr)
    // anonymous page, leave them dying on the clock is enough
    // they will be evicted eventually
    // and because no one should access the anon page again
    // there's no need to wrtie back in this situation
    {
      this->doNotKill = false;
      this->bytes = nullptr;
    }
  }
}

void MyDB_Page::addRef()
{
  this->ref++;
}

#endif
