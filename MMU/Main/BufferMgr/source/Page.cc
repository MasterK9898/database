
#ifndef PAGE_C
#define PAGE_C

#include "Page.h"
#include "PageHandle.h"
#include "Table.h"
#include "BufferManager.h"

using namespace std;

Page::Page(TablePtr table, size_t pageIndex, BufferManager *manager, bool pinned)
    : bytes(nullptr), doNotKill(false), dirty(false), pinned(pinned), manager(manager), table(table), pageIndex(pageIndex), ref(0) {}

Page::~Page()
{
  // no need to write back at this stage
  // because the clock has a pointer to the page
  // and in order to destruct the clock will need to lose the page
  // which means an evict happens, and at that time write back already happened
}

void Page::removeRef()
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

void Page::addRef()
{
  this->ref++;
}

#endif
