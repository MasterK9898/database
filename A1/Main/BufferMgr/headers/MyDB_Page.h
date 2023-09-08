
#ifndef PAGE_H
#define PAGE_H

#include <memory>
#include "MyDB_BufferManager.h"
#include "MyDB_Page.h"
#include "MyDB_Table.h"

using namespace std;
class MyDB_Page;

typedef shared_ptr<MyDB_Page> MyDB_PagePtr;
// a page object that stores the meat
class MyDB_Page
{

public:
  // access the raw bytes in this page
  void *getBytes(MyDB_PagePtr self)
  {
    this->manager->retrivePage(self);
    return this->bytes;
  }

  // let the page know that we have written to the bytes
  void setDirty()
  {
    this->dirty = true;
  }

  // free the memory
  ~MyDB_Page();

  MyDB_Page(MyDB_TablePtr table, size_t pageIndex, MyDB_BufferManager *manager, bool pinned)
      : doNotKill(false), manager(manager), table(table), pageIndex(pageIndex), ref(0), dirty(false), bytes(nullptr), remaining(0), pinned(pinned) {}

  // sets the bytes in the page
  void setBytes(void *bytes, size_t numBytes);

  // decrements the ref count
  void removeRef(MyDB_PagePtr self)
  {
    this->ref--;
    if (ref == 0)
    {
      // no longer pinned
      this->pinned = false;

      if (this->table == nullptr)
      // anonymous page shall be destroyed when no reference
      {
        // do we need to store the page into anonfile?
        if (this->bytes != nullptr)
        // free the memeory
        {
          this->manager->ram.push_back(this->bytes);
        }

        // mark it self as useless
        this->bytes = nullptr;
        this->dirty = false;

        // remove it from clock, no matter pinned or not
        // for (size_t i = 0; i < this->manager->clock.size(); i++)
        // {
        //   if (this->manager->clock.at(i) == self)
        //   {
        //     this->manager->clock.at(i) = nullptr;
        //   }
        // }
      }
    }
  }

  // increments the ref count
  void addRef()
  {
    this->ref++;
  }

private:
  friend class MyDB_BufferManager;
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

  // remaining bytes
  size_t remaining;
  // last modifed
  // size_t timeStamp;

  // the number of references
  int ref;
};

#endif