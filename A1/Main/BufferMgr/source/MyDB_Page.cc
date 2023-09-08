
#ifndef PAGE_C
#define PAGE_C

#include "MyDB_Page.h"
#include "MyDB_BufferManager.h"
#include <string>

using namespace std;

// void *MyDB_Page::getBytes(MyDB_PagePtr self)
// {
//   this->manager->retrivePage(self);
//   return this->bytes;
// }

// void MyDB_Page::setDirty()
// {
//   this->dirty = true;
// }

// MyDB_Page::MyDB_Page(MyDB_TablePtr table, size_t pageIndex, MyDB_BufferManager *manager, bool pinned)
//     : bytes(nullptr), doNotKill(false), dirty(false), pinned(pinned), manager(manager), table(table), pageIndex(pageIndex), ref(0), remaining(0) {}

// void MyDB_Page::removeRef(MyDB_PagePtr self)
// {
//   this->ref--;
//   if (ref == 0)
//   {
//     // no longer pinned
//     this->pinned = false;

//     if (this->table == nullptr)
//     // anonymous page shall be destroyed when no reference
//     {
//       // do we need to store the page into anonfile?
//       if (this->bytes != nullptr)
//       // free the memeory
//       {
//         this->manager->ram.push_back(this->bytes);
//       }

//       // mark it self as useless
//       this->bytes = nullptr;
//       this->dirty = false;

//       // remove it from clock, no matter pinned or not
//       // for (size_t i = 0; i < this->manager->clock.size(); i++)
//       // {
//       //   if (this->manager->clock.at(i) == self)
//       //   {
//       //     this->manager->clock.at(i) = nullptr;
//       //   }
//       // }
//     }
//   }
// }

// void MyDB_Page::addRef()
// {
//   this->ref++;
// }

#endif