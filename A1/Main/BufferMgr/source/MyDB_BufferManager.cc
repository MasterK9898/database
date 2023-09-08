
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include <string>
#include <vector>
#include <unordered_map>
#include <utility>
#include <memory>
#include <ctime>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include "MyDB_Page.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include "MyDB_BufferManager.h"

using namespace std;

MyDB_PageHandle MyDB_BufferManager ::getPage(MyDB_TablePtr whichTable, long i)
{
  // open the file
  int fd = open(whichTable->getStorageLoc().c_str(), O_CREAT | O_RDWR, 0666);
  this->fileTable[whichTable] = fd;

  // get the key
  pair<MyDB_TablePtr, long> key = make_pair(whichTable, i);
  if (this->pageTable.count(key) == 0)
  // create one from scratch, insert to table
  {
    // it is not there, so create a page
    auto newPage = make_shared<MyDB_Page>(whichTable, i, this, true);
    this->pageTable[key] = newPage;
    return make_shared<MyDB_PageHandleBase>(newPage);
  }
  else
  // get from table
  {
    MyDB_PagePtr page = this->pageTable[key];
    return make_shared<MyDB_PageHandleBase>(page);
  }
}

MyDB_PageHandle MyDB_BufferManager ::getPage()
{
  auto anonPage = make_shared<MyDB_Page>(nullptr, this->tempIndex++, this, true);
  return make_shared<MyDB_PageHandleBase>(anonPage);
}

MyDB_PageHandle MyDB_BufferManager ::getPinnedPage(MyDB_TablePtr whichTable, long i)
{
  // open the file
  int fd = open(whichTable->getStorageLoc().c_str(), O_CREAT | O_RDWR, 0666);
  this->fileTable[whichTable] = fd;

  pair<MyDB_TablePtr, long> key = make_pair(whichTable, i);
  if (this->pageTable.count(key) == 0)
  // create one from scratch, insert to table
  {
    // it is not there, so create a page
    auto newPage = make_shared<MyDB_Page>(whichTable, i, this, true);
    this->pageTable[key] = newPage;
    return make_shared<MyDB_PageHandleBase>(newPage);
  }
  else
  // get from table
  {
    MyDB_PagePtr page = this->pageTable[key];
    // pin the page anyway
    page->pinned = true;
    return make_shared<MyDB_PageHandleBase>(page);
  }
}

MyDB_PageHandle MyDB_BufferManager ::getPinnedPage()
{
  auto anonPage = make_shared<MyDB_Page>(nullptr, this->tempIndex++, this, true);
  return make_shared<MyDB_PageHandleBase>(anonPage);
}

void MyDB_BufferManager ::unpin(MyDB_PageHandle unpinMe)
{
  unpinMe->getPage()->pinned = false;
}

MyDB_BufferManager ::MyDB_BufferManager(size_t pageSize, size_t numPages, string tempFile) : clockHand(0), pageSize(pageSize), numPages(numPages), tempFile(tempFile), tempIndex(0)
{
  for (size_t i = 0; i < numPages; i++)
  {
    ram.push_back(malloc(pageSize));
    clock.push_back(nullptr);
  }
  // open the file
  int fd = open(tempFile.c_str(), O_CREAT | O_RDWR);
  this->fileTable[nullptr] = fd;
}

MyDB_BufferManager ::~MyDB_BufferManager()
{
  for (auto pair : this->pageTable)
  {
    auto currentPage = pair.second;

    if (currentPage->bytes != nullptr)
    {

      if (currentPage->dirty)
      // dirty page write back
      {
        int fd = this->fileTable[currentPage->table];
        lseek(fd, currentPage->pageIndex * pageSize, SEEK_SET);
        write(fd, currentPage->bytes, pageSize);
        currentPage->dirty = false;
      }
      ram.push_back(currentPage->bytes);

      currentPage->bytes = nullptr;
    }
  }

  // free all memory
  for (auto ramUnit : this->ram)
  {
    free(ramUnit);
  }

  // close all file
  for (auto pair : this->fileTable)
  {
    int fd = pair.second;
    close(fd);
  }

  // say goodbye the the file
  remove(tempFile.c_str());
}

void MyDB_BufferManager::retrivePage(MyDB_PagePtr page)
{
  if (page->bytes == nullptr)
  // check the pointer to see if it is buffered
  {
    // if there are empty ram space, arrange the space and put onto the clock

    // else evict one page and suqeeze out the space
    // evict any way
    evict();

    // something is going wrong
    if (ram.size() == 0)
    {
      throw new runtime_error("out of memory");
    }
    // it's quite silly that there's no pop only pop back...
    // so the best idea is to use the last one
    page->bytes = this->ram[this->ram.size() - 1];
    this->ram.pop_back();

    page->remaining = this->pageSize;

    // read from file
    int fd = this->fileTable[page->table];
    lseek(fd, page->pageIndex * this->pageSize, SEEK_SET);
    read(fd, page->bytes, this->pageSize);

    this->clock.at(this->clockHand) = page;
  }

  // update the "do not kill bit"
  page->doNotKill = true;
  // move the clockhand to the next position
  this->rotate();
}

// say goodbye to somebody on clock
// free the meory and points clock hand to this unit
void MyDB_BufferManager::evict()
{
  while (true)
  {
    auto currentPage = this->clock.at(this->clockHand);
    if (currentPage == nullptr)
    // this block is not initialized, good
    // nullptr means there's still place left on ram
    {
      return;
    }
    if (currentPage->doNotKill)
    // second chance given
    {
      currentPage->doNotKill = false;
    }
    else if (!currentPage->pinned)
    // preserve pinned page, else say goodbye
    {
      if (currentPage->bytes == nullptr)
      // this page self destructed for some reason
      // probably a enon page
      {
        return;
      }
      if (currentPage->dirty)
      // dirty page write back
      {
        int fd = this->fileTable[currentPage->table];
        lseek(fd, currentPage->pageIndex * pageSize, SEEK_SET);
        write(fd, currentPage->bytes, pageSize);
        currentPage->dirty = false;
      }
      // needs to clear?
      ram.push_back(currentPage->bytes);
      currentPage->bytes = nullptr;
      // the page itself could still be kept, for future use
      // but it's memory is gone, and need to read again from disk
      return;
    }
    this->rotate();
  }
}

void *MyDB_Page::getBytes(MyDB_PagePtr self)
{
  this->manager->retrivePage(self);
  return this->bytes;
}

void MyDB_Page::setDirty()
{
  this->dirty = true;
}

MyDB_Page::MyDB_Page(MyDB_TablePtr table, size_t pageIndex, MyDB_BufferManager *manager, bool pinned)
    : bytes(nullptr), doNotKill(false), dirty(false), pinned(pinned), manager(manager), table(table), pageIndex(pageIndex), ref(0), remaining(0) {}

MyDB_Page::~MyDB_Page() {}

void MyDB_Page::removeRef(MyDB_PagePtr self)
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

void MyDB_Page::addRef()
{
  this->ref++;
}

#endif
