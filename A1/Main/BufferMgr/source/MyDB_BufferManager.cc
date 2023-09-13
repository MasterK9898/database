
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
#include <iostream>
#include "MyDB_Page.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include "MyDB_BufferManager.h"

using namespace std;

MyDB_PageHandle MyDB_BufferManager ::getPage(MyDB_TablePtr whichTable, long i)
{
  return this->getNormalPage(whichTable, i, false);
}

MyDB_PageHandle MyDB_BufferManager ::getPage()
{
  return this->getAnonPage(false);
}

MyDB_PageHandle MyDB_BufferManager ::getPinnedPage(MyDB_TablePtr whichTable, long i)
{
  return this->getNormalPage(whichTable, i, true);
}

MyDB_PageHandle MyDB_BufferManager ::getPinnedPage()
{
  return this->getAnonPage(true);
}

void MyDB_BufferManager ::unpin(MyDB_PageHandle unpinMe)
{
  unpinMe->unPin();
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
      throw std::runtime_error("out of memory");
    }
    // it's quite silly that there's no pop only pop back...
    // so the best idea is to use the last one
    page->bytes = this->ram.back();
    this->ram.pop_back();

    // read from file
    int fd = this->fileTable[page->table];
    lseek(fd, page->pageIndex * this->pageSize, SEEK_SET);
    read(fd, page->bytes, this->pageSize);

    // update
    this->clock.at(this->clockHand) = page;
  }

  // update the "do not kill bit" according to stage
  page->doNotKill = this->initialized;
  // move the clockhand to the next position
  this->rotate();
}

// say goodbye to somebody on clock
// free the meory and points clock hand to this unit
void MyDB_BufferManager::evict()
{
  // if there is still ram, then it is better to evict the useless page
  bool outOfRam = this->ram.size() == 0;

  size_t pinnedCount = 0;
  while (true)
  {
    auto currentPage = this->clock.at(this->clockHand);

    if (currentPage == nullptr || currentPage->bytes == nullptr)
    // this block is not initialized, good
    // nullptr means there's still place left on ram
    {
      return;
    }
    if (outOfRam)
    // only when out of ram need to consider evciting
    {
      if (currentPage->doNotKill)
      // second chance given
      {
        currentPage->doNotKill = false;
      }
      else if (currentPage->pinned)
      // preserve pinned page
      {
        pinnedCount++;
        if (pinnedCount == this->numPages)
        {
          throw std::runtime_error("dude, all the pages are pinned");
        }
      }
      else
      // say goodbye
      {
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
        // say goodbye
        this->clock.at(this->clockHand) = nullptr;
        // the page itself could still be kept, for future use
        // but it's memory is gone, and need to read again from disk
        return;
      }
    }

    this->rotate();
    if (this->clockHand == 0)
    // a new round means the clock is full
    {
      this->initialized = true;
    }
  }
}

void MyDB_BufferManager::rotate()
{
  this->clockHand++;
  this->clockHand %= this->numPages;
}

MyDB_PageHandle MyDB_BufferManager::getNormalPage(MyDB_TablePtr whichTable, long i, bool pinned)
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
    auto newPage = make_shared<MyDB_Page>(whichTable, i, this, pinned);
    this->pageTable[key] = newPage;
    return make_shared<MyDB_PageHandleBase>(newPage);
  }
  else
  // get from table
  {
    MyDB_PagePtr page = this->pageTable[key];
    if (pinned)
    // update pinned if needed
    {
      page->pinned = true;
    }
    return make_shared<MyDB_PageHandleBase>(page);
  }
}

MyDB_PageHandle MyDB_BufferManager::getAnonPage(bool pinned)
{
  // under radar, no record
  auto anonPage = make_shared<MyDB_Page>(nullptr, this->tempIndex++, this, pinned);
  return make_shared<MyDB_PageHandleBase>(anonPage);
}

MyDB_Page::MyDB_Page(MyDB_TablePtr table, size_t pageIndex, MyDB_BufferManager *manager, bool pinned)
    : bytes(nullptr), doNotKill(false), dirty(false), pinned(pinned), manager(manager), table(table), pageIndex(pageIndex), ref(0) {}

MyDB_Page::~MyDB_Page()
{
  this->selfEvict();
}

void MyDB_Page::removeRef()
{
  this->ref--;
  if (ref == 0)
  {
    // no longer pinned
    this->pinned = false;

    if (this->table == nullptr)
    // anonymous page shall be destroyed when no reference
    {
      this->selfEvict();
    }
  }
}

void MyDB_Page::addRef()
{
  this->ref++;
}

void MyDB_Page::selfEvict()
{
  if (this->bytes != nullptr)
  // give back the memory, if there is any
  {
    this->manager->ram.push_back(this->bytes);
    this->bytes = nullptr;
  }
  this->doNotKill = false;
}

#endif