
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include <string>
#include <vector>
#include <unordered_map>
#include <utility>
#include <memory>
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

size_t MyDB_BufferManager::getPageSize()
{
  return this->pageSize;
}

MyDB_BufferManager ::MyDB_BufferManager(size_t pageSize, size_t numPages, string tempFile) : clockHand(0), pageSize(pageSize), numPages(numPages), tempFile(tempFile), tempIndex(0)
{
  // init the clock
  clock.resize(numPages, nullptr);
  // create the memory
  this->memory = malloc(pageSize * numPages);
  // open the temp file
  this->openFile(nullptr);

  // if malloc failed, then the initialization failed
  if (this->memory == nullptr)
  {
    throw std::runtime_error("Dude, there's not enough memory for buffering, maybe you shall buy a better machine?");
  }
}

MyDB_BufferManager ::~MyDB_BufferManager()
{
  // write back all pages
  for (auto pair : this->pageTable)
  {
    auto currentPage = pair.second;

    this->writeBackPage(currentPage);
  }

  // free all memory
  free(this->memory);

  // close all file
  for (auto pair : this->fileTable)
  {
    int fd = pair.second;
    close(fd);
  }

  // say goodbye the temp file
  remove(tempFile.c_str());
}

void MyDB_BufferManager::retrivePage(MyDB_PagePtr page)
{
  if (page->bytes == nullptr)
  // check the pointer to see if it is buffered
  {
    size_t evictIndex = evict();

    // calculate the byte position by offset
    page->bytes = (void *)((char *)this->memory + evictIndex * this->pageSize);

    // read from file
    int fd = this->openFile(page->table);
    lseek(fd, page->pageIndex * this->pageSize, SEEK_SET);
    read(fd, page->bytes, this->pageSize);

    // update
    this->clock.at(evictIndex) = page;
  }

  // update the "do not kill bit" according to stage
  if (this->initialized)
  {
    page->doNotKill = true;
  }
}

void MyDB_BufferManager::writeBackPage(MyDB_PagePtr page)
{
  // the page itself could still be kept, for future use
  // but it's memory is gone, and need to read again from disk
  if (page->bytes != nullptr)
  // nothing todo if the page has no bytes at all
  {
    if (page->dirty)
    // dirty page write back
    {
      int fd = this->openFile(page->table);
      lseek(fd, page->pageIndex * pageSize, SEEK_SET);
      write(fd, page->bytes, pageSize);
      page->dirty = false;
    }
    page->bytes = nullptr;
  }
}

// encapsulats the file table away from the outside, proxy pattern
int MyDB_BufferManager::openFile(MyDB_TablePtr whichTable)
{
  if (this->fileTable.count(whichTable) == 0)
  // open the file and store it onto the table if it's not
  {
    // for null pointers (anon page case), use the global temp file name
    string fileName = whichTable == nullptr ? this->tempFile : whichTable->getStorageLoc();
    int fd = open(fileName.c_str(), O_CREAT | O_RDWR, 0666);
    this->fileTable[whichTable] = fd;
    return fd;
  }
  else
  {
    return this->fileTable[whichTable];
  }
}

// say goodbye to somebody on clock
// free the meory and points clock hand to this unit
// the idea of clock hand is not visible to the out side, a layer of encapsulation
// no matter whether there's a "hole" in the clock ,the clock only turns to the next position
size_t MyDB_BufferManager::evict()
{

  // track the total rounds, prevent infinite loop
  size_t count = 0;

  while (true)
  {
    if (count++ > this->numPages * 2)
    // even in the worst case, this will mean a infinite loop
    {
      throw std::runtime_error("Dude, you got a infinite loop, are all the pages pinned?");
    }
    auto currentPage = this->clock.at(this->clockHand);

    if (currentPage == nullptr)
    // this unit on clock is empty
    // use it directly
    {
      break;
    }
    if (!currentPage->pinned)
    // pinned pages are ignored
    {
      if (currentPage->doNotKill)
      // second chance given
      {
        currentPage->doNotKill = false;
      }
      else
      // no chance, say goodbye
      {
        this->writeBackPage(currentPage);
        break;
      }
    }

    this->clockHand++;
    this->clockHand %= this->numPages;

    if (this->clockHand == 0)
    // if the clock reaches 0 again, then the clock is full
    {
      this->initialized = true;
    }
  }
  // at this stage, the target is figured out and dealt with

  // return the current clock position
  size_t evictIndex = this->clockHand;
  // next time, start from the next index
  this->clockHand++;
  this->clockHand %= this->numPages;
  return evictIndex;
}

MyDB_PageHandle MyDB_BufferManager::getNormalPage(MyDB_TablePtr whichTable, long i, bool pinned)
{
  this->openFile(whichTable);

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
  // under radar, no record on table
  auto anonPage = make_shared<MyDB_Page>(nullptr, this->tempIndex++, this, pinned);
  return make_shared<MyDB_PageHandleBase>(anonPage);
}

#endif
