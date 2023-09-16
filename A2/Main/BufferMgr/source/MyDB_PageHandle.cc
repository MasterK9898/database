
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include "MyDB_PageHandle.h"
#include "MyDB_BufferManager.h"

void *MyDB_PageHandleBase ::getBytes()
{
  this->page->manager->retrivePage(this->page);
  return this->page->bytes;
}

void MyDB_PageHandleBase ::wroteBytes()
{
  this->page->dirty = true;
}

MyDB_PageHandleBase ::~MyDB_PageHandleBase()
{
  this->page->removeRef();
}

MyDB_PageHandleBase ::MyDB_PageHandleBase(MyDB_PagePtr page) : page(page)
{
  this->page->addRef();
}

void MyDB_PageHandleBase::unPin()
{
  this->page->pinned = false;
}

size_t MyDB_PageHandleBase::getPageSize()
{
  return this->page->manager->pageSize;
}

#endif
