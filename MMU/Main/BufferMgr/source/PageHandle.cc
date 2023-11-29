
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include "PageHandle.h"
#include "BufferManager.h"

void *PageHandleBase ::getBytes()
{
  this->page->manager->retrivePage(this->page);
  return this->page->bytes;
}

void PageHandleBase ::wroteBytes()
{
  this->page->dirty = true;
}

PageHandleBase ::~PageHandleBase()
{
  this->page->removeRef();
}

PageHandleBase ::PageHandleBase(PagePtr page) : page(page)
{
  this->page->addRef();
}

void PageHandleBase::unPin()
{
  this->page->pinned = false;
}

#endif
