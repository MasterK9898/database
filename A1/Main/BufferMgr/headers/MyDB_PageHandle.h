
#ifndef PAGE_HANDLE_H
#define PAGE_HANDLE_H

#include <memory>
#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include "MyDB_Page.h"
#include "MyDB_Table.h"

// page handles are basically smart pointers
using namespace std;
class MyDB_PageHandleBase;
typedef shared_ptr<MyDB_PageHandleBase> MyDB_PageHandle;

class MyDB_PageHandleBase
{

public:
	// THESE METHODS MUST BE IMPLEMENTED WITHOUT CHANGING THE DEFINITION

	// access the raw bytes in this page... if the page is not currently
	// in the buffer, then the contents of the page are loaded from
	// secondary storage.
	void *getBytes()
	{
		if (!this->page->getBuffered())
		// if the page is not currently in the buffer, then the contents of the page are loaded from secondary storage.
		{
			int fd = open(this->table->getStorageLoc().c_str(), O_CREAT | O_RDONLY | S_IRUSR | O_SYNC);
			size_t offset = this->page->getPageIndex() * this->pageSize;
			lseek(fd, offset, SEEK_SET);
			read(fd, this->page->getBytes(), this->pageSize);
			close(fd);
			this->page->setBuffered(true);
		}
		return this->page->getBytes();
	}

	// let the page know that we have written to the bytes.  Must always
	// be called once the page's bytes have been written.  If this is not
	// called, then the page will never be marked as dirty, and the page
	// will never be written to disk.
	void wroteBytes()
	{
		this->page->setDirty(true);
	}

	// There are no more references to the handle when this is called...
	// this should decrmeent a reference count to the number of handles
	// to the particular page that it references.  If the number of
	// references to a pinned page goes down to zero, then the page should
	// become unpinned.
	~MyDB_PageHandleBase()
	{
		this->page->removeRef();
		if (this->page->getRef() == 0)
		// If the number of references to a pinned page goes down to zero, then the page should become unpinned.
		{
			this->page->setPinned(false);
		}
	}

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS

	// init the page handle
	MyDB_PageHandleBase(MyDB_Page *page, size_t pageSize, MyDB_TablePtr whichTable) : table(whichTable), pageSize(pageSize)
	{
		this->page = page;
		this->page->addRef();
	}

	// return the current page
	MyDB_Page *getPage()
	{
		return this->page;
	}

private:
	// YOUR CODE HERE

	// total reference
	int count;
	// pointer to page
	MyDB_Page *page;
	// know which table it is
	MyDB_TablePtr table;
	// know the page size
	size_t pageSize;
};

#endif
