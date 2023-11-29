
#ifndef PAGE_HANDLE_H
#define PAGE_HANDLE_H

#include <memory>
#include <string>
#include <iostream>
#include "Page.h"
#include "Table.h"

// page handles are basically smart pointers
using namespace std;
class PageHandleBase;

typedef shared_ptr<Page> PagePtr;
typedef shared_ptr<PageHandleBase> PageHandle;

class PageHandleBase
{

public:
	// THESE METHODS MUST BE IMPLEMENTED WITHOUT CHANGING THE DEFINITION

	// access the raw bytes in this page... if the page is not currently
	// in the buffer, then the contents of the page are loaded from
	// secondary storage.
	void *getBytes();

	// let the page know that we have written to the bytes.  Must always
	// be called once the page's bytes have been written.  If this is not
	// called, then the page will never be marked as dirty, and the page
	// will never be written to disk.
	void wroteBytes();

	// There are no more references to the handle when this is called...
	// this should decrmeent a reference count to the number of handles
	// to the particular page that it references.  If the number of
	// references to a pinned page goes down to zero, then the page should
	// become unpinned.
	~PageHandleBase();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS

	// init the page handle
	PageHandleBase(PagePtr page);

	// unpin a apge
	void unPin();

private:
	// YOUR CODE HERE

	// pointer to page
	PagePtr page;
};

#endif
