
#ifndef PAGE_RW_H
#define PAGE_RW_H

#include <memory>
#include "MyDB_PageType.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Page.h"

using namespace std;

class MyDB_PageReaderWriter;

typedef shared_ptr<MyDB_PageReaderWriter> MyDB_PageReaderWriterPtr;

// the header part in page
struct PageMeta
{
	// number of used bytes (excluding the header)
	size_t numBytesUsed;
	// type of the page
	MyDB_PageType type;
	// points to the starting of the first record
	// pageMeta->rec[0] = (char*)page->bytes + sizeof(PageMeta)
	char recs[0];
};

// cast the pageheader out from the page
PageMeta *castPageHeader(MyDB_PageHandle page)
{
	return (PageMeta *)page->getBytes();
};

// typedef PageMetaSize sizeof(PageMeta);

class MyDB_PageReaderWriter
{

public:
	// ANY OTHER METHODS YOU WANT HERE

	// empties out the contents of this page, so that it has no records in it
	// the type of the page is set to MyDB_PageType :: RegularPage
	void clear();

	// return an itrator over this page... each time returnVal->next () is
	// called, the resulting record will be placed into the record pointed to
	// by iterateIntoMe
	MyDB_RecordIteratorPtr getIterator(MyDB_RecordPtr iterateIntoMe);

	// appends a record to this page... return false is the append fails because
	// there is not enough space on the page; otherwise, return true
	bool append(MyDB_RecordPtr appendMe);

	// gets the type of this page... this is just a value from an ennumeration
	// that is stored within the page
	MyDB_PageType getType();

	// sets the type of the page
	void setType(MyDB_PageType toMe);

	// create the reader writer
	MyDB_PageReaderWriter(MyDB_PageHandle whichPage);

	~MyDB_PageReaderWriter();

private:
	// ANYTHING ELSE YOU WANT HERE

	// the current page that is being read and write
	MyDB_PageHandle page;
};

#endif
