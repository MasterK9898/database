
#ifndef PAGE_RW_C
#define PAGE_RW_C

#include <memory>
#include "../headers/MyDB_PageReaderWriter.h"
#include "../headers/MyDB_PageRecordIterator.h"
#include "MyDB_PageType.h"

using namespace std;

void MyDB_PageReaderWriter ::clear()
{
	PageMeta *pageHeader = castPageHeader(this->page);
	pageHeader->type = MyDB_PageType::RegularPage;
	pageHeader->numBytesUsed = 0;
	this->page->wroteBytes();
}

MyDB_PageType MyDB_PageReaderWriter ::getType()
{
	PageMeta *pageHeader = castPageHeader(this->page);
	return pageHeader->type;
}

MyDB_RecordIteratorPtr MyDB_PageReaderWriter ::getIterator(MyDB_RecordPtr iterateIntoMe)
{
	return make_shared<MyDB_PageRecordIterator>(this->page, iterateIntoMe);
}

void MyDB_PageReaderWriter ::setType(MyDB_PageType toMe)
{
	PageMeta *pageHeader = castPageHeader(this->page);
	pageHeader->type = toMe;
	this->page->wroteBytes();
}

bool MyDB_PageReaderWriter ::append(MyDB_RecordPtr appendMe)
{
	size_t pageSize = this->page->getPageSize();

	PageMeta *pageHeader = castPageHeader(this->page);
	size_t recSize = appendMe->getBinarySize();

	// calculate the current consumption status
	size_t currentUsage = pageHeader->numBytesUsed + sizeof(PageMeta);

	if ((currentUsage + recSize) > pageSize)
	// not enough space left
	{
		return false;
	}
	// do the aristhmetic again
	char *nextPos = pageHeader->recs + pageHeader->numBytesUsed;
	// write in
	appendMe->toBinary((void *)nextPos);
	// up date the offset
	pageHeader->numBytesUsed += appendMe->getBinarySize();

	this->page->wroteBytes();

	return true;
}

MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_PageHandle whichPage) : page(whichPage) {}

// constructor for a page in the same file as the parent
MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_TableReaderWriter &parent, int whichPage)
{
	// get the actual page
	this->page = parent.getParent()->getPage(parent.getTable(), whichPage);
	this->pageSize = parent.getParent()->getPageSize();
}

// constructor for a page that can be pinned, if desired
MyDB_PageReaderWriter::MyDB_PageReaderWriter(bool pinned, MyDB_TableReaderWriter &parent, int whichPage) {}

// constructor for an anonymous page
MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_BufferManager &parent) {}

// constructor for an anonymous page that can be pinned, if desired
MyDB_PageReaderWriter::MyDB_PageReaderWriter(bool pinned, MyDB_BufferManager &parent) {}

MyDB_PageReaderWriter::~MyDB_PageReaderWriter() {}

#endif
