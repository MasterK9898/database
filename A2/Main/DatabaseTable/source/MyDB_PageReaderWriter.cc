
#ifndef PAGE_RW_C
#define PAGE_RW_C

#include <memory>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageRecordIterator.h"
#include "MyDB_PageType.h"

void MyDB_PageReaderWriter ::clear()
{
	this->setType(MyDB_PageType::RegularPage);
	PageMeta *pageHeader = castPageHeader(this->page);
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

	// calculate the current sonsumption status
	size_t currentUsage = pageHeader->numBytesUsed + sizeof(PageMeta);

	if (currentUsage + recSize > pageSize)
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

#endif
