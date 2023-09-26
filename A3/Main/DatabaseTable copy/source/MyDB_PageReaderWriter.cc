
#ifndef PAGE_RW_C
#define PAGE_RW_C

#include <memory>
#include "../headers/MyDB_PageReaderWriter.h"
#include "MyDB_PageRecordIterator.h"
#include "MyDB_PageRecIteratorAlt.h"
#include "MyDB_PageListIteratorAlt.h"
#include "MyDB_PageType.h"
#include "RecordComparator.h"

using namespace std;

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(MyDB_TableReaderWriter &parent, int whichPage)
{

	// get the actual page
	this->page = parent.getManager()->getPage(parent.getTable(), whichPage);
}

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(bool pinned, MyDB_TableReaderWriter &parent, int whichPage)
{

	// get the actual page
	if (pinned)
	{
		this->page = parent.getManager()->getPinnedPage(parent.getTable(), whichPage);
	}
	else
	{
		this->page = parent.getManager()->getPage(parent.getTable(), whichPage);
	}
}

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(MyDB_BufferManager &parent)
{
	this->page = parent.getPage();
	this->clear();
}

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(bool pinned, MyDB_BufferManager &parent)
{

	if (pinned)
	{
		this->page = parent.getPinnedPage();
	}
	else
	{
		this->page = parent.getPage();
	}
	clear();
}

MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_PageHandle whichPage) : page(whichPage) {}

MyDB_PageReaderWriter::~MyDB_PageReaderWriter() {}

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

MyDB_RecordIteratorAltPtr getIteratorAlt(vector<MyDB_PageReaderWriter> &forUs)
{
	return make_shared<MyDB_PageListIteratorAlt>(forUs);
}

MyDB_RecordIteratorPtr MyDB_PageReaderWriter ::getIterator(MyDB_RecordPtr iterateIntoMe)
{
	return make_shared<MyDB_PageRecordIterator>(this->page, iterateIntoMe);
}

MyDB_RecordIteratorAltPtr MyDB_PageReaderWriter ::getIteratorAlt()
{
	return make_shared<MyDB_PageRecIteratorAlt>(this->page);
}

void MyDB_PageReaderWriter ::setType(MyDB_PageType toMe)
{
	PageMeta *pageHeader = castPageHeader(this->page);
	pageHeader->type = toMe;
	this->page->wroteBytes();
}

void *MyDB_PageReaderWriter ::appendAndReturnLocation(MyDB_RecordPtr appendMe)
{
	PageMeta *pageHeader = castPageHeader(this->page);
	void *recLocation = pageHeader->numBytesUsed + sizeof(PageMeta) + (char *)this->getBytes();
	if (append(appendMe))
		return recLocation;
	else
		return nullptr;
}

bool MyDB_PageReaderWriter ::append(MyDB_RecordPtr appendMe)
{
	size_t pageSize = this->getPageSize();

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

void MyDB_PageReaderWriter ::
		sortInPlace(function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	size_t pageSize = this->getPageSize();

	void *temp = malloc(pageSize);
	memcpy(temp, this->page->getBytes(), pageSize);

	// first, read in the positions of all of the records
	vector<void *> positions;

	PageMeta *pageHeader = castPageHeader(this->page);
	size_t currentUsage = pageHeader->numBytesUsed + sizeof(PageMeta);

	// this basically iterates through all of the records on the page
	int bytesConsumed = sizeof(size_t) * 2;
	while (bytesConsumed != currentUsage)
	{
		void *pos = bytesConsumed + (char *)temp;
		positions.push_back(pos);
		void *nextPos = lhs->fromBinary(pos);
		bytesConsumed += ((char *)nextPos) - ((char *)pos);
	}

	// and now we sort the vector of positions, using the record contents to build a comparator
	RecordComparator myComparator(comparator, lhs, rhs);
	std::stable_sort(positions.begin(), positions.end(), myComparator);

	// and write the guys back
	pageHeader->numBytesUsed = 0;
	this->page->wroteBytes();
	for (void *pos : positions)
	{
		lhs->fromBinary(pos);
		append(lhs);
	}

	free(temp);
}

MyDB_PageReaderWriterPtr MyDB_PageReaderWriter ::
		sort(function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{

	// first, read in the positions of all of the records
	vector<void *> positions;

	PageMeta *pageHeader = castPageHeader(this->page);
	size_t currentUsage = pageHeader->numBytesUsed + sizeof(PageMeta);

	// this basically iterates through all of the records on the page
	int bytesConsumed = sizeof(size_t) * 2;
	while (bytesConsumed != currentUsage)
	{
		void *pos = bytesConsumed + (char *)this->page->getBytes();
		positions.push_back(pos);
		void *nextPos = lhs->fromBinary(pos);
		bytesConsumed += ((char *)nextPos) - ((char *)pos);
	}

	// and now we sort the vector of positions, using the record contents to build a comparator
	RecordComparator myComparator(comparator, lhs, rhs);
	std::stable_sort(positions.begin(), positions.end(), myComparator);

	// and now create the page to return
	MyDB_PageReaderWriterPtr returnVal = make_shared<MyDB_PageReaderWriter>(this->page->getParent());
	returnVal->clear();

	// loop through all of the sorted records and write them out
	for (void *pos : positions)
	{
		lhs->fromBinary(pos);
		returnVal->append(lhs);
	}

	return returnVal;
}

size_t MyDB_PageReaderWriter ::getPageSize()
{
	return this->page->getPageSize();
	;
}

void *MyDB_PageReaderWriter ::getBytes()
{
	return this->page->getBytes();
}

#endif
