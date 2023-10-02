

/****************************************************
** COPYRIGHT 2016, Chris Jermaine, Rice University **
**                                                 **
** The MyDB Database System, COMP 530              **
** Note that this file contains SOLUTION CODE for  **
** A2.  You should not be looking at this file     **
** unless you have completed A2!                   **
****************************************************/

#ifndef PAGE_RW_C
#define PAGE_RW_C

#include <algorithm>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageRecIterator.h"
#include "MyDB_PageRecIteratorAlt.h"
#include "MyDB_PageListIteratorAlt.h"
#include "RecordComparator.h"

// #define PAGE_TYPE *((MyDB_PageType *) ((char *) myPage->getBytes ()))
// #define NUM_BYTES_USED *((size_t *) (((char *) myPage->getBytes ()) + sizeof (size_t)))
// #define NUM_BYTES_LEFT (pageSize - NUM_BYTES_USED)

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(MyDB_TableReaderWriter &parent, int whichPage)
{

	// get the actual page
	myPage = parent.getBufferMgr()->getPage(parent.getTable(), whichPage);
	pageSize = parent.getBufferMgr()->getPageSize();
}

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(bool pinned, MyDB_TableReaderWriter &parent, int whichPage)
{

	// get the actual page
	if (pinned)
	{
		myPage = parent.getBufferMgr()->getPinnedPage(parent.getTable(), whichPage);
	}
	else
	{
		myPage = parent.getBufferMgr()->getPage(parent.getTable(), whichPage);
	}
	pageSize = parent.getBufferMgr()->getPageSize();
}

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(MyDB_BufferManager &parent)
{
	myPage = parent.getPage();
	pageSize = parent.getPageSize();
	clear();
}

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(bool pinned, MyDB_BufferManager &parent)
{

	if (pinned)
	{
		myPage = parent.getPinnedPage();
	}
	else
	{
		myPage = parent.getPage();
	}
	pageSize = parent.getPageSize();
	clear();
}

void MyDB_PageReaderWriter ::clear()
{
	PageMeta *pageHeader = castPageHeader(myPage);
	pageHeader->type = MyDB_PageType::RegularPage;
	pageHeader->numBytesUsed = 0;
	myPage->wroteBytes();
}

MyDB_PageType MyDB_PageReaderWriter ::getType()
{
	PageMeta *pageHeader = castPageHeader(myPage);
	return pageHeader->type;
}

MyDB_RecordIteratorAltPtr getIteratorAlt(vector<MyDB_PageReaderWriter> &forUs)
{
	return make_shared<MyDB_PageListIteratorAlt>(forUs);
}

MyDB_RecordIteratorPtr MyDB_PageReaderWriter ::getIterator(MyDB_RecordPtr iterateIntoMe)
{
	return make_shared<MyDB_PageRecIterator>(myPage, iterateIntoMe);
}

MyDB_RecordIteratorAltPtr MyDB_PageReaderWriter ::getIteratorAlt()
{
	return make_shared<MyDB_PageRecIteratorAlt>(myPage);
}

void MyDB_PageReaderWriter ::setType(MyDB_PageType toMe)
{
	PageMeta *pageHeader = castPageHeader(myPage);
	pageHeader->type = toMe;
	myPage->wroteBytes();
}

void *MyDB_PageReaderWriter ::appendAndReturnLocation(MyDB_RecordPtr appendMe)
{
	PageMeta *pageHeader = castPageHeader(myPage);

	void *recLocation = pageHeader->numBytesUsed + (char *)myPage->getBytes();
	if (append(appendMe))
		return recLocation;
	else
		return nullptr;
}

bool MyDB_PageReaderWriter ::append(MyDB_RecordPtr appendMe)
{
	PageMeta *pageHeader = castPageHeader(myPage);

	size_t recSize = appendMe->getBinarySize();

	size_t currentUsage = pageHeader->numBytesUsed + sizeof(PageMeta);

	if (recSize + currentUsage > pageSize)
		return false;

	// do the aristhmetic again
	char *nextPos = pageHeader->recs + pageHeader->numBytesUsed;
	// write in
	appendMe->toBinary((void *)nextPos);
	// up date the offset
	pageHeader->numBytesUsed += appendMe->getBinarySize();

	myPage->wroteBytes();
	return true;
}

void MyDB_PageReaderWriter ::
		sortInPlace(function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	PageMeta *pageHeader = castPageHeader(myPage);

	void *temp = malloc(pageSize);
	memcpy(temp, myPage->getBytes(), pageSize);

	// we need another page header for the temp one
	PageMeta *pageHeaderTemp = (PageMeta *)temp;

	// first, read in the positions of all of the records
	vector<void *> positions;

	// this basically iterates through all of the records on the page
	int bytesConsumed = 0;
	while (bytesConsumed != pageHeaderTemp->numBytesUsed)
	{
		void *pos = (void *)(pageHeaderTemp->recs + bytesConsumed);
		positions.push_back(pos);
		void *nextPos = lhs->fromBinary(pos);
		bytesConsumed += ((char *)nextPos) - ((char *)pos);
	}

	// and now we sort the vector of positions, using the record contents to build a comparator
	RecordComparator myComparator(comparator, lhs, rhs);
	std::stable_sort(positions.begin(), positions.end(), myComparator);

	// and write the guys back
	pageHeader->numBytesUsed = 0;
	myPage->wroteBytes();
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
	PageMeta *pageHeader = castPageHeader(myPage);
	// first, read in the positions of all of the records
	vector<void *> positions;

	// this basically iterates through all of the records on the page
	int bytesConsumed = 0;
	while (bytesConsumed != pageHeader->numBytesUsed)
	{
		void *pos = (void *)(pageHeader->recs + bytesConsumed);
		positions.push_back(pos);
		void *nextPos = lhs->fromBinary(pos);
		bytesConsumed += ((char *)nextPos) - ((char *)pos);
	}

	// and now we sort the vector of positions, using the record contents to build a comparator
	RecordComparator myComparator(comparator, lhs, rhs);
	std::stable_sort(positions.begin(), positions.end(), myComparator);

	// and now create the page to return
	MyDB_PageReaderWriterPtr returnVal = make_shared<MyDB_PageReaderWriter>(myPage->getParent());
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
	return pageSize;
}

void *MyDB_PageReaderWriter ::getBytes()
{
	return myPage->getBytes();
}

#endif
