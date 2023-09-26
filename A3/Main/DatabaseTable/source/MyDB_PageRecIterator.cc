

/****************************************************
** COPYRIGHT 2016, Chris Jermaine, Rice University **
**                                                 **
** The MyDB Database System, COMP 530              **
** Note that this file contains SOLUTION CODE for  **
** A2.  You should not be looking at this file     **
** unless you have completed A2!                   **
****************************************************/

#ifndef PAGE_REC_ITER_C
#define PAGE_REC_ITER_C

#include "MyDB_PageRecIterator.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageType.h"
#include <iostream>

void MyDB_PageRecIterator ::getNext()
{
	if (this->hasNext())
	{
		PageMeta *pageHeader = castPageHeader(myPage);
		// do the pointer arrithmetic directly, no need to deref
		void *pos = (void *)(pageHeader->recs + bytesConsumed);
		myRec->fromBinary(pos);
		bytesConsumed += myRec->getBinarySize();
	}
}

void *MyDB_PageRecIterator ::getCurrentPointer()
{
	return bytesConsumed + (char *)myPage->getBytes();
}

bool MyDB_PageRecIterator ::hasNext()
{
	PageMeta *pageHeader = castPageHeader(myPage);
	std::cout << " current usage " << bytesConsumed << " total " << pageHeader->numBytesUsed << std::endl;
	return (bytesConsumed < pageHeader->numBytesUsed + sizeof(PageMeta));
}

MyDB_PageRecIterator ::MyDB_PageRecIterator(MyDB_PageHandle myPageIn, MyDB_RecordPtr myRecIn)
{
	bytesConsumed = sizeof(PageMeta);
	myPage = myPageIn;
	myRec = myRecIn;
}

MyDB_PageRecIterator ::~MyDB_PageRecIterator() {}

#endif
