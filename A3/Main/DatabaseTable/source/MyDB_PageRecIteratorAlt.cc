

/****************************************************
** COPYRIGHT 2016, Chris Jermaine, Rice University **
**                                                 **
** The MyDB Database System, COMP 530              **
** Note that this file contains SOLUTION CODE for  **
** A2.  You should not be looking at this file     **
** unless you have completed A2!                   **
****************************************************/

#ifndef PAGE_REC_ITER_ALT_C
#define PAGE_REC_ITER_ALT_C

#include "MyDB_PageRecIteratorAlt.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageType.h"

void MyDB_PageRecIteratorAlt ::getCurrent(MyDB_RecordPtr intoMe)
{
	void *pos = bytesConsumed + (char *)myPage->getBytes();
	void *nextPos = intoMe->fromBinary(pos);
	nextRecSize = ((char *)nextPos) - ((char *)pos);
}

void *MyDB_PageRecIteratorAlt ::getCurrentPointer()
{
	return bytesConsumed + (char *)myPage->getBytes();
}

bool MyDB_PageRecIteratorAlt ::advance()
{
	PageMeta *pageHeader = castPageHeader(myPage);
	if (nextRecSize == -1)
	{
		cout << "You can't call advance without calling getCurrent!!\n";
		exit(1);
	}
	bytesConsumed += nextRecSize;
	nextRecSize = -1;
	return bytesConsumed != pageHeader->numBytesUsed + sizeof(PageMeta);
}

MyDB_PageRecIteratorAlt ::MyDB_PageRecIteratorAlt(MyDB_PageHandle myPageIn)
{
	bytesConsumed = sizeof(PageMeta);
	myPage = myPageIn;
	nextRecSize = 0;
}

MyDB_PageRecIteratorAlt ::~MyDB_PageRecIteratorAlt() {}

#endif
