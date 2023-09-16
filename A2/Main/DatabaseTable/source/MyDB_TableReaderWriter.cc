
#ifndef TABLE_RW_C
#define TABLE_RW_C

#include <fstream>
#include "MyDB_PageRecordIterator.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecordIterator.h"
#include "MyDB_TableReaderWriter.h"

using namespace std;

MyDB_TableReaderWriter ::MyDB_TableReaderWriter(MyDB_TablePtr forMe, MyDB_BufferManagerPtr myBuffer) : table(forMe), manager(myBuffer)
{
	if (table->lastPage() < 0)
	// initialize if not
	{
		table->setLastPage(0);
		this->getPageReaderWriter(0, true);
	}
}

MyDB_PageReaderWriter MyDB_TableReaderWriter ::operator[](size_t i)
{
	while (i > this->table->lastPage())
	// if goes beyond then we need more space, and stretch
	{
		this->table->setLastPage(this->table->lastPage() + 1);
	}

	// it's not returning reference, so wo need to create a whole new object and return it
	MyDB_PageHandle page = this->manager->getPage(this->table, i);
	return MyDB_PageReaderWriter(page);
}

MyDB_RecordPtr MyDB_TableReaderWriter ::getEmptyRecord()
{
	return make_shared<MyDB_Record>(this->table->getSchema());
}

MyDB_PageReaderWriter MyDB_TableReaderWriter ::last()
{
	// it's not returning reference, so wo need to create a whole new object and return it
	MyDB_PageHandle page = this->manager->getPage(this->table, this->table->lastPage());
	return MyDB_PageReaderWriter(page);
}

void MyDB_TableReaderWriter ::append(MyDB_RecordPtr appendMe)
{
	auto lastPage = this->getPageReaderWriter(this->table->lastPage(), false);

	if (!lastPage->append(appendMe))
	// if current insertion failed, insert a new page into the table
	{
		int newTableSize = this->table->lastPage() + 1;
		this->table->setLastPage(newTableSize);
		// get a new clean page, and append it
		lastPage = this->getPageReaderWriter(newTableSize, true);
		if (!lastPage->append(appendMe))
		// if failed, it's the creator's false, they allocate too tiny space for page
		{
			throw runtime_error("dude, insertion to new page failed, your page size is too small");
		}
	}
}

void MyDB_TableReaderWriter ::loadFromTextFile(string fromMe)
{
	string line;
	ifstream file(fromMe);
	if (file.is_open())
	{
		MyDB_RecordPtr rec = getEmptyRecord();
		while (getline(file, line))
		{
			rec->fromString(line);
			this->append(rec);
		}
		file.close();
	}
	// throwing error at this time seems to be too harsh?
	// else
	// {
	// 	throw new runtime_error("dude, cannot open this file");
	// }

	// all current record are overwritten = re initialize
	this->table->setLastPage(0);
	this->getPageReaderWriter(0, true);
}

MyDB_RecordIteratorPtr MyDB_TableReaderWriter ::getIterator(MyDB_RecordPtr iterateIntoMe)
{
	return make_shared<MyDB_TableRecordIterator>(this->table, iterateIntoMe, this);
}

void MyDB_TableReaderWriter ::writeIntoTextFile(string toMe)
{
	ofstream file(toMe);
	if (file.is_open())
	{
		MyDB_RecordPtr rec = getEmptyRecord();
		MyDB_RecordIteratorPtr iter = getIterator(rec);
		while (iter->hasNext())
		{
			iter->getNext();
			file << rec << endl;
		}
		file.close();
	}
	// throwing error at this time seems to be too harsh?
	// else
	// {
	// 	throw new runtime_error("dude, cannot open this file");
	// }
}

MyDB_PageReaderWriterPtr MyDB_TableReaderWriter::getPageReaderWriter(int i, bool clean)
{
	MyDB_PageHandle page = this->manager->getPage(this->table, i);
	auto readerWriter = make_shared<MyDB_PageReaderWriter>(page);
	if (clean)
	{
		readerWriter->clear();
	}
	return readerWriter;
}

#endif
