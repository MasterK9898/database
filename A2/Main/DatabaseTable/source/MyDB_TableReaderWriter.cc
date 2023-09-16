
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
		this->lastPage = this->getCleanPage(0);
	}
	else
	{
		this->lastPage = make_shared<MyDB_PageReaderWriter>(forMe->lastPage());
	}
}

MyDB_PageReaderWriter MyDB_TableReaderWriter ::operator[](size_t i)
{
	// if goes beyond then we need more space, and stretch
	while (i > this->table->lastPage())
	{
		int newTableSize = this->table->lastPage() + 1;
		this->table->setLastPage(newTableSize);
		this->lastPage = this->getCleanPage(newTableSize);
	}

	MyDB_PageHandle page = this->manager->getPage(this->table, i);
	return MyDB_PageReaderWriter(page);
}

MyDB_RecordPtr MyDB_TableReaderWriter ::getEmptyRecord()
{
	return make_shared<MyDB_Record>(this->table->getSchema());
}

MyDB_PageReaderWriter MyDB_TableReaderWriter ::last()
{
	MyDB_PageHandle page = this->manager->getPage(this->table, this->table->lastPage());
	return MyDB_PageReaderWriter(page);
}

void MyDB_TableReaderWriter ::append(MyDB_RecordPtr appendMe)
{
	MyDB_PageHandle page = this->manager->getPage(this->table, this->table->lastPage());
	if (this->lastPage->append(appendMe))
	// if current insertion failed, insert a new page into the table
	{
		int newTableSize = this->table->lastPage() + 1;
		this->table->setLastPage(newTableSize);
		this->lastPage = this->getCleanPage(newTableSize);
		this->lastPage->append(appendMe);
	}
}

void MyDB_TableReaderWriter ::loadFromTextFile(string fromMe)
{
	// all current record are overwritten
	// initialize
	this->table->setLastPage(0);
	this->lastPage = this->getCleanPage(0);

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
	else
	{
		throw new std::runtime_error("dude, cannot open this file");
	}
}

MyDB_RecordIteratorPtr MyDB_TableReaderWriter ::getIterator(MyDB_RecordPtr iterateIntoMe)
{
	return make_shared<MyDB_TableRecordIterator>(this->table, iterateIntoMe, *this);
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
	else
	{
		throw new std::runtime_error("dude, cannot open this file");
	}
}

MyDB_PageReaderWriterPtr MyDB_TableReaderWriter::getCleanPage(int i)
{
	MyDB_PageHandle page = this->manager->getPage(this->table, i);
	auto empty = make_shared<MyDB_PageReaderWriter>(page);
	empty->clear();
	return empty;
}

#endif
