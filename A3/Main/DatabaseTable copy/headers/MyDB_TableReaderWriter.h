
#ifndef TABLE_RW_H
#define TABLE_RW_H

#include <memory>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_BufferManager.h"
#include "MyDB_Record.h"
#include "MyDB_RecordIterator.h"
#include "MyDB_Table.h"

// create a smart pointer for the catalog
using namespace std;

class MyDB_TableReaderWriter;
class MyDB_PageReaderWriter;

typedef shared_ptr<MyDB_TableReaderWriter> MyDB_TableReaderWriterPtr;
typedef shared_ptr<MyDB_PageReaderWriter> MyDB_PageReaderWriterPtr;

class MyDB_TableReaderWriter
{

public:
	// ANYTHING ELSE YOU NEED HERE

	// create a table reader/writer for the specified table, using the specified
	// buffer manager
	MyDB_TableReaderWriter(MyDB_TablePtr forMe, MyDB_BufferManagerPtr myBuffer);

	// gets an empty record from this table
	MyDB_RecordPtr getEmptyRecord();

	// append a record to the table
	void append(MyDB_RecordPtr appendMe);

	// return an itrator over this table... each time returnVal->next () is
	// called, the resulting record will be placed into the record pointed to
	// by iterateIntoMe
	MyDB_RecordIteratorPtr getIterator(MyDB_RecordPtr iterateIntoMe);

	// gets an instance of an alternate iterator over the page; this iterator
	// works on a range of pages in the file, and iterates from lowPage through
	// highPage inclusive
	MyDB_RecordIteratorAltPtr getIteratorAlt(int lowPage, int highPage);

	// gets an instance of an alternate iterator over the table... this is an
	// iterator that has the alternate getCurrent ()/advance () interface
	MyDB_RecordIteratorAltPtr getIteratorAlt();

	// load a text file into this table... overwrites the current contents
	void loadFromTextFile(string fromMe);

	// dump the contents of this table into a text file
	void writeIntoTextFile(string toMe);

	// access the i^th page in this file
	MyDB_PageReaderWriter operator[](size_t i);

	// access the last page in the file
	MyDB_PageReaderWriter last();

	// get the current buffer manager
	MyDB_BufferManagerPtr getManager();

	// get the current table
	MyDB_TablePtr getTable();

private:
	// ANYTHING YOU NEED HERE

	// current table
	MyDB_TablePtr table;
	// current buffer manager
	MyDB_BufferManagerPtr manager;

	// get the target page's reader writer, clean if needed
	MyDB_PageReaderWriterPtr getPageReaderWriter(int i, bool clean);
};

#endif
