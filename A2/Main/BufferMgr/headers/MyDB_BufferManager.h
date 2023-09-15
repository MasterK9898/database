
#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include <vector>
#include <unordered_map>
#include <utility>
#include <memory>
#include <ctime>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include "MyDB_Page.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"

using namespace std;

class MyDB_BufferManager;
class MyDB_PageHandleBase;

typedef shared_ptr<MyDB_BufferManager> MyDB_BufferManagerPtr;
// A hash function used to hash the pair
struct hashPair
{
	std::size_t operator()(const std::pair<MyDB_TablePtr, long> &p) const
	{
		auto ptr_hash = std::hash<void *>{}(p.first.get());
		auto long_hash = std::hash<long>{}(p.second);

		return ptr_hash ^ long_hash;
	}
};

class MyDB_BufferManager
{

public:
	// THESE METHODS MUST APPEAR AND THE PROTOTYPES CANNOT CHANGE!

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle
	// to that already-buffered page should be returned
	// return the page, but the ram is currently not arranged
	MyDB_PageHandle getPage(MyDB_TablePtr whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular
	// table
	MyDB_PageHandle getPage();

	// gets the i^th page in the table whichTable... the only difference
	// between this method and getPage (whicTable, i) is that the page will be
	// pinned in RAM; it cannot be written out to the file
	MyDB_PageHandle getPinnedPage(MyDB_TablePtr whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage();

	// un-pins the specified page
	void unpin(MyDB_PageHandle unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager(size_t pageSize, size_t numPages, string tempFile);

	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS

	// get the page size
	size_t getPageSize();

private:
	friend class MyDB_PageHandleBase;
	friend class MyDB_Page;
	// YOUR STUFF HERE

	// use a whole chunck of space is cooler
	// the idea is to map all the pages on the clock directly to the pointers by calculation
	void *memory;

	// wheather the clock is fiiled up (to determine the init reference value)
	bool initialized;

	// clock "face"
	vector<MyDB_PagePtr> clock;

	// current position of the clock hand
	size_t clockHand;

	// size of page
	size_t pageSize;

	// number of pages
	size_t numPages;

	// the page table, from table and index to the page
	std::unordered_map<pair<MyDB_TablePtr, size_t>, MyDB_PagePtr, hashPair> pageTable;

	// keep the file for anonymous pages
	string tempFile;

	// keep track of the anonymous page index
	size_t tempIndex;

	// maintain a file table for faster performance
	std::unordered_map<MyDB_TablePtr, int> fileTable;

	// say goodbye to somebody on clock
	// free the memory and return points clock hand to this unit
	size_t evict();

	// get the page read on ram
	void retrivePage(MyDB_PagePtr page);

	// wrtie back page
	void writeBackPage(MyDB_PagePtr page);

	// open the file if it is not yet opened, return the fd
	int openFile(MyDB_TablePtr table);

	// unify the logic of getting normal page
	MyDB_PageHandle getNormalPage(MyDB_TablePtr whichTable, long i, bool pinned);

	// unify the logic of getting anon page
	MyDB_PageHandle getAnonPage(bool pinned);
};

#endif
