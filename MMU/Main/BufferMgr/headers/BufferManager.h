
#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include <vector>
#include <unordered_map>
#include <utility>
#include <memory>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include "Page.h"
#include "PageHandle.h"
#include "Table.h"

using namespace std;

class BufferManager;
class PageHandleBase;

typedef shared_ptr<BufferManager> BufferManagerPtr;
// A hash function used to hash the pair
struct hashPair
{
	std::size_t operator()(const std::pair<TablePtr, long> &p) const
	{
		auto ptr_hash = std::hash<void *>{}(p.first.get());
		auto long_hash = std::hash<long>{}(p.second);

		return ptr_hash ^ long_hash;
	}
};

class BufferManager
{

public:
	// THESE METHODS MUST APPEAR AND THE PROTOTYPES CANNOT CHANGE!

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle
	// to that already-buffered page should be returned
	// return the page, but the ram is currently not arranged
	PageHandle getPage(TablePtr whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular
	// table
	PageHandle getPage();

	// gets the i^th page in the table whichTable... the only difference
	// between this method and getPage (whicTable, i) is that the page will be
	// pinned in RAM; it cannot be written out to the file
	PageHandle getPinnedPage(TablePtr whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	PageHandle getPinnedPage();

	// un-pins the specified page
	void unpin(PageHandle unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	BufferManager(size_t pageSize, size_t numPages, string tempFile);

	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~BufferManager();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS

private:
	friend class PageHandleBase;
	friend class Page;
	// YOUR STUFF HERE

	// use a whole chunck of space is cooler
	// the idea is to map all the pages on the clock directly to the pointers by calculation
	void *memory;

	// wheather the clock is fiiled up (to determine the init reference value)
	bool initialized;

	// clock "face"
	vector<PagePtr> clock;

	// current position of the clock hand
	size_t clockHand;

	// size of page
	size_t pageSize;

	// number of pages
	size_t numPages;

	// the page table, from table and index to the page
	std::unordered_map<pair<TablePtr, size_t>, PagePtr, hashPair> pageTable;

	// keep the file for anonymous pages
	string tempFile;

	// keep track of the anonymous page index
	size_t tempIndex;

	// maintain a file table for faster performance
	std::unordered_map<TablePtr, int> fileTable;

	// say goodbye to somebody on clock
	// free the memory and return points clock hand to this unit
	size_t evict();

	// get the page read on ram
	void retrivePage(PagePtr page);

	// wrtie back page
	void writeBackPage(PagePtr page);

	// open the file if it is not yet opened, return the fd
	int openFile(TablePtr table);

	// unify the logic of getting normal page
	PageHandle getNormalPage(TablePtr whichTable, long i, bool pinned);

	// unify the logic of getting anon page
	PageHandle getAnonPage(bool pinned);
};

#endif
