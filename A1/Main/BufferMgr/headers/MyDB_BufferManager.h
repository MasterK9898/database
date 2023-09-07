
#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include <vector>
#include <unordered_map>
#include <utility>
#include <memory>
#include <ctime>
#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include "MyDB_Page.h"

using namespace std;

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

struct ClockUnit
{
	bool referenced;
	MyDB_PageHandleBase *base;
	ClockUnit()
	{
		referenced = false;
		base = nullptr;
	};
	ClockUnit(bool referenced, MyDB_PageHandleBase base)
	{
		referenced = referenced;
		base = base;
	};
};

class MyDB_BufferManager
{

public:
	// THESE METHODS MUST APPEAR AND THE PROTOTYPES CANNOT CHANGE!

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle
	// to that already-buffered page should be returned
	MyDB_PageHandle getPage(MyDB_TablePtr whichTable, long i)
	{
		if (pageTable.count({whichTable, i}))
		// if the page is currently being used (that is, the page is current buffered) a handle
		// to that already-buffered page should be returned
		{
			size_t clockIndex = this->pageTable[{whichTable, i}];
			ClockUnit *unit = this->clock.at(this->clockHand);
			// update reference bit
			unit->referenced = true;
			// retrive the page
			MyDB_PageHandleBase *base = unit->base;
			size_t currentTime = time(0);
			base->getPage()->setTimeStamp(currentTime);
			// make the handle
			MyDB_PageHandle handle = make_shared<MyDB_PageHandleBase>(base);
			return handle;
		}
		else
		// create the page from scratch
		{
			this->evict();
			// get the victim
			ClockUnit *unit = this->clock.at(this->clockHand);
			// the pointer at clock position i will be buffer + i * pageSize
			void *buffer = this->buffer + this->clockHand * this->pageSize;

			auto victim = unit->base;
			if (victim != nullptr)
			{
				// remove the victim from page table
				this->pageTable.erase({whichTable, i});
				// remove the victim page
				delete victim->getPage();
				// remove the hander base
				delete victim;
			}
			// update the referenced bit according to initialization
			unit->referenced = this->initialized;

			// create page using the space, no need to worry about original page
			auto *page = new MyDB_Page(buffer, this->pageSize, false, this->clockHand, whichTable);
			// create base
			MyDB_PageHandleBase *base = new MyDB_PageHandleBase(page);
			// update the clock
			unit->base = base;
			// update page table
			this->pageTable[{whichTable, i}] = this->clockHand;
			MyDB_PageHandle handle = make_shared<MyDB_PageHandleBase>(base);
			return handle;
		}
	}

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular
	// table
	MyDB_PageHandle getPage()
	{
	}

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
	MyDB_BufferManager(size_t pageSize, size_t numPages, string tempFile) : pageSize(pageSize), clockHand(0), numPages(numPages)
	{
		// all clock position are set to be default
		clock.resize(numPages, &ClockUnit());
		// initialize the buffer
		this->buffer = calloc(pageSize, numPages);
	}

	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS

private:
	// YOUR STUFF HERE

	// the real buffer
	void *buffer;

	// IMPLEMENTING CLOCK

	// record of another chance
	std::vector<ClockUnit *> clock;
	// wheather the clock is fiiled up (to determine the init reference value)
	bool initialized;
	// current position of the clock hand
	size_t clockHand;
	// size of page
	size_t pageSize;
	// number of pages
	size_t numPages;

	// evict a page for the new page, the clock hand will be the new place to evict
	void evict()
	{
		// prevent infinite loop
		size_t pinnedCount = 0;
		while (true)
		{
			ClockUnit *unit = this->clock.at(this->clockHand);

			// increment and mod
			this->clockHand++;
			this->clockHand %= this->pageSize;

			// if a new cycle is reached, then the filling up stage is over
			if (this->clockHand == 0 && !this->initialized)
			{
				this->initialized = true;
			}

			if (unit->referenced)
			// second chance given
			{
				unit->referenced = false;
				continue;
			}
			auto currentPage = unit->base->getPage();
			bool pinned = currentPage->getPinned();
			if (pinned)
			// pinned page will be skipped
			{
				if (++pinnedCount == this->numPages)
				{
					// all pages are pinned, eviction failed
					throw std::runtime_error("all pages are pinned eviction failed");
				}
				continue;
			}
			// this pages will be evicted
			return;
		}
		throw std::runtime_error("exception");
	}

	// IMPLEMENTING PAGE TABLE

	// the page table
	// table points to the position on clock
	std::unordered_map<pair<MyDB_TablePtr, long>, size_t, hashPair> pageTable;
};

#endif
