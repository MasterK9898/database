
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
	std::size_t operator()(const std::pair<string, long> &p) const
	{
		auto ptr_hash = std::hash<string>{}(p.first);
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
		return this->getPage(whichTable, i, false);
	}

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular
	// table
	MyDB_PageHandle getPage()
	{
		return this->getPage(nullptr, -1);
	}

	// gets the i^th page in the table whichTable... the only difference
	// between this method and getPage (whicTable, i) is that the page will be
	// pinned in RAM; it cannot be written out to the file
	MyDB_PageHandle getPinnedPage(MyDB_TablePtr whichTable, long i)
	{
		return this->getPage(whichTable, i, false);
	}

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage()
	{
		return this->getPage(nullptr, -1, true);
	}

	// un-pins the specified page
	void unpin(MyDB_PageHandle unpinMe)
	{
		unpinMe->getPage()->setPinned(false);
	}

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager(size_t pageSize, size_t numPages, string tempFile)
			: pageSize(pageSize), clockHand(0), numPages(numPages), anonymousFile(tempFile)
	{
		// all clock position are set to be default
		clock.resize(numPages, &ClockUnit());
		// initialize the buffer
		this->buffer = calloc(pageSize, numPages);
	}

	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager()
	{
		// kill them all
		for (auto &pair : this->pageTable)
		{
			size_t clockIndex = pair.second;
			auto unit = this->clock.at(clockIndex);
			this->evict(unit);
		}

		// kill temperarry file
	}

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

	// IMPLEMENTING PAGE TABLE

	// the page table
	// table points to the position on clock
	std::unordered_map<pair<string, long>, size_t, hashPair> pageTable;

	// FOR ANONYMOUS FILES

	// keep track of the index
	size_t anonymousIndex;

	// keep the file for anonymous pages
	string anonymousFile;

	// evict a page for the new page, the clock hand will be the new place to evict
	// also return the clock hand
	size_t findVictim()
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
			return this->clockHand;
		}
		throw std::runtime_error("exception");
	}

	// unifies the logic getting any kind of page
	// if i is negative then it is considered as anonymous
	MyDB_PageHandle getPage(MyDB_TablePtr whichTable, long i, bool pinned)
	{
		// pass in negative i means anonymous
		bool anonymous = i < 0;

		long pageIndex = anonymous ? this->anonymousIndex : i;
		string filename = anonymous ? this->anonymousFile : whichTable->getStorageLoc();

		if (anonymous)
		// increment the index
		{
			this->anonymousIndex++;
		}

		auto key = make_pair(filename, pageIndex);

		if (pageTable.count(key))

		// if the page is currently being used (that is, the page is current buffered) a handle
		// to that already-buffered page should be returned
		{
			// get the index from table and retrive the unit
			size_t clockIndex = this->pageTable[key];
			ClockUnit *unit = this->clock.at(clockIndex);
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
			size_t clockIndex = this->findVictim();
			// get the victim
			ClockUnit *unit = this->clock.at(clockIndex);
			// the pointer at clock position i will be buffer + i * pageSize
			void *buffer = this->buffer + clockIndex * this->pageSize;

			// say goodbye
			this->evict(unit);

			// update the referenced bit according to initialization
			unit->referenced = this->initialized;

			// create page using the space, no need to worry about original page
			auto *page = new MyDB_Page(buffer, this->pageSize, pinned, pageIndex, filename);
			// create base
			MyDB_PageHandleBase *base = new MyDB_PageHandleBase(page, anonymous);
			// update the clock
			unit->base = base;
			// update page table
			this->pageTable[key] = this->clockHand;
			MyDB_PageHandle handle = make_shared<MyDB_PageHandleBase>(base);
			return handle;
		}
	}

	// say goodbye to the page in this unit
	void evict(ClockUnit *unit)
	{
		auto victim = unit->base;
		if (victim != nullptr)
		{
			auto victimPage = victim->getPage();
			// remove the victim from page table
			this->pageTable.erase({victimPage->getPageFilename(), victimPage->getPageIndex()});
			// remove the victim page
			delete victimPage;
			// remove the hander base
			delete victim;
		}
	}
};

#endif
