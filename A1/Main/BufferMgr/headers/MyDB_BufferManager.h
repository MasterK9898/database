
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
#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include "MyDB_Page.h"

using namespace std;

class MyDB_BufferManager;
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
	MyDB_PageHandle getPage(MyDB_TablePtr whichTable, long i)
	{
		// open the file
		int fd = open(whichTable->getStorageLoc().c_str(), O_CREAT | O_RDWR);
		this->fileTable[whichTable] = fd;

		// get the key
		pair<MyDB_TablePtr, long> key = make_pair(whichTable, i);
		if (this->pageTable.count(key) == 0)
		// create one from scratch, insert to table
		{
			// it is not there, so create a page
			auto newPage = make_shared<MyDB_Page>(whichTable, i, this, true);
			this->pageTable[key] = newPage;
			return make_shared<MyDB_PageHandleBase>(newPage);
		}
		else
		// get from table
		{
			MyDB_PagePtr page = this->pageTable[key];
			return make_shared<MyDB_PageHandleBase>(page);
		}
	}

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular
	// table
	MyDB_PageHandle getPage()
	{
		auto anonPage = make_shared<MyDB_Page>(nullptr, this->anonymousIndex++, this, true);
		return make_shared<MyDB_PageHandleBase>(anonPage);
	}

	// gets the i^th page in the table whichTable... the only difference
	// between this method and getPage (whicTable, i) is that the page will be
	// pinned in RAM; it cannot be written out to the file
	MyDB_PageHandle getPinnedPage(MyDB_TablePtr whichTable, long i)
	{
		// open the file
		int fd = open(whichTable->getStorageLoc().c_str(), O_CREAT | O_RDWR);
		this->fileTable[whichTable] = fd;

		pair<MyDB_TablePtr, long> key = make_pair(whichTable, i);
		if (this->pageTable.count(key) == 0)
		// create one from scratch, insert to table
		{
			// it is not there, so create a page
			auto newPage = make_shared<MyDB_Page>(whichTable, i, this, true);
			this->pageTable[key] = newPage;
			return make_shared<MyDB_PageHandleBase>(newPage);
		}
		else
		// get from table
		{
			MyDB_PagePtr page = this->pageTable[key];
			// pin the page anyway
			page->pinned = true;
			return make_shared<MyDB_PageHandleBase>(page);
		}
	}

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage()
	{
		auto anonPage = make_shared<MyDB_Page>(nullptr, this->anonymousIndex++, *this, true);
		return make_shared<MyDB_PageHandleBase>(anonPage);
	}

	// un-pins the specified page
	void unpin(MyDB_PageHandle unpinMe)
	{
		unpinMe->getPage()->pinned = false;
	}

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager(size_t pageSize, size_t numPages, string tempFile)
			: clockHand(0), pageSize(pageSize), numPages(numPages), tempFile(tempFile), anonymousIndex(0)
	{
		for (size_t i = 0; i < numPages; i++)
		{
			ram.push_back(malloc(pageSize));
			clock.push_back(nullptr);
		}
		// open the file
		int fd = open(tempFile.c_str(), O_CREAT | O_RDWR);
		this->fileTable[nullptr] = fd;
	}

	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager()
	{
		for (auto pair : this->pageTable)
		{
			auto currentPage = pair.second;

			if (currentPage->bytes != nullptr)
			{

				if (currentPage->dirty)
				// dirty page write back
				{
					int fd = this->fileTable[currentPage->table];
					lseek(fd, currentPage->pageIndex * pageSize, SEEK_SET);
					write(fd, currentPage->bytes, pageSize);
					currentPage->dirty = false;
				}
				ram.push_back(currentPage->bytes);

				currentPage->bytes = nullptr;
			}
		}

		// free all memory
		for (auto ramUnit : this->ram)
		{
			free(ramUnit);
		}

		// close all file
		for (auto pair : this->fileTable)
		{
			int fd = pair.second;
			close(fd);
		}

		// say goodbye the the file
		remove(tempFile.c_str());
	}

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS

	void retrivePage(MyDB_PagePtr page)
	{
		if (page->bytes == nullptr)
		// check the pointer to see if it is buffered
		{
			// if there are empty ram space, arrange the space and put onto the clock

			// else evict one page and suqeeze out the space
			// evict any way
			evict();

			// something is going wrong
			if (ram.size() == 0)
			{
				throw new runtime_error("out of memory");
			}
			// it's quite silly that there's no pop only pop back...
			// so the best idea is to use the last one
			page->bytes = this->ram[this->ram.size() - 1];
			this->ram.pop_back();

			page->remaining = this->pageSize;

			// read from file
			int fd = this->fileTable[page->table];
			lseek(fd, page->pageIndex * this->pageSize, SEEK_SET);
			read(fd, page->bytes, this->pageSize);

			this->clock.at(this->clockHand) = page;
		}

		// update the "do not kill bit"
		page->doNotKill = true;
		// move the clockhand to the next position
		this->rotate();
	}

private:
	friend class MyDB_Page;
	// YOUR STUFF HERE
	vector<void *> ram;

	// IMPLEMENTING CLOCK

	// wheather the clock is fiiled up (to determine the init reference value)
	bool initialized;
	// clock "face"
	// TODO: ordered map seems to be a better choice
	vector<MyDB_PagePtr> clock;
	// current position of the clock hand
	size_t clockHand;
	// size of page
	size_t pageSize;
	// number of pages
	size_t numPages;

	// IMPLEMENTING PAGE TABLE

	// the page table
	// table points to the position on clock
	std::unordered_map<pair<MyDB_TablePtr, size_t>, MyDB_PagePtr, hashPair> pageTable;

	// FOR ANONYMOUS FILES

	// keep track of the index
	size_t anonymousIndex;

	// keep the file for anonymous pages
	string tempFile;

	// maintain a file table for faster performance
	std::unordered_map<MyDB_TablePtr, int> fileTable;

	// say goodbye to somebody on clock
	// free the meory and points clock hand to this unit
	void evict()
	{
		while (true)
		{
			auto currentPage = this->clock.at(this->clockHand);
			if (currentPage == nullptr)
			// this block is not initialized, good
			// nullptr means there's still place left on ram
			{
				return;
			}
			if (currentPage->doNotKill)
			// second chance given
			{
				currentPage->doNotKill = false;
			}
			else if (!currentPage->pinned)
			// preserve pinned page, else say goodbye
			{
				if (currentPage->bytes == nullptr)
				// this page self destructed for some reason
				// probably a enon page
				{
					return;
				}
				if (currentPage->dirty)
				// dirty page write back
				{
					int fd = this->fileTable[currentPage->table];
					lseek(fd, currentPage->pageIndex * pageSize, SEEK_SET);
					write(fd, currentPage->bytes, pageSize);
					currentPage->dirty = false;
				}
				// needs to clear?
				ram.push_back(currentPage->bytes);
				currentPage->bytes = nullptr;
				// the page itself could still be kept, for future use
				// but it's memory is gone, and need to read again from disk
				return;
			}
			this->rotate();
		}
	}

	// rotate the clock hand
	void rotate()
	{
		this->clockHand++;
		this->clockHand %= this->numPages;
	}

	// get page filename
	string getFile(MyDB_PagePtr page)
	{
		if (page->table == nullptr)
		{
			return this->tempFile;
		}
		else
		{
			return page->table->getStorageLoc();
		}
	}
};

#endif
