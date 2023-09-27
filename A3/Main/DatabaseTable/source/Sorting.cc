
#ifndef SORT_C
#define SORT_C

#include <queue>
#include <memory>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"

using namespace std;

// most part of merge list and merge file are the same
// so I created this template method that accepts a function to do the inside work
void unifiedMergeHelper(vector<MyDB_RecordIteratorAltPtr> &mergeUs, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs, function<bool()> comparator, function<void(MyDB_RecordIteratorAltPtr)> inside)
{
	// bascially it's the leetcode question merge k sorted list

	// init the comparator
	auto comp = [lhs, rhs, comparator](const MyDB_RecordIteratorAltPtr leftIter, const MyDB_RecordIteratorAltPtr rightIter)
	{
		leftIter->getCurrent(lhs);
		rightIter->getCurrent(rhs);
		// by default priority queue is max heap, so we need to reverse the comparator
		return !comparator();
	};
	// init the priority queue
	priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, decltype(comp)>
			priorityQueue(comp);

	for (MyDB_RecordIteratorAltPtr iterator : mergeUs)
	{
		// we cannot risk to put empty iteators in, the comparator will crash
		if (iterator->advance())
		{
			priorityQueue.push(iterator);
		}
	}

	while (!priorityQueue.empty())
	{
		// we cannot do the get current and advance step inside the heap
		// because the sequence matters, we need to activate the comparison by insertion

		// it's quite silly that the pot does not have a return value
		auto iterator = priorityQueue.top();
		priorityQueue.pop();

		// time for the inside work
		inside(iterator);

		// push next in if possible
		if (iterator->advance())
		{
			priorityQueue.push(iterator);
		}
	}
}

void mergeIntoFile(MyDB_TableReaderWriter &sortIntoMe, vector<MyDB_RecordIteratorAltPtr> &mergeUs,
									 function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	// copy the lhs record as a helper to read and write
	MyDB_RecordPtr helper = make_shared<MyDB_Record>(lhs->getSchema());

	auto inside = [&sortIntoMe, helper](MyDB_RecordIteratorAltPtr iterator)
	{
		iterator->getCurrent(helper);
		sortIntoMe.append(helper);
	};

	unifiedMergeHelper(mergeUs, lhs, rhs, comparator, inside);
}

// merge k list at a time
vector<MyDB_PageReaderWriter> mergeIntoList(MyDB_BufferManagerPtr parent, vector<MyDB_RecordIteratorAltPtr> &mergeUs, function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	// init the result
	vector<MyDB_PageReaderWriter> result;
	// init the reader writer
	MyDB_PageReaderWriter current(*parent);
	// copy the lhs record as a helper to read and write
	MyDB_RecordPtr helper = make_shared<MyDB_Record>(lhs->getSchema());

	auto inside = [&current, &parent, &result, helper](MyDB_RecordIteratorAltPtr iterator)
	{
		iterator->getCurrent(helper);

		if (!current.append(helper))
		{
			// if we cannot append, then we need to create a new page
			result.push_back(current);
			// create a new page
			MyDB_PageReaderWriter newPage(*parent);
			// swap current page into new page
			current = newPage;
			// append the target, this time it must be successful
			current.append(helper);
		}
	};

	unifiedMergeHelper(mergeUs, lhs, rhs, comparator, inside);

	// don't forget the current page
	result.push_back(current);

	return result;
}

void sort(int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe,
					function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	size_t numPages = sortMe.getNumPages();

	// gather the runs
	vector<vector<MyDB_RecordIteratorAltPtr>> allRuns;
	for (size_t i = 0; i < numPages; i += runSize)
	{
		vector<MyDB_RecordIteratorAltPtr> currentRun;
		for (size_t j = i; j < i + runSize && j < numPages; j++)
		{
			currentRun.push_back((sortMe[j].sort(comparator, lhs, rhs))->getIteratorAlt());
		}
		allRuns.push_back(currentRun);
	}

	// sort each run sperately
	vector<MyDB_RecordIteratorAltPtr> mergeUs;

	for (auto currentRun : allRuns)
	{
		auto sortedRun = mergeIntoList(sortMe.getBufferMgr(), currentRun, comparator, lhs, rhs);
		mergeUs.push_back(getIteratorAlt(sortedRun));
	}

	mergeIntoFile(sortIntoMe, mergeUs, comparator, lhs, rhs);
}

#endif
