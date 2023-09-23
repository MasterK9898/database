
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"
#include <queue>

using namespace std;

/**
 1. sort phase, repeat until file processed
		1. read in R pages of rec (R = number of buffer pages available)
		2. sort all recs(usually with merge sort)
		3. write R pages of sorted recs to disk-those R pages are a "run"
	2. merge phase
		1. have a current page for each run j init to first page
		2. insert a pointer to a rec from each current page into priority queue
		3. while not all runs processed
			1. take smallest rec from Qi write to output
			2. add next rec from smallest rec's into Q
			3. if next rec is in next page, load next page in run
*/

void unifiedMergeHelper(vector<MyDB_RecordIteratorAltPtr> &mergeUs, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs, function<bool()> comparator, function<void(MyDB_RecordIteratorAltPtr)> inside)
{
	auto comp = [lhs, rhs, comparator](const MyDB_RecordIteratorAltPtr leftIter, const MyDB_RecordIteratorAltPtr rightIter)
	{
		leftIter->getCurrent(lhs);
		rightIter->getCurrent(rhs);
		// by default priority queue is max heap, so we need to reverse the comparator
		return !comparator();
	};

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

		// read and write
		inside(iterator);

		if (iterator->advance())
		{
			priorityQueue.push(iterator);
		}
	}
}

void mergeIntoFile(MyDB_TableReaderWriter &sortIntoMe, vector<MyDB_RecordIteratorAltPtr> &mergeUs,
									 function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	// use a helper to read and write
	MyDB_RecordPtr helper = sortIntoMe.getEmptyRecord();

	auto inside = [&sortIntoMe, helper](MyDB_RecordIteratorAltPtr iterator)
	{
		iterator->getCurrent(helper);
		sortIntoMe.append(helper);
	};

	unifiedMergeHelper(mergeUs, lhs, rhs, comparator, inside);
}

// merge k list at a time
vector<MyDB_PageReaderWriter> mergeIntoList(MyDB_BufferManagerPtr parent, vector<MyDB_RecordIteratorAltPtr> &mergeUs, function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs, MyDB_RecordPtr helper)
{
	// init the result
	vector<MyDB_PageReaderWriter> result;
	// init the reader writer
	MyDB_PageReaderWriter current(*parent);

	auto inside = [&current, &parent, &result, helper](MyDB_RecordIteratorAltPtr iterator)
	{
		// read and write
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
		auto sortedRun = mergeIntoList(sortMe.getBufferMgr(), currentRun, comparator, lhs, rhs, sortMe.getEmptyRecord());
		mergeUs.push_back(getIteratorAlt(sortedRun));
	}

	mergeIntoFile(sortIntoMe, mergeUs, comparator, lhs, rhs);
}

// actually I think that merge into list can be changed to merge k list, then we might not need the merge helper
// So I implemented a merge k version below
// this one is just writte for fun, not used in the other place

// a helper function to help appending even when current page is full
// if full create a new page and then append
void appendHelper(MyDB_PageReaderWriter *current, MyDB_RecordPtr target, vector<MyDB_PageReaderWriter> *result, MyDB_BufferManagerPtr parent)
{
	if (current->append(target))
	{
		// if we can append, then all good
		return;
	}
	else
	{
		// if we cannot append, then we need to create a new page
		result->push_back(*current);
		// create a new page
		MyDB_PageReaderWriter newPage(*parent);
		// swap current page into new page
		*current = newPage;
		// append the target, this time it must be successful
		current->append(target);
	}
}

vector<MyDB_PageReaderWriter> mergeIntoList(MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter,
																						MyDB_RecordIteratorAltPtr rightIter, function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	// it's just leetcode question merge 2 list

	// init the result first
	vector<MyDB_PageReaderWriter> result;
	// init the reader writer
	MyDB_PageReaderWriter current(*parent);

	// but it's quite annoying that there's no such thing for hasNext
	// so we need extra care for how to get the next member
	while (true)
	{
		// get the record
		leftIter->getCurrent(lhs);
		rightIter->getCurrent(rhs);
		// compare
		if (comparator())
		{
			appendHelper(&current, lhs, &result, parent);
			if (!leftIter->advance())
			{
				appendHelper(&current, rhs, &result, parent);
				while (rightIter->advance())
				{
					rightIter->getCurrent(rhs);
					appendHelper(&current, rhs, &result, parent);
				}
				break;
			}
		}
		else
		{
			appendHelper(&current, rhs, &result, parent);
			if (!rightIter->advance())
			{
				appendHelper(&current, lhs, &result, parent);
				while (leftIter->advance())
				{
					leftIter->getCurrent(lhs);
					appendHelper(&current, lhs, &result, parent);
				}
				break;
			}
		}
	}

	// remeber to put the last into the result
	result.push_back(current);

	return result;
}

#endif
