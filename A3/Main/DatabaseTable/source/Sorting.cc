
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"
#include <queue>

using namespace std;

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

// leet code merge sort, because it need recursion, so it must be separated
vector<MyDB_PageReaderWriter> mergeHelper(size_t start, size_t end, vector<MyDB_PageReaderWriter> *currentRun, MyDB_BufferManagerPtr parent, function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	if (start < end)
	{
		return vector<MyDB_PageReaderWriter>();
	}
	else if (start == end)
	{
		vector<MyDB_PageReaderWriter> result;
		result.push_back(currentRun->at(start));
		return result;
	}
	else
	{
		size_t mid = (start + end) / 2;
		auto left = mergeHelper(start, mid, currentRun, parent, comparator, lhs, rhs);
		auto right = mergeHelper(mid + 1, end, currentRun, parent, comparator, lhs, rhs);
		return mergeIntoList(parent, getIteratorAlt(left), getIteratorAlt(right), comparator, lhs, rhs);
	}
}

void mergeIntoFile(MyDB_TableReaderWriter &sortIntoMe, vector<MyDB_RecordIteratorAltPtr> &mergeUs,
									 function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	// leetcode again, merge k sorted list

	// init the comparator by closure
	auto comp = [lhs, rhs, comparator](const MyDB_RecordIteratorAltPtr leftIter, const MyDB_RecordIteratorAltPtr rightIter)
	{
		leftIter->getCurrent(lhs);
		rightIter->getCurrent(rhs);
		// by default priority queue is max heap, so we need to reverse the comparator
		return !comparator();
	};

	priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, decltype(comp)> priorityQueue(comp);

	// first phase: push in the first element of each iterator
	for (MyDB_RecordIteratorAltPtr iterator : mergeUs)
	{
		if (iterator->advance())
		{
			priorityQueue.push(iterator);
		}
	}

	// second phase: pop the top element and push the next element of the target iterator
	MyDB_RecordPtr helper = sortIntoMe.getEmptyRecord();
	while (!priorityQueue.empty())
	{
		auto iterator = priorityQueue.top();
		priorityQueue.pop();

		// read and write
		iterator->getCurrent(helper);
		sortIntoMe.append(helper);

		if (iterator->advance())
		{
			priorityQueue.push(iterator);
		}
	}
}

vector<MyDB_PageReaderWriter> mergeIntoList(MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter,
																						MyDB_RecordIteratorAltPtr rightIter, function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	// init the result first
	vector<MyDB_PageReaderWriter> result;
	// init the reader writer
	MyDB_PageReaderWriter current(*parent);

	// then it's just leetcode question
	// while both iterators have not reached the end
	while (leftIter->advance() && rightIter->advance())
	{
		// get the record
		leftIter->getCurrent(lhs);
		rightIter->getCurrent(rhs);
		// compare
		if (comparator())
		{
			appendHelper(&current, lhs, &result, parent);
			leftIter->advance();
		}
		else
		{
			appendHelper(&current, rhs, &result, parent);
			rightIter->advance();
		}
	}

	// append the rest of the left iterator
	while (leftIter->advance())
	{
		leftIter->getCurrent(lhs);
		appendHelper(&current, lhs, &result, parent);
	}

	// append the rest of the right iterator
	while (rightIter->advance())
	{
		rightIter->getCurrent(rhs);
		appendHelper(&current, rhs, &result, parent);
	}

	return result;
}

void sort(int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe,
					function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	size_t numPages = sortMe.getNumPages();
	size_t counter = 0;

	// step 1: push all the iterators into a vector
	vector<vector<MyDB_PageReaderWriter>> allRuns;
	for (size_t i = 0; i < numPages; i += runSize)
	{
		vector<MyDB_PageReaderWriter> currentRun;
		for (size_t j = i; j < i + runSize && j < numPages; j++)
		{
			currentRun.push_back(*(sortMe[j].sort(comparator, lhs, rhs)));
		}
		allRuns.push_back(currentRun);
	}

	// step 2: sort all runs
	vector<MyDB_RecordIteratorAltPtr> mergeUs;

	for (auto currentRun : allRuns)
	{
		auto sortedRun = mergeHelper(0, currentRun.size() - 1, &currentRun, sortMe.getBufferMgr(), comparator, lhs, rhs);
		mergeUs.push_back(getIteratorAlt(sortedRun));
	}

	mergeIntoFile(sortIntoMe, mergeUs, comparator, lhs, rhs);
}

#endif
