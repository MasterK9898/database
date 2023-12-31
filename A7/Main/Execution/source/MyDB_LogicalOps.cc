
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "RegularSelection.h"
#include "Aggregate.h"
#include "SortMergeJoin.h"
#include "ScanJoin.h"
#include <regex>
#include <algorithm>

// making predicates into a super predicate
string joinPredicates(vector<ExprTreePtr> pred, bool includeTable)
{
	string res;
	if (pred.size() == 0)
	{
		res = "bool[true]";
	}
	else if (pred.size() == 1)
	{
		res = pred[0]->toString(includeTable);
	}
	else
	{
		// each and unit can have only two predicate, for multiple ands we need to do a recursion, like &&(a,b,c) = &&(&&(a,b),c)
		res = "&& (" + pred[0]->toString(includeTable) + "," + pred[1]->toString(includeTable) + ")";
		for (int i = 2; i < pred.size(); i++)
		{
			res = "&& (" + res + "," + pred[i]->toString(includeTable) + ")";
		}
	}

	return res;
}

// fill this out!  This should actually run the aggregation via an appropriate RelOp, and then it is going to
// have to unscramble the output attributes and compute exprsToCompute using an execution of the RegularSelection
// operation (why?  Note that the aggregate always outputs all of the grouping atts followed by the agg atts.
// After, a selection is required to compute the final set of aggregate expressions)
//
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two
// sides should be deleted (via a kill to killFile () on the buffer manager)
MyDB_TableReaderWriterPtr LogicalAggregate ::execute()
{
	// first we need to run the input to get the table reader writer
	auto input = inputOp->execute();
	if (input == nullptr)
	{
		cout << "input table reader writer is null" << endl;
		exit(1);
	}

	auto bufferMgr = input->getBufferMgr();

	auto output = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

	vector<pair<MyDB_AggType, string>> aggsToCompute;

	for (auto expr : exprsToCompute)
	{
		if (expr->isAvg())
		{
			aggsToCompute.push_back(make_pair(avg, expr->getChild()->toString()));
		}
		else if (expr->isSum())
		{
			aggsToCompute.push_back(make_pair(sum, expr->getChild()->toString()));
		}
		else
		{
			cout << "illegal expression in aggregation" << endl;
			exit(1);
		}
	}

	vector<string> groupingStrs;

	for (auto expr : groupings)
	{
		groupingStrs.push_back(expr->toString());
	}

	// no need for more predicates
	string predicateString = "bool[true]";

	Aggregate aggregate(input, output, aggsToCompute, groupingStrs, predicateString);

	aggregate.run();

	// cout << "agg finished" << endl;

	return output;
}
// we don't really count the cost of the aggregate, so cost its subplan and return that
pair<double, MyDB_StatsPtr> LogicalAggregate ::cost()
{
	return inputOp->cost();
}

// this costs the entire query plan with the join at the top, returning the compute set of statistics for
// the output.  Note that it recursively costs the left and then the right, before using the statistics from
// the left and the right to cost the join itself
pair<double, MyDB_StatsPtr> LogicalJoin ::cost()
{
	auto left = leftInputOp->cost();
	auto right = rightInputOp->cost();
	MyDB_StatsPtr outputStats = left.second->costJoin(outputSelectionPredicate, right.second);
	return make_pair(left.first + right.first + outputStats->getTupleCount(), outputStats);
}

// Fill this out!  This should recursively execute the left hand side, and then the right hand side, and then
// it should heuristically choose whether to do a scan join or a sort-merge join (if it chooses a scan join, it
// should use a heuristic to choose which input is to be hashed and which is to be scanned), and execute the join.
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two
// sides should be deleted (via a kill to killFile () on the buffer manager)
MyDB_TableReaderWriterPtr LogicalJoin ::execute()
{

	// first we need to execute the left and right, to get the left and right table reader writer
	auto left = leftInputOp->execute();
	auto right = rightInputOp->execute();
	if (left == nullptr || right == nullptr)
	{
		cout << "left or right table is null" << endl;
		exit(1);
	}

	// auto print = joinPredicates(outputSelectionPredicate, true);
	// cout << "target " << outputSpec->getName() << ": JOIN START"
	// 		 << " | left: " << left->getTable()->getName() << " | right: " << right->getTable()->getName() << " | pred: " << print << endl;

	auto bufferMgr = left->getBufferMgr();

	auto output = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

	auto finalPred = joinPredicates(outputSelectionPredicate, true);

	// projection is the same as the exprsToCompute, but we need to convert it to string
	vector<string> projections;
	for (auto expr : exprsToCompute)
	{
		projections.push_back(expr->toString());
	}

	auto rightAtts = right->getTable()->getSchema()->getAtts();

	// equality check is the same as the output selection predicate, but we need to convert it to pairs
	// we need to put the left and right into the pair, and make sure first is the left
	vector<pair<string, string>> equalityChecks;
	for (const auto &expr : outputSelectionPredicate)
	{
		if (expr->getLHS() && expr->getRHS())
		{
			auto leftStr = expr->getLHS()->toString();
			auto rightStr = expr->getRHS()->toString();
			// left is in right, we need to swap
			if (any_of(rightAtts.begin(), rightAtts.end(), [&leftStr](const pair<string, MyDB_AttTypePtr> &att)
								 { return leftStr.find(att.first) != string::npos; }))
			{
				string temp = leftStr;
				leftStr = rightStr;
				rightStr = temp;
			}
			equalityChecks.emplace_back(leftStr, rightStr);
		}
	}

	// we have to fake it up if there is no equality check
	if (equalityChecks.size() == 0)
	{
		equalityChecks.emplace_back("bool[true]", "bool[true]");
	}

	// the left and right part have already done their check,
	// so we don't need to check again
	string leftSelectionPredicateString = "bool[true]";
	string rightSelectionPredicateString = "bool[true]";

	MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, left->getBufferMgr());

	// SortMergeJoin sortMergeJoin(left, right, outputTable, finalPred, projections, equalityChecks[0], leftSelectionPredicateString, rightSelectionPredicateString);
	// sortMergeJoin.run();

	ScanJoin scanJoin(left, right, outputTable, finalPred, projections, equalityChecks, leftSelectionPredicateString, rightSelectionPredicateString);
	scanJoin.run();
	// cout << "target " << outputSpec->getName() << ": JOIN COMPLETE" << endl;

	bufferMgr->killTable(left->getTable());
	bufferMgr->killTable(right->getTable());

	return outputTable;
}

// this costs the table scan returning the compute set of statistics for the output
pair<double, MyDB_StatsPtr> LogicalTableScan ::cost()
{
	MyDB_StatsPtr returnVal = inputStats->costSelection(selectionPred);
	return make_pair(returnVal->getTupleCount(), returnVal);
}

// fill this out!  This should heuristically choose whether to use a B+-Tree (if appropriate) or just a regular
// table scan, and then execute the table scan using a relational selection.  Note that a desirable optimization
// is to somehow set things up so that if a B+-Tree is NOT used, that the table scan does not actually do anything,
// and the selection predicate is handled at the level of the parent (by filtering, for example, the data that is
// input into a join)
MyDB_TableReaderWriterPtr LogicalTableScan ::execute()
{
	// cout << "target " << outputSpec->getName() << ": SCAN START" << endl;
	// we will make the predicate into a super predicate
	auto finalPred = joinPredicates(selectionPred, false);

	auto bufferMgr = inputSpec->getBufferMgr();

	MyDB_TableReaderWriterPtr output = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

	RegularSelection regularSelection(inputSpec, output, finalPred, exprsToCompute);
	regularSelection.run();

	// cout << "target " << outputSpec->getName() << ": SCAN COMPLETE" << endl;
	return output;
}

pair<double, MyDB_StatsPtr> LogicalConvertScan::cost()
{
	return inputOp->cost();
}

MyDB_TableReaderWriterPtr LogicalConvertScan::execute()
{
	MyDB_TableReaderWriterPtr input = inputOp->execute();
	if (input == nullptr)
	{
		cout << "input table is null" << endl;
		exit(1);
	}
	// cout << "conversion started" << endl;

	auto bufferMgr = input->getBufferMgr();

	auto output = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

	string pred = "bool[true]";

	RegularSelection regularSelection(input, output, pred, exprsToCompute);

	regularSelection.run();

	bufferMgr->killTable(input->getTable());

	return output;
}

#endif
