
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "RegularSelection.h"
#include "Aggregate.h"
#include "SortMergeJoin.h"
#include "ScanJoin.h"
#include <regex>
#include <algorithm>

// && (&& (== ([l_l_shipdate], string[1994-05-12]),== ([l_l_commitdate], string[1994-05-22])),== ([l_l_receiptdate], string[1994-06-10]))
// && (== ([l_l_shipdate], string[1994-05-12]),== ([l_l_commitdate], string[1994-05-22])),         == ([l_l_receiptdate], string[1994-06-10])
// == ([l_l_shipdate], string[1994-05-12])     == ([l_l_commitdate], string[1994-05-22])

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

	// cout << "res: " << res << endl;

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
	/**
	LogicalOpPtr inputOp;
	MyDB_TablePtr outputSpec;
	vector<ExprTreePtr> exprsToCompute;
	vector<ExprTreePtr> groupings;
	*/

	/**
	 * Aggregate (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
			vector <pair <MyDB_AggType, string>> aggsToCompute,
			vector <string> groupings, string selectionPredicate);

	*/
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

	auto bufferMgr = left->getBufferMgr();

	auto output = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

	// auto leftSchema = left->getTable()->getSchema();
	// string rightSchema = right->getTable()->getSchema();

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
	// the left and right part have already done their check,
	// so we don't need to check again
	string leftSelectionPredicateString = "bool[true]";
	string rightSelectionPredicateString = "bool[true]";

	// equality check is the same as the output selection predicate, but we need to convert it to pairs
	// vector<pair<string, string>> equalityChecks;
	// for (auto expr : outputSelectionPredicate)
	// {
	// 	// we need to put the left and right into the pair, and make sure first is the left
	// 	string leftStr = expr->getLHS()->toString();
	// 	string rightStr = expr->getRHS()->toString();

	// 	// equality checks must be in the form of a.x = b.y

	// 	// TODO: considring OR case
	// 	auto leftExpr = expr->getLHS();
	// 	auto rightExpr = expr->getRHS();
	// 	if (leftExpr == nullptr || rightExpr == nullptr)
	// 	{
	// 		cout << "left or right expr is null" << endl;
	// 		return nullptr;
	// 	}

	// 	// we need to make sure that the left is the left table
	// 	if (!leftExpr->referencesTable(left->getTable()->getName()))
	// 	{
	// 		string temp = leftStr;
	// 		leftStr = rightStr;
	// 		rightStr = temp;
	// 	}
	// else if (leftExpr->referencesTable(right->getTable()->getName()))
	// {
	// 	cout << "left expr references right table" << endl;
	// }
	// else
	// {
	// 	cout << "left expr references right table" << endl;
	// }

	// cout << expr->getLHS()->toString() << " " << expr->getRHS()->toString() << endl;
	// equalityChecks.push_back(make_pair(left, right));
	// }

	MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, left->getBufferMgr());
	ScanJoin scanJoin(left, right, outputTable, finalPred, projections, equalityChecks, leftSelectionPredicateString, rightSelectionPredicateString);

	scanJoin.run();

	bufferMgr->killTable(left->getTable());
	bufferMgr->killTable(right->getTable());

	return outputTable;
}
/**
	LogicalOpPtr leftInputOp;
	LogicalOpPtr rightInputOp;
	MyDB_TablePtr outputSpec;
	vector<ExprTreePtr> outputSelectionPredicate;
	vector<ExprTreePtr> exprsToCompute;
*/

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
	// we will make the predicate into a super predicate
	auto finalPred = joinPredicates(selectionPred, false);

	auto bufferMgr = inputSpec->getBufferMgr();

	MyDB_TableReaderWriterPtr output = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

	RegularSelection regularSelection(inputSpec, output, finalPred, exprsToCompute);
	regularSelection.run();

	return output;
}

/**
 * MyDB_TableReaderWriterPtr inputSpec;
	MyDB_TablePtr outputSpec;
	MyDB_StatsPtr inputStats;
	vector<ExprTreePtr> selectionPred;
	vector<string> exprsToCompute;
*/

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

	cout << "where the fuck is my execution?" << endl;
	auto bufferMgr = input->getBufferMgr();

	auto output = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

	string pred = "bool[true]";

	RegularSelection regularSelection(input, output, pred, exprsToCompute);

	regularSelection.run();

	bufferMgr->killTable(input->getTable());

	return output;
}

#endif
