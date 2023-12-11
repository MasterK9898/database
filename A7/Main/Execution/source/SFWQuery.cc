
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"
#include <sstream>

using namespace std;

inline void printSchema(MyDB_SchemaPtr schema)
{
	cout << "Schema is: ";
	for (auto att : schema->getAtts())
	{
		cout << att.first << " " << att.second->toString() << "|";
	}
	cout << endl;
}

LogicalOpPtr SFWQuery ::join(map<string, MyDB_TablePtr> &allTables, map<string, MyDB_TableReaderWriterPtr> &allTableReaderWriters, vector<ExprTreePtr> targetExprs,
														 vector<pair<string, string>> targetTables,
														 vector<ExprTreePtr> CNF)
{
	double bestCost = numeric_limits<double>::max();
	if (targetTables.size() == 1)
	{
		// we only have one, we don't need to join, just scan
		auto tableInfo = targetTables[0];
		auto targetTable = allTables[tableInfo.first];

		vector<string> exprsToCompute;

		MyDB_SchemaPtr outputScheme = make_shared<MyDB_Schema>();
		for (const auto &expr : targetExprs)
		{
			for (const auto &inputAtt : targetTable->getSchema()->getAtts())
			{
				if (expr->referencesAtt(tableInfo.second, inputAtt.first))
				{
					outputScheme->getAtts().emplace_back(tableInfo.second + "_" + inputAtt.first, inputAtt.second);
					exprsToCompute.emplace_back("[" + inputAtt.first + "]");
				}
			}
		}

		auto res = make_shared<LogicalTableScan>(
				allTableReaderWriters[tableInfo.first],
				make_shared<MyDB_Table>(tableInfo.second, tableInfo.second + "StorageLoc", outputScheme),
				make_shared<MyDB_Stats>(targetTable, tableInfo.second),
				CNF, exprsToCompute);

		// if (totTables == 1)
		// 	finalScheme = outputScheme;
		return res;
	}
	else
	{
		cout << "Joining " << targetTables.size() << " tables\n";
		// we need to partition the tables into left and right
		// we will first do this arbitraryly

		// we can resue professor's code to a great extent, but we need to deal with multiple tables
		// so we need some loops
		vector<pair<string, string>> leftTables;
		vector<pair<string, string>> rightTables;
		for (int i = 0; i < targetTables.size(); i++)
		{
			if (i < targetTables.size() / 2)
				leftTables.push_back(targetTables[i]);
			else
				rightTables.push_back(targetTables[i]);
		}

		// find the various parts of the CNF
		vector<ExprTreePtr> leftCNF;
		vector<ExprTreePtr> rightCNF;
		vector<ExprTreePtr> topCNF;

		for (auto cnf : CNF)
		{
			bool inLeft = any_of(leftTables.begin(), leftTables.end(), [&cnf](const pair<string, string> &table)
													 { return cnf->referencesTable(table.second); });
			bool inRight = any_of(rightTables.begin(), rightTables.end(), [&cnf](const pair<string, string> &table)
														{ return cnf->referencesTable(table.second); });
			if (inLeft && inRight)
				topCNF.push_back(cnf);
			else if (inLeft)
				leftCNF.push_back(cnf);
			else
				rightCNF.push_back(cnf);
		}

		// instead of using string, we use ExprTreePtr to represent the expressions
		// because reversing string is quit annoying
		vector<ExprTreePtr> leftExprs;
		vector<ExprTreePtr> rightExprs;
		// we cannot use targetExprs directly, because we now don't need all of them
		// we need to pick the out from targetExprs
		vector<ExprTreePtr> topExprs;
		MyDB_SchemaPtr leftSchema = make_shared<MyDB_Schema>();
		MyDB_SchemaPtr rightSchema = make_shared<MyDB_Schema>();
		MyDB_SchemaPtr topSchema = make_shared<MyDB_Schema>();

		for (auto table : leftTables)
		{
			auto leftTable = allTables[table.first];
			for (auto att : leftTable->getSchema()->getAtts())
			{

				bool needInSelect = any_of(targetExprs.begin(), targetExprs.end(), [&att, &table](const ExprTreePtr &exp)
																	 { return exp->referencesAtt(table.second, att.first); });
				bool needInCNF = any_of(CNF.begin(), CNF.end(), [&att, &table](const ExprTreePtr &exp)
																{ return exp->referencesAtt(table.second, att.first); });

				ExprTreePtr idExp = make_shared<Identifier>(strdup(table.second.c_str()), strdup(att.first.c_str()));

				if (needInSelect || needInCNF)
				{
					leftSchema->getAtts().emplace_back(table.second + "_" + att.first, att.second);
					leftExprs.push_back(idExp);
				}

				if (needInSelect)
				{
					topSchema->getAtts().emplace_back(table.second + "_" + att.first, att.second);
					topExprs.push_back(idExp);
				}
			}
		}

		for (auto table : rightTables)
		{
			auto rightTable = allTables[table.first];
			for (auto att : rightTable->getSchema()->getAtts())
			{

				bool needInSelect = any_of(targetExprs.begin(), targetExprs.end(), [&att, &table](const ExprTreePtr &exp)
																	 { return exp->referencesAtt(table.second, att.first); });
				bool needInCNF = any_of(CNF.begin(), CNF.end(), [&att, &table](const ExprTreePtr &exp)
																{ return exp->referencesAtt(table.second, att.first); });

				ExprTreePtr idExp = make_shared<Identifier>(strdup(table.second.c_str()), strdup(att.first.c_str()));

				if (needInSelect || needInCNF)
				{
					rightSchema->getAtts().emplace_back(table.second + "_" + att.first, att.second);
					rightExprs.push_back(idExp);
				}

				if (needInSelect)
				{
					topSchema->getAtts().emplace_back(table.second + "_" + att.first, att.second);
					topExprs.push_back(idExp);
				}
			}
		}

		// we cannot give it a predicated name, because now we have recursion, we need to give it a unique name
		string tableName = "Join";
		for (auto table : targetTables)
		{
			tableName += ("_" + table.second);
		}

		// do it recursively
		LogicalOpPtr leftJoin = join(allTables, allTableReaderWriters, leftExprs, leftTables, leftCNF);
		LogicalOpPtr rightJoin = join(allTables, allTableReaderWriters, rightExprs, rightTables, rightCNF);

		cout << "Joined " << targetTables.size() << "left and right tables\n";
		// we can use targetExprs ?
		LogicalOpPtr res = make_shared<LogicalJoin>(
				leftJoin, rightJoin,
				make_shared<MyDB_Table>(tableName, tableName + "StorageLoc", topSchema),
				topCNF, topExprs);

		// if (targetTables.size() == totTables)
		// 	finalScheme = topSchema;

		return res;
	}
}

// replace all ocurances of b in a with c
// written by chatgpt
void replaceAll(std::string &a, const std::string &b, const std::string &c)
{
	size_t pos = 0;
	while ((pos = a.find(b, pos)) != std::string::npos)
	{
		a.replace(pos, b.length(), c);
		pos += c.length();
	}
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
//
// note that this implementation only works for two-table queries that do not have an aggregation
//
LogicalOpPtr SFWQuery ::buildLogicalQueryPlan(map<string, MyDB_TablePtr> &allTables, map<string, MyDB_TableReaderWriterPtr> &allTableReaderWriters)
{
	bool areAggs = groupingClauses.size() != 0;
	if (!areAggs)
	{
		for (auto a : valuesToSelect)
		{
			if (a->hasAgg())
			{
				areAggs = true;
				break;
			}
		}
	}

	auto joinOp = join(allTables, allTableReaderWriters, valuesToSelect, tablesToProcess, allDisjunctions);

	LogicalOpPtr intermediate = joinOp;

	// we need to rename the aggregated attributs or else it will be compiled wrongly
	unordered_map<string, string> renameAtt;
	if (areAggs)
	{
		// now we have the biggest table, we need to get aggregates
		// be carefull that we nned to put the group clauses before the aggregate ones

		auto joinSchema = joinOp->getOutputTable()->getSchema();
		MyDB_Record joinRecord(joinSchema);

		auto aggScheme = make_shared<MyDB_Schema>();

		// unordered_map<string, string> renameTable;
		int groupIndex = 0;
		for (const auto &groupExpr : groupingClauses)
		{
			// we need to get rid of the brackets

			// // string newName = "group_" + to_string(attId);
			// // aggScheme->getAtts().emplace_back(newName, myRecord.getType(groupExpr->toString()));
			// aggScheme->getAtts().emplace_back(groupExpr->toString(), joinRecord.getType(groupExpr->toString()));
			// // renameTable[groupExpr->toString()] = newName;
			// // attId++;

			// 	// we need to get rid of the brackets

			// // string newName = "group_" + to_string(attId);
			// // aggScheme->getAtts().emplace_back(newName, myRecord.getType(groupExpr->toString()));
			// aggScheme->getAtts().emplace_back(groupExpr->toString(), joinRecord.getType(groupExpr->toString()));
			// // renameTable[groupExpr->toString()] = newName;
			// // attId++;

			string originalName = groupExpr->toString();
			// give it a much simpler name
			string newName = "group_" + to_string(groupIndex++);
			aggScheme->getAtts().emplace_back(newName, joinRecord.getType(originalName));

			renameAtt[originalName] = newName;
		}

		// extract identifier from group by clauses
		// for (const auto &groupExpr : groupingClauses)
		// {
		// 	for (const auto &idExpr : groupExpr->getIdentifiers())
		// 	{
		// 		string idExprStr = idExpr->toString();
		// 		if (renameTable.find(idExprStr) == renameTable.end())
		// 		{
		// 			groupingClauses.push_back(idExpr);
		// 			// assert(idExprStr.size() >= 2);
		// 			string newName = idExprStr.substr(1, idExprStr.size() - 2);
		// 			aggScheme->getAtts().emplace_back(newName, myRecord.getType(idExpr->toString()));
		// 			renameTable[idExprStr] = newName;
		// 			attId++;
		// 		}
		// 	}
		// }

		vector<ExprTreePtr> aggToSelect;

		int aggIndex = 0;
		for (auto &expr : valuesToSelect)
		{
			auto aggsExprs = expr->getAggExprs();
			for (auto aggExpr : aggsExprs)
			{
				string originalName = aggExpr->toString();
				// give it a much simpler name
				string newName = "agg_" + to_string(aggIndex++);
				aggToSelect.push_back(aggExpr);
				aggScheme->getAtts().emplace_back(newName, joinRecord.getType(originalName));

				renameAtt[originalName] = newName;
			}
			// if (expr->hasAgg())
			// {
			// 	aggToSelect.push_back(expr);
			// 	aggScheme->getAtts().emplace_back(expr->toString(), joinRecord.getType(expr->toString()));
			// }
		}

		// vector<string> allRenamedKeys;
		// for (const auto &p : renameTable)
		// 	allRenamedKeys.push_back(p.first);

		// vector<string> finalSelections;
		// for (auto &expr : valuesToSelect)
		// {
		// 	string exprStr = expr->toString();
		// 	for (const auto &renamedKeys : allRenamedKeys)
		// 	{
		// 		replace(exprStr, renamedKeys, "[" + renameTable[renamedKeys] + "]");
		// 	}
		// 	finalSelections.push_back(exprStr);
		// }

		intermediate = make_shared<LogicalAggregate>(
				joinOp,
				make_shared<MyDB_Table>("agg", "agg.bin", aggScheme),
				aggToSelect,
				groupingClauses);
	}

	// return intermediate;

	// now we got all the information we need, we need to map them into the final result, which is valueToSelect

	// there is one more step: we need to deal with computation

	// the only case we dont need to do anything is when there is no computation and no aggregation
	// bool hasComputation = false;
	// for (auto &expr : valuesToSelect)
	// {
	// 	if (expr->isId() || expr)
	// 	{
	// 		hasComputation = true;
	// 		break;
	// 	}
	// }
	printSchema(intermediate->getOutputTable()->getSchema());

	auto intermediateRecord = MyDB_Record(intermediate->getOutputTable()->getSchema());

	// we a final select to get the results we need

	auto retScheme = make_shared<MyDB_Schema>();
	// auto retRecord = MyDB_Record(retScheme);

	vector<string> exprsToCompute;

	int outputIndex = 0;
	for (auto &expr : valuesToSelect)
	{
		// we first create the schema matching to final result
		// we need to replace the name of the attributes
		// if (expr->hasAgg())
		// {
		string exprStr = expr->toString();
		for (const auto &p : renameAtt)
		{
			cout << "replacing " << p.first << " with "
					 << "[" + p.second + "]" << endl;
			replaceAll(exprStr, p.first, "[" + p.second + "]");
		}
		retScheme->getAtts().emplace_back("output_" + to_string(outputIndex++), intermediateRecord.getType(exprStr));
		exprsToCompute.push_back(exprStr);
		// }
		// else
		// {
		// 	retScheme->getAtts().emplace_back(expr->toString(), intermediateRecord.getType(expr->toString()));
		// 	exprsToCompute.push_back(expr->toString());
		// }
		// string exprStr = expr->toString();
		// for (const auto &p : renameAtt)
		// {
		// 	cout << "replacing " << p.first << " with "
		// 			 << p.second << endl;
		// 	replaceAll(exprStr, p.first, p.second);
		// }
		// retScheme->getAtts().emplace_back(expr->toString(), intermediateRecord.getType(exprStr));
		// exprsToCompute.push_back("[" + exprStr + "]");
		// if (expr->isId() || expr->isSum() || expr->isAvg())
		// {
		// 	// easy case: we just need to copy the attributes

		// }
		// else
		// {

		// }
	}

	cout << "expr to compute" << endl;

	for (auto str : exprsToCompute)
	{
		cout << str << "|";
	}
	cout << endl;

	printSchema(retScheme);

	cout << "finished construction" << endl;

	/**
	 * LogicalConvertScan(LogicalOpPtr inputOp, MyDB_TablePtr outputSpec,
											 vector<string> &exprsToCompute)
	*/
	auto res = make_shared<LogicalConvertScan>(intermediate, make_shared<MyDB_Table>("final", "finalStorageLoc", retScheme), exprsToCompute);

	// auto res = make_shared<LogicalTableScan>(
	// 		input,
	// 		make_shared<MyDB_Table>("final", "finalStorageLoc", outputScheme),
	// 		make_shared<MyDB_Stats>(input->getTable(), onlyTable.second),
	// 		CNF, exprs);

	// cout << intermediate->getOutputTable()->getSchema()->getAtts() << endl;

	return res;
}

void SFWQuery ::print()
{
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect)
	{
		cout << "\t" << a->toString() << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess)
	{
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions)
	{
		cout << "\t" << a->toString() << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses)
	{
		cout << "\t" << a->toString() << "\n";
	}
}

SFWQuery ::SFWQuery(struct ValueList *selectClause, struct FromList *fromClause,
										struct CNF *CNF, struct ValueList *grouping)
{
	valuesToSelect = selectClause->valuesToCompute;
	tablesToProcess = fromClause->aliases;
	allDisjunctions = CNF->disjunctions;
	groupingClauses = grouping->valuesToCompute;
}

SFWQuery ::SFWQuery(struct ValueList *selectClause, struct FromList *fromClause,
										struct CNF *CNF)
{
	valuesToSelect = selectClause->valuesToCompute;
	tablesToProcess = fromClause->aliases;
	allDisjunctions = CNF->disjunctions;
}

SFWQuery ::SFWQuery(struct ValueList *selectClause, struct FromList *fromClause)
{
	valuesToSelect = selectClause->valuesToCompute;
	tablesToProcess = fromClause->aliases;
	allDisjunctions.push_back(make_shared<BoolLiteral>(true));
}

#endif