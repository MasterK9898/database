
#ifndef SFWQUERY_H
#define SFWQUERY_H

#include "ExprTree.h"
#include "MyDB_LogicalOps.h"

// structure that stores an entire SFW query
struct SFWQuery
{

private:
	// the various parts of the SQL query

	// SELECT
	vector<ExprTreePtr> valuesToSelect;

	// FROM first as second
	vector<pair<string, string>> tablesToProcess;

	// WHERE
	vector<ExprTreePtr> allDisjunctions;

	// GROUP BY
	vector<ExprTreePtr> groupingClauses;

	// joins the tables recursively
	LogicalOpPtr join(map<string, MyDB_TablePtr> &allTables,
										map<string, MyDB_TableReaderWriterPtr> &allTableReaderWriters, vector<ExprTreePtr> targetExprs,
										vector<pair<string, string>> targetTables,
										vector<ExprTreePtr> CNF);

	MyDB_SchemaPtr finalScheme;

public:
	SFWQuery() {}

	SFWQuery(struct ValueList *selectClause, struct FromList *fromClause,
					 struct CNF *cnf, struct ValueList *grouping);

	SFWQuery(struct ValueList *selectClause, struct FromList *fromClause,
					 struct CNF *cnf);

	SFWQuery(struct ValueList *selectClause, struct FromList *fromClause);

	// builds and optimizes a logical query plan for a SFW query, returning the resulting logical query plan
	//
	// allTables: this is the list of all of the tables currently in the system
	// allTableReaderWriters: this is so we can store the info that we need to be able to execute the query
	LogicalOpPtr buildLogicalQueryPlan(map<string, MyDB_TablePtr> &allTables,
																		 map<string, MyDB_TableReaderWriterPtr> &allTableReaderWriters);

	~SFWQuery() {}

	void print();

#include "FriendDecls.h"
};

#endif
