
#include "Lexer.h"
#include "Parser.h"
#include "ParserTypes.h"
#include "MyDB_BufferManager.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iterator>

using namespace std;
string toLower(string data)
{
	transform(data.begin(), data.end(), data.begin(), ::tolower);
	return data;
}

int main(int numArgs, char **args)
{

	// make sure we have the correct arguments
	if (numArgs != 3)
	{
		cout << "args: catalog_file directory_for_tables\n";
		return 0;
	}

	// open up the catalog file
	MyDB_CatalogPtr myCatalog = make_shared<MyDB_Catalog>(args[1]);

	// start up the buffer manager
	MyDB_BufferManagerPtr myMgr = make_shared<MyDB_BufferManager>(131072, 4028, "tempFile");

	// and create tables for everything in the database
	static map<string, MyDB_TablePtr> allTables = MyDB_Table ::getAllTables(myCatalog);

	// this is all of the tables
	static map<string, MyDB_TableReaderWriterPtr> allTableReaderWriters;

	// and this is just the B+-Trees
	static map<string, MyDB_BPlusTreeReaderWriterPtr> allBPlusReaderWriters;

	// load 'em up
	for (auto &a : allTables)
	{
		if (a.second->getFileType() == "heap")
		{
			allTableReaderWriters[a.first] = make_shared<MyDB_TableReaderWriter>(a.second, myMgr);
		}
		else if (a.second->getFileType() == "bplustree")
		{
			allBPlusReaderWriters[a.first] = make_shared<MyDB_BPlusTreeReaderWriter>(a.second->getSortAtt(), a.second, myMgr);
			allTableReaderWriters[a.first] = allBPlusReaderWriters[a.first];
		}
	}

	// print out the intro notification
	cout << "\n          Welcome to MyDB v0.1\n\n";
	cout << "\"Not the worst database in the world\" (tm) \n\n";

	// and repeatedly accept queries
	while (true)
	{

		cout << "MyDB> ";

		// this will be used to collect the query
		stringstream ss;

		// get a line
		for (string line; getline(cin, line);)
		{

			// see if it has a ";" at the end
			size_t pos = line.find(';');

			// it does!!  so we are ready yo parse
			if (pos != string ::npos)
			{

				// append the last line
				line.resize(pos);
				ss << line;

				// see if someone wants to load a file
				vector<string> tokens{istream_iterator<string>{ss},
															istream_iterator<string>{}};

				// see if we got a "quit" or "exit"
				if (tokens.size() == 1 && (toLower(tokens[0]) == "exit" || toLower(tokens[0]) == "quit"))
				{
					cout << "OK, goodbye.\n";
					// before we get outta here, write everything into the catalog
					for (auto &a : allTables)
					{
						a.second->putInCatalog(myCatalog);
					}
					return 0;
				}

				if (tokens.size() == 1 && toLower(tokens[0]) == "init")
				{
					cout << "OK, initializing all tables.\n";
					auto allInitTables = {"lineitem", "orders", "customer", "supplier", "part", "partsupp", "nation", "region"};
					for (auto name : allInitTables)
					{
						// make sure the table is there
						if (allTableReaderWriters.count(name) == 0)
						{
							cout << "Could not find table " << name << ".\n";
						}
						else
						{
							cout << "OK, loading " << name << " from text file.\n";

							// load up the file
							auto fileName = "./tables/" + string(name) + ".tbl";
							pair<vector<size_t>, size_t> res = allTableReaderWriters[name]->loadFromTextFile(fileName);

							// and record the tuple various counts
							allTableReaderWriters[name]->getTable()->setDistinctValues(res.first);
							allTableReaderWriters[name]->getTable()->setTupleCount(res.second);
						}
					}
					break;
				}

				// see if we got a "load soandso from afile"
				if (tokens.size() == 4 && toLower(tokens[0]) == "load" && toLower(tokens[2]) == "from")
				{

					// make sure the table is there
					if (allTableReaderWriters.count(tokens[1]) == 0)
					{
						cout << "Could not find table " << tokens[1] << ".\n";
						break;
					}
					else
					{
						cout << "OK, loading " << tokens[1] << " from text file.\n";

						// load up the file
						pair<vector<size_t>, size_t> res = allTableReaderWriters[tokens[1]]->loadFromTextFile(tokens[3]);

						// and record the tuple various counts
						allTableReaderWriters[tokens[1]]->getTable()->setDistinctValues(res.first);
						allTableReaderWriters[tokens[1]]->getTable()->setTupleCount(res.second);
						break;
					}
				}

				// get the string to parse
				string parseMe = ss.str();

				// see if we got a load-from

				// add an extra zero at the end; needed by lexer
				parseMe.push_back('\0');

				// now parse it
				yyscan_t scanner;
				LexerExtra extra{""};
				yylex_init_extra(&extra, &scanner);
				const YY_BUFFER_STATE buffer{yy_scan_string(parseMe.data(), scanner)};
				SQLStatement *final = nullptr;
				const int parseFailed{yyparse(scanner, &final)};
				yy_delete_buffer(buffer, scanner);
				yylex_destroy(scanner);

				// if we did not parse correctly
				if (parseFailed)
				{

					// print out the parse error
					cout << "Parse error: " << extra.errorMessage;

					// get outta here
					if (final != nullptr)
						delete final;
					break;

					// if we did parse correctly, just print out the query
				}
				else
				{

					// see if we got a create table
					if (final->isCreateTable())
					{

						string tableName = final->addToCatalog(args[2], myCatalog);
						MyDB_TablePtr temp = make_shared<MyDB_Table>();
						temp->fromCatalog(tableName, myCatalog);
						if (tableName != "nothing")
						{
							allTables[tableName] = temp;
							if (allTables[tableName]->getFileType() == "heap")
							{
								allTableReaderWriters[tableName] = make_shared<MyDB_TableReaderWriter>(allTables[tableName], myMgr);
							}
							else if (allTables[tableName]->getFileType() == "bplustree")
							{
								allBPlusReaderWriters[tableName] = make_shared<MyDB_BPlusTreeReaderWriter>(allTables[tableName]->getSortAtt(),
																																													 allTables[tableName], myMgr);
								allTableReaderWriters[tableName] = allBPlusReaderWriters[tableName];
							}
							cout << "Added table " << final->addToCatalog(args[2], myCatalog) << "\n";
						}
					}
					else if (final->isSFWQuery())
					{

						LogicalOpPtr myPlan = final->buildLogicalQueryPlan(allTables, allTableReaderWriters);
						if (myPlan != nullptr)
						{
							auto res = myPlan->cost();
							cout << "cost was " << res.first << "\n";

							// and execute it
							auto output = myPlan->execute();

							if (output != nullptr)
							{
								cout << "top 30 results: " << endl;

								// Create number counters
								int counter = 0;

								MyDB_RecordPtr temp = output->getEmptyRecord();
								auto myIter = output->getIteratorAlt();

								while (myIter->advance())
								{
									myIter->getCurrent(temp);
									if (counter < 30)
									{
										cout << temp << endl;
									}
									else
									{
										break;
									}
									counter++;
								}
							}
						}
					}

					// get outta here
					if (final != nullptr)
						delete final;
					break;
				}
			}
			else
			{
				ss << line << "\n";
				cout << "    > ";
			}
		}
	}
}
