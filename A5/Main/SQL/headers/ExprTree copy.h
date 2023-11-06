
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include "MyDB_Catalog.h"
#include <string>
#include <vector>

enum ExprType
// inspired from js types
{
	UNDEFINED,
	BOOLEAN,
	NUMBER,
	STRING,
};

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr<ExprTree> ExprTreePtr;

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree
{

public:
	virtual string toString() = 0;
	virtual ~ExprTree(){};
	virtual bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess) = 0;

	virtual ExprType getType()
	{
		return type;
	};

protected:
	ExprType type;
};

class BoolLiteral : public ExprTree
{

private:
	bool myVal;

public:
	BoolLiteral(bool fromMe)
	{
		// new knowledge: I cannot do initializer list to protected memeber
		type = BOOLEAN;
		myVal = fromMe;
	}

	string toString()
	{
		if (myVal)
		{
			return "bool[true]";
		}
		else
		{
			return "bool[false]";
		}
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return true;
	}
};

class DoubleLiteral : public ExprTree
{

private:
	double myVal;

public:
	DoubleLiteral(double fromMe)
	{
		myVal = fromMe;
		type = NUMBER;
	}

	string toString()
	{
		return "double[" + to_string(myVal) + "]";
	}

	~DoubleLiteral() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return true;
	}
};

// this implement class ExprTree
class IntLiteral : public ExprTree
{

private:
	int myVal;

public:
	IntLiteral(int fromMe)
	{
		myVal = fromMe;
		type = NUMBER;
	}

	string toString()
	{
		return "int[" + to_string(myVal) + "]";
	}

	~IntLiteral() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return true;
	}
};

class StringLiteral : public ExprTree
{

private:
	string myVal;

public:
	StringLiteral(char *fromMe)
	{
		fromMe[strlen(fromMe) - 1] = 0;
		myVal = string(fromMe + 1);
		type = STRING;
	}

	string toString()
	{
		return "string[" + myVal + "]";
	}

	~StringLiteral() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return true;
	}
};

class Identifier : public ExprTree
{

private:
	string tableName;
	string attName;

public:
	Identifier(char *tableNameIn, char *attNameIn)
	{
		tableName = string(tableNameIn);
		attName = string(attNameIn);
		// no idea at the beginning
		type = UNDEFINED;
	}

	string toString()
	{
		return "[" + tableName + "_" + attName + "]";
	}

	~Identifier() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		string tableKey = "";
		for (auto pair : tablesToProcess)
		{
			if (pair.second == tableName)
			{
				tableKey = pair.first;
				break;
			}
		}
		if (tableKey == "")
		{
			cout << "IDENTIFIER: Table Name " << tableName << " is not found" << endl;
			return false;
		}

		string res;
		if (!myCatalog->getString(tableKey + "." + attName + ".type", res))
		{
			cout << "IDENTIFIER: Attribute Name " << attName << " is not found" << endl;
			return false;
		};

		if (res == "int" || res == "double")
		{
			type = NUMBER;
		}
		else if (res == "string")
		{
			type = STRING;
		}
		else if (res == "bool")
		{
			type = BOOLEAN;
		}
		return true;
	}
};

class MinusOp : public ExprTree
{

private:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

public:
	MinusOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
		type = NUMBER;
	}

	string toString()
	{
		return "- (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	~MinusOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != NUMBER)
		{
			cout << "MINUS: lhs " << lhs->toString() << " is not NUMBER" << endl;
			return false;
		}

		if (typeRight != NUMBER)
		{
			cout << "MINUS: rhs " << rhs->toString() << " is not NUMBER" << endl;
			return false;
		}

		return true;
	}
};

class PlusOp : public ExprTree
{

private:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

public:
	PlusOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
		// string or number, you just dont know
		type = UNDEFINED;
	}

	string toString()
	{
		return "+ (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	~PlusOp() {}

	ExprType getType()
	{
		return this->type;
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != typeRight)
		{
			cout << "PLUS: lhs " << lhs->toString() << " and rhs " << rhs->toString() + " are not the same type" << endl;
			return false;
		}

		if (typeLeft == STRING)
		{
			type = STRING;
			return true;
		}
		else if (typeLeft == NUMBER)
		{
			type = NUMBER;
			return true;
		}
		else
		{
			cout << "PLUS: rhs " << rhs->toString() << " is not NUMBER or STRING" << endl;
			return false;
		}

		return true;
	}
};

class TimesOp : public ExprTree
{

private:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

public:
	TimesOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
		type = NUMBER;
	}

	string toString()
	{
		return "* (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	~TimesOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != NUMBER)
		{
			cout << "TIME: lhs " << lhs->toString() << " is not NUMBER" << endl;
			return false;
		}

		if (typeRight != NUMBER)
		{
			cout << "TIME: rhs " << rhs->toString() << " is not NUMBER" << endl;
			return false;
		}

		return true;
	}
};

class DivideOp : public ExprTree
{

private:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

public:
	DivideOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
		type = NUMBER;
	}

	string toString()
	{
		return "/ (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	~DivideOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != NUMBER)
		{
			cout << "DIVIDE: lhs " << lhs->toString() << " is not NUMBER" << endl;
			return false;
		}

		if (typeRight != NUMBER)
		{
			cout << "DIVIDE: rhs " << rhs->toString() << " is not NUMBER" << endl;
			return false;
		}

		if (rhs->toString().compare("0") == 0)
		{
			cout << "DIVIDE: rhs " << rhs->toString() << " is 0" << endl;
			return false;
		}

		return true;
	}
};

class GtOp : public ExprTree
{

private:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

public:
	GtOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
		type = BOOLEAN;
	}

	string toString()
	{
		return "> (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	~GtOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != typeRight)
		{
			cout << "GREATER: lhs " << lhs->toString() << " and rhs " << rhs->toString() + " are not the same type" << endl;
			return false;
		}

		return true;
	}
};

class LtOp : public ExprTree
{

private:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

public:
	LtOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
		type = BOOLEAN;
	}

	string toString()
	{
		return "< (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	~LtOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != typeRight)
		{
			cout << "LESS: lhs " << lhs->toString() << " and rhs " << rhs->toString() + " are not the same type" << endl;
			return false;
		}

		return true;
	}
};

class NeqOp : public ExprTree
{

private:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

public:
	NeqOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
		type = BOOLEAN;
	}

	string toString()
	{
		return "!= (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	~NeqOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != typeRight)
		{
			cout << "NOT EQUAL: lhs " << lhs->toString() << " and rhs " << rhs->toString() + " are not the same type" << endl;
			return false;
		}

		return true;
	}
};

class OrOp : public ExprTree
{

private:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

public:
	OrOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
		type = BOOLEAN;
	}

	string toString()
	{
		return "|| (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	~OrOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != BOOLEAN)
		{
			cout << "DIVIDE: lhs " << lhs->toString() << " is not BOOLEAN" << endl;
			return false;
		}

		if (typeRight != BOOLEAN)
		{
			cout << "DIVIDE: lhs " << lhs->toString() << " is not BOOLEAN" << endl;
			return false;
		}

		return true;
	}
};

class EqOp : public ExprTree
{

private:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

public:
	EqOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
		type = BOOLEAN;
	}

	string toString()
	{
		return "== (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	~EqOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != typeRight)
		{
			cout << "EQUAL: lhs " << lhs->toString() << " and rhs " << rhs->toString() + " are not the same type" << endl;
			return false;
		}

		return true;
	}
};

class NotOp : public ExprTree
{

private:
	ExprTreePtr child;

public:
	NotOp(ExprTreePtr childIn)
	{
		child = childIn;
		type = BOOLEAN;
	}

	string toString()
	{
		return "!(" + child->toString() + ")";
	}

	~NotOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!child->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType childType = child->getType();

		if (childType != BOOLEAN)
		{
			cout << "NOT: child " << childType->toString() << " is not BOOLEAN" << endl;
			return false;
		}

		return true;
	}
};

class SumOp : public ExprTree
{

private:
	ExprTreePtr child;

public:
	SumOp(ExprTreePtr childIn)
	{
		child = childIn;
		type = NUMBER;
	}

	string toString()
	{
		return "sum(" + child->toString() + ")";
	}

	~SumOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!child->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType childType = child->getType();

		if (childType != NUMBER)
		{
			cout << "SUM: child " << childType->toString() << " is not NUMBER" << endl;
			return false;
		}

		return true;
	}
};

class AvgOp : public ExprTree
{

private:
	ExprTreePtr child;

public:
	AvgOp(ExprTreePtr childIn)
	{
		child = childIn;
		type = NUMBER;
	}

	string toString()
	{
		return "avg(" + child->toString() + ")";
	}

	~AvgOp() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!child->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType childType = child->getType();

		if (childType != NUMBER)
		{
			cout << "AVG: child " << childType->toString() << " is not BOOLEAN" << endl;
			return false;
		}

		return true;
	}
};

#endif
