
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include "MyDB_Catalog.h"
#include <string>
#include <vector>

enum ExprType
// inspired from js types
{
	TYPE_UNDEFINED,
	TYPE_BOOLEAN,
	TYPE_NUMBER,
	TYPE_STRING,
};

// get the name for logging use
inline string getTypeName(ExprType type)
{
	switch (type)
	{
	case TYPE_BOOLEAN:
		return "BOOLEAN";
	case TYPE_NUMBER:
		return "NUMBER";
	case TYPE_STRING:
		return "STRING";
	case TYPE_UNDEFINED:
	default:
		return "UNDEFINED";
	}
}

inline string getTypesName(vector<ExprType> types)
{
	string res = "";
	for (int i = 0; i < types.size(); i++)
	{
		res += getTypeName(types[i]);
		if (i != types.size() - 1)
		{
			res += " or ";
		}
	}
	return res;
}

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

	bool isIdentifier()
	{
		return false;
	}

protected:
	ExprType type;
	// the name of this current exprssion
	string name;
};

template <typename T>
class Literal : public ExprTree
{
protected:
	T myVal;

	Literal(T fromMe)
	{
		myVal = fromMe;
	}

	Literal() {}

	~Literal() {}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return true;
	}
};

class BoolLiteral : public Literal<bool>
{

public:
	BoolLiteral(bool fromMe) : Literal(fromMe)
	{
		// new knowledge: I cannot do initializer list to protected memeber
		type = TYPE_BOOLEAN;
		name = "BOOLEAN";
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
};

class DoubleLiteral : public Literal<double>
{

public:
	DoubleLiteral(double fromMe) : Literal(fromMe)
	{
		type = TYPE_NUMBER;
		name = "DOUBLE";
	}

	string toString()
	{
		return "double[" + to_string(myVal) + "]";
	}
};

// this implement class ExprTree
class IntLiteral : public Literal<int>
{

public:
	IntLiteral(int fromMe) : Literal(fromMe)
	{
		type = TYPE_NUMBER;
		name = "INT";
	}

	string toString()
	{
		return "int[" + to_string(myVal) + "]";
	}
};

class StringLiteral : public Literal<string>
{

public:
	StringLiteral(char *fromMe)
	{
		fromMe[strlen(fromMe) - 1] = 0;
		myVal = string(fromMe + 1);
		type = TYPE_STRING;
		name = "STRING";
	}

	string toString()
	{
		return "string[" + myVal + "]";
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
		type = TYPE_UNDEFINED;
		name = "IDENTIFIER";
	}

	string toString()
	{
		return "[" + tableName + "_" + attName + "]";
	}

	bool isIdentifier()
	{
		return true;
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
			cout << "TYPE_IDENTIFIER: Table Name " << tableName << " is not found" << endl;
			return false;
		}

		string res;
		if (!myCatalog->getString(tableKey + "." + attName + ".type", res))
		{
			cout << "TYPE_IDENTIFIER: Attribute Name " << attName << " is not found" << endl;
			return false;
		};

		if (res == "int" || res == "double")
		{
			type = TYPE_NUMBER;
		}
		else if (res == "string")
		{
			type = TYPE_STRING;
		}
		else if (res == "bool")
		{
			type = TYPE_BOOLEAN;
		}
		return true;
	}
};

class BinaryOp : public ExprTree
{
protected:
	ExprTreePtr lhs;
	ExprTreePtr rhs;

	BinaryOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn)
	{
		lhs = lhsIn;
		rhs = rhsIn;
	}

	~BinaryOp() {}

	// the template method
	bool checkQueryHelper(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess, vector<ExprType> types)
	{
		if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeLeft = lhs->getType();
		ExprType typeRight = rhs->getType();

		if (typeLeft != typeRight)
		{
			cout << name << ": lhs " << lhs->toString() << " and rhs " << rhs->toString() + " are not the same type" << endl;
			return false;
		}

		// do a easy short circuit
		if (types.size() == 0)
		{
			return true;
		}

		for (auto type : types)
		{
			if (type == typeLeft)
			{
				return true;
			}
		}

		cout << name << ": lhs " << lhs->toString() << " is not " << getTypesName(types) << endl;

		return false;
	}
};

class MinusOp : public BinaryOp
{

public:
	MinusOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn) : BinaryOp(lhsIn, rhsIn)
	{
		type = TYPE_NUMBER;
		name = "MINUS";
	}

	string toString()
	{
		return "- (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return BinaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_NUMBER});
	}
};

class PlusOp : public BinaryOp
{

public:
	PlusOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn) : BinaryOp(lhsIn, rhsIn)
	{
		// string or TYPE_number, you just dont know
		type = TYPE_UNDEFINED;
		name = "PLUS";
	}

	string toString()
	{
		return "+ (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		// assign the left type to the plus type
		type = lhs->getType();
		// then we check the query
		return BinaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_NUMBER, TYPE_STRING});
	}
};

class TimesOp : public BinaryOp
{

public:
	TimesOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn) : BinaryOp(lhsIn, rhsIn)
	{
		type = TYPE_NUMBER;
		name = "TIMES";
	}

	string toString()
	{
		return "* (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return BinaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_NUMBER});
	}
};

class DivideOp : public BinaryOp
{

public:
	DivideOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn) : BinaryOp(lhsIn, rhsIn)
	{
		type = TYPE_NUMBER;
		name = "DIVIDE";
	}

	string toString()
	{
		return "/ (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		if (!BinaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_NUMBER}))
		{
			return false;
		}

		// need to check for zero
		if (rhs->toString().compare("0") == 0)
		{
			cout << name << ": rhs " << rhs->toString() << " is 0" << endl;
			return false;
		}

		return true;
	}
};

class GtOp : public BinaryOp
{

public:
	GtOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn) : BinaryOp(lhsIn, rhsIn)
	{
		type = TYPE_BOOLEAN;
		name = "GREATER";
	}

	string toString()
	{
		return "> (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	// can we do a greater than for string or TYPE_boolean?
	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return BinaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_NUMBER});
	}
};

class LtOp : public BinaryOp
{

public:
	LtOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn) : BinaryOp(lhsIn, rhsIn)
	{
		type = TYPE_BOOLEAN;
		name = "LESS";
	}

	string toString()
	{
		return "< (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	// can we do a less than for string or TYPE_boolean?
	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return BinaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_NUMBER});
	}
};

class NeqOp : public BinaryOp
{

public:
	NeqOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn) : BinaryOp(lhsIn, rhsIn)
	{
		type = TYPE_BOOLEAN;
		name = "NOT EQUAL";
	}

	string toString()
	{
		return "!= (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return BinaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_BOOLEAN, TYPE_NUMBER, TYPE_STRING});
	}
};

class OrOp : public BinaryOp
{

public:
	OrOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn) : BinaryOp(lhsIn, rhsIn)
	{
		type = TYPE_BOOLEAN;
		name = "OR";
	}

	string toString()
	{
		return "|| (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return BinaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_BOOLEAN});
	}
};

class EqOp : public BinaryOp
{

public:
	EqOp(ExprTreePtr lhsIn, ExprTreePtr rhsIn) : BinaryOp(lhsIn, rhsIn)
	{
		type = TYPE_BOOLEAN;
		name = "EQUAL";
	}

	string toString()
	{
		return "== (" + lhs->toString() + ", " + rhs->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return BinaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_BOOLEAN, TYPE_NUMBER, TYPE_STRING});
	}
};

class UnaryOp : public ExprTree
{
protected:
	ExprTreePtr child;

	UnaryOp(ExprTreePtr childIn)
	{
		child = childIn;
	}

	~UnaryOp() {}

	// the template method
	bool checkQueryHelper(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess, vector<ExprType> types)
	{
		if (!child->checkQuery(myCatalog, tablesToProcess))
		{
			return false;
		}

		ExprType typeChild = child->getType();

		// do a easy short circuit
		if (types.size() == 0)
		{
			return true;
		}

		for (auto type : types)
		{
			if (type == typeChild)
			{
				return true;
			}
		}

		cout << name << ": child " << child->toString() << " is not " << getTypesName(types) << endl;

		return false;
	}
};

class NotOp : public UnaryOp
{

public:
	NotOp(ExprTreePtr childIn) : UnaryOp(childIn)
	{
		type = TYPE_BOOLEAN;
		name = "NOT";
	}

	string toString()
	{
		return "!(" + child->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return UnaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_BOOLEAN});
	}
};

class SumOp : public UnaryOp
{

public:
	SumOp(ExprTreePtr childIn) : UnaryOp(childIn)
	{
		type = TYPE_NUMBER;
		name = "SUM";
	}

	string toString()
	{
		return "sum(" + child->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return UnaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_NUMBER});
	}
};

class AvgOp : public UnaryOp
{

public:
	AvgOp(ExprTreePtr childIn) : UnaryOp(childIn)
	{
		type = TYPE_NUMBER;
		name = "AVG";
	}

	string toString()
	{
		return "avg(" + child->toString() + ")";
	}

	bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string, string>> tablesToProcess)
	{
		return UnaryOp::checkQueryHelper(myCatalog, tablesToProcess, {TYPE_NUMBER});
	}
};

#endif
