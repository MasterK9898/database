
#ifndef TABLE_C
#define TABLE_C

#include "Table.h"

Table :: Table (string name, string storageLocIn) {
	tableName = name;
	storageLoc = storageLocIn;
}

Table :: ~Table () {}

string &Table :: getName () {
	return tableName;
}

string &Table :: getStorageLoc () {
	return storageLoc;
}

#endif

