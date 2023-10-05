
#ifndef BPLUS_C
#define BPLUS_C

#include <memory>
#include <algorithm>
#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"
#include "MyDB_PageType.h"

using namespace std;

MyDB_BPlusTreeReaderWriter ::MyDB_BPlusTreeReaderWriter(string orderOnAttName, MyDB_TablePtr forMe,
                                                        MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter(forMe, myBuffer)
{

  // find the ordering attribute
  auto res = forMe->getSchema()->getAttByName(orderOnAttName);

  // remember information about the ordering attribute
  orderingAttType = res.second;
  whichAttIsOrdering = res.first;

  // and the root location
  rootLocation = getTable()->getRootLocation();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter ::getSortedRangeIteratorAlt(MyDB_AttValPtr low, MyDB_AttValPtr high)
{
  return this->unifiedGetRangeIteratorAltHelper(low, high, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter ::getRangeIteratorAlt(MyDB_AttValPtr low, MyDB_AttValPtr high)
{
  return this->unifiedGetRangeIteratorAltHelper(low, high, false);
}

void MyDB_BPlusTreeReaderWriter ::discoverPages(int whichPage, vector<MyDB_PageReaderWriter> &list,
                                                MyDB_AttValPtr low, MyDB_AttValPtr high)
{
  // get the page first
  MyDB_PageReaderWriter page = this->operator[](whichPage);

  if (page.getType() == MyDB_PageType::RegularPage)
  // easy case: it's a leaf page, no need to find more
  {
    list.push_back(page);
  }
  else
  // else we need a dfs, compare the page's child, until we reach the leaf
  {
    // fortunately we got the alt iter
    MyDB_RecordIteratorAltPtr iter = page.getIteratorAlt();

    MyDB_INRecordPtr helper = getINRecord();

    auto comparators = this->getComparators(low, high, helper);

    // do for all child
    while (iter->advance())
    {
      iter->getCurrent(helper);

      if (comparators[0]())
      {
        continue;
      }

      discoverPages(helper->getPtr(), list, low, high);

      if (comparators[1]())
      // due to inclusive, high check is at the end of the loop
      {
        break;
      }
    }
  }
}

void MyDB_BPlusTreeReaderWriter ::append(MyDB_RecordPtr rec)
{
  if (this->rootLocation == -1)
  // there's no root, build one first
  {
    // build the root first
    size_t next = this->getTable()->lastPage() + 1;
    MyDB_PageReaderWriter root = this->operator[](next);
    root.setType(MyDB_PageType::DirectoryPage);

    this->rootLocation = next;

    // build a leaf page
    next++;
    MyDB_PageReaderWriter leaf = this->operator[](next);
    MyDB_INRecordPtr helper = getINRecord();
    helper->setPtr(next);

    root.append(helper);
  }
  // now append it
  MyDB_RecordPtr res = append(this->rootLocation, rec);

  if (res == nullptr)
  // every thing is doing ok
  {
    return;
  }
  else
  // we need a new root
  {
    size_t next = this->getTable()->lastPage() + 1;
    MyDB_PageReaderWriter root = this->operator[](next);
    root.setType(MyDB_PageType::DirectoryPage);

    root.append(res);

    MyDB_INRecordPtr helper = getINRecord();
    helper->setPtr(this->rootLocation);
    root.append(helper);

    this->rootLocation = next;
  }
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter ::split(MyDB_PageReaderWriter splitMe, MyDB_RecordPtr addMe)
{
  // create a helper function to get the records
  auto getHelperRecord = [this](MyDB_PageType type) -> MyDB_RecordPtr
  {
    if (type == MyDB_PageType::RegularPage)
    {
      return getEmptyRecord();
    }
    else
    {
      return getINRecord();
    }
  };

  // the following logic is modified from sortInPlace

  PageMeta *pageHeader = castPageHeader(splitMe.getPage());
  // first, read in the positions of all of the records
  vector<void *> positions;

  // put the current budy onto the vector first
  void *current = malloc(addMe->getBinarySize());
  addMe->toBinary(current);
  positions.push_back(current);

  size_t pageSize = splitMe.getPage()->getPageSize();
  void *temp = malloc(pageSize);
  memcpy(temp, splitMe.getPage()->getBytes(), pageSize);

  // we need another page header for the temp one
  PageMeta *pageHeaderTemp = (PageMeta *)temp;

  // this basically iterates through all of the records on the page
  MyDB_RecordPtr helper = getHelperRecord(splitMe.getType());
  size_t bytesConsumed = 0;
  while (bytesConsumed != pageHeader->numBytesUsed)
  {
    void *pos = (void *)(pageHeaderTemp->recs + bytesConsumed);
    positions.push_back(pos);
    void *nextPos = helper->fromBinary(pos);
    bytesConsumed += ((char *)nextPos) - ((char *)pos);
  }

  MyDB_RecordPtr lhs = getHelperRecord(splitMe.getType());
  MyDB_RecordPtr rhs = getHelperRecord(splitMe.getType());

  auto comparator = buildComparator(lhs, rhs);
  // and now we sort the vector of positions, using the record contents to build a comparator
  RecordComparator myComparator(comparator, lhs, rhs);
  std::stable_sort(positions.begin(), positions.end(), myComparator);

  // now we have all the data in the vector, we can clear out the page
  MyDB_PageType type = splitMe.getType();
  splitMe.clear();
  splitMe.setType(type);

  int next = this->getTable()->lastPage() + 1;
  // create the new page
  MyDB_PageReaderWriter appendMe = this->operator[](next);
  appendMe.setType(type);

  MyDB_INRecordPtr res = getINRecord();
  res->setPtr(next);

  // copy the data from vector back to the pages
  size_t count = 0;
  size_t half = positions.size() / 2;
  for (auto position : positions)
  {

    helper->fromBinary(position);
    if (count <= half)
    // new page has all the small key values
    {
      appendMe.append(helper);
    }
    else
    {
      splitMe.append(helper);
    }

    if (count == half)
    // put the last key value result
    {
      res->setKey(getKey(helper));
    }

    count++;
  }

  // last step, free all the memory
  free(current);
  free(temp);

  return res;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter ::append(int whichPage, MyDB_RecordPtr appendMe)
{
  // get the page first
  MyDB_PageReaderWriter page = this->operator[](whichPage);
  if (page.getType() == MyDB_PageType::RegularPage)
  // regular page, try to append
  {
    if (page.append(appendMe))
    {
      return nullptr;
    }
    else
    // if fails, split the page
    {
      return this->split(page, appendMe);
    }
  }
  else
  // directory page, which means recursion
  {
    MyDB_INRecordPtr helper = getINRecord();
    function<bool()> comparator = buildComparator(appendMe, helper);

    MyDB_RecordIteratorAltPtr iter = page.getIteratorAlt();
    while (iter->advance())
    {
      iter->getCurrent(helper);
      if (comparator())
      {
        MyDB_RecordPtr res = this->append(helper->getPtr(), appendMe);

        if (res == nullptr)
        // success
        {
          return nullptr;
        }
        else
        // split happends
        {
          if (page.append(res))
          // try to stick it to current page
          {
            // node pages need to be sorted in order to maintain the logic
            MyDB_RecordPtr lhs = getINRecord();
            MyDB_RecordPtr rhs = getINRecord();
            function<bool()> comparator = buildComparator(lhs, rhs);
            page.sortInPlace(comparator, lhs, rhs);
            return nullptr;
          }
          else
          // we cannot handle it, kick it up
          {
            return split(page, res);
          }
        }
      }
    }
  }

  return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter ::getINRecord()
{
  return make_shared<MyDB_INRecord>(orderingAttType->createAttMax());
}

void MyDB_BPlusTreeReaderWriter ::printTree()
{
  cout << "===root page at " << rootLocation << endl;
  printTree(rootLocation);
}

void MyDB_BPlusTreeReaderWriter ::printTree(int whichPage)
{
  MyDB_PageReaderWriter page = this->operator[](whichPage);
  MyDB_RecordIteratorAltPtr iter = page.getIteratorAlt();

  if (page.getType() == MyDB_PageType ::RegularPage)
  // leaf page, print out all the record keys
  {
    MyDB_RecordPtr helper = getEmptyRecord();

    cout << "leaf page " << whichPage << endl;
    cout << "======================== start of current page " << endl;
    while (iter->advance())
    {
      iter->getCurrent(helper);
      cout << "record " << helper << endl;
    }
    cout << "======================== end of current page " << endl;
  }
  else
  // non-leaf page, print slef info, then the child info
  {
    MyDB_INRecordPtr helper = getINRecord();

    cout << "node page " << whichPage << endl;
    while (iter->advance())
    {
      iter->getCurrent(helper);
      cout << "key " << getKey(helper) << " record " << helper << " pointing to " << helper->getPtr() << " |" << endl;
      printTree(helper->getPtr());
    }
  }
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter ::getKey(MyDB_RecordPtr fromMe)
{

  // in this case, got an IN record
  if (fromMe->getSchema() == nullptr)
    return fromMe->getAtt(0)->getCopy();

  // in this case, got a data record
  else
    return fromMe->getAtt(whichAttIsOrdering)->getCopy();
}

function<bool()> MyDB_BPlusTreeReaderWriter ::buildComparator(MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{

  MyDB_AttValPtr lhAtt, rhAtt;

  // in this case, the LHS is an IN record
  if (lhs->getSchema() == nullptr)
  {
    lhAtt = lhs->getAtt(0);

    // here, it is a regular data record
  }
  else
  {
    lhAtt = lhs->getAtt(whichAttIsOrdering);
  }

  // in this case, the LHS is an IN record
  if (rhs->getSchema() == nullptr)
  {
    rhAtt = rhs->getAtt(0);

    // here, it is a regular data record
  }
  else
  {
    rhAtt = rhs->getAtt(whichAttIsOrdering);
  }

  // now, build the comparison lambda and return
  if (orderingAttType->promotableToInt())
  {
    return [lhAtt, rhAtt]
    { return lhAtt->toInt() < rhAtt->toInt(); };
  }
  else if (orderingAttType->promotableToDouble())
  {
    return [lhAtt, rhAtt]
    { return lhAtt->toDouble() < rhAtt->toDouble(); };
  }
  else if (orderingAttType->promotableToString())
  {
    return [lhAtt, rhAtt]
    { return lhAtt->toString() < rhAtt->toString(); };
  }
  else
  {
    cout << "This is bad... cannot do anything with the >.\n";
    exit(1);
  }
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter::unifiedGetRangeIteratorAltHelper(MyDB_AttValPtr low, MyDB_AttValPtr high, bool sort)
{
  // it's actually only a function that init the variables and call apis

  // get the pages
  vector<MyDB_PageReaderWriter> pages;
  this->discoverPages(this->rootLocation, pages, low, high);
  // make the comparator
  MyDB_RecordPtr lhs = this->getEmptyRecord();
  MyDB_RecordPtr rhs = this->getEmptyRecord();
  function<bool()> comparator = buildComparator(lhs, rhs);

  MyDB_RecordPtr rec = this->getEmptyRecord();

  // low & high comparator
  auto comparators = this->getComparators(low, high, rec);

  return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pages, lhs, rhs, comparator, rec, comparators[0],
                                                          comparators[1], sort);
}

array<function<bool()>, 2> MyDB_BPlusTreeReaderWriter::getComparators(MyDB_AttValPtr low, MyDB_AttValPtr high, MyDB_RecordPtr rec)
{

  MyDB_INRecordPtr lowRec = this->getINRecord();
  lowRec->setKey(low);
  function<bool()> lowComparator = this->buildComparator(rec, lowRec);

  MyDB_INRecordPtr highRec = this->getINRecord();
  highRec->setKey(high);
  function<bool()> highComparator = this->buildComparator(highRec, rec);

  return {lowComparator, highComparator};
}

#endif