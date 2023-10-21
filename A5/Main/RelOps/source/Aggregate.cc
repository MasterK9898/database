
#ifndef AGG_CC
#define AGG_CC

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "../headers/Aggregate.h"
#include <unordered_map>
#include <memory>

using namespace std;

// SELECT SUM (att1 + att2), AVG (att1 + att2), att4, att6 - att8
// FROM input
// GROUP BY att4, att6 - att8

Aggregate ::Aggregate(MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                      vector<pair<MyDB_AggType, string>> aggsToCompute,
                      vector<string> groupings, string selectionPredicate) : input(input),
                                                                             output(output),
                                                                             aggsToCompute(aggsToCompute),
                                                                             groupings(groupings),
                                                                             selectionPredicate(selectionPredicate)
{
  // output->();
}
void Aggregate ::run()
{
  // the output is combining the aggsToCompute and groupings, basically putting the together

  unordered_map<size_t, void *> myHash;

  // we need to store the count in order to calculate the average
  // it can also be used to calculate the real count
  unordered_map<size_t, size_t> counts;

  MyDB_RecordPtr inputRec = input->getEmptyRecord();

  vector<func> groupingFuncs;
  for (auto &group : groupings)
  {
    groupingFuncs.push_back(inputRec->compileComputation(group));
  }

  std::cout << "group func length " << groupings.size() << endl;

  vector<pair<MyDB_AggType, func>> aggPairs;
  for (auto &agg : aggsToCompute)
  {
    aggPairs.push_back(make_pair(agg.first, inputRec->compileComputation(agg.second)));
  }

  // now get the predicate
  func pred = inputRec->compileComputation(this->selectionPredicate);

  // get all of the pages
  vector<MyDB_PageReaderWriter> allData;
  for (int i = 0; i < input->getNumPages(); i++)
  {
    MyDB_PageReaderWriter temp = input->operator[](i);
    if (temp.getType() == MyDB_PageType ::RegularPage)
    {
      allData.push_back(temp);
    }
  }

  // add all of the records to the hash table
  MyDB_RecordIteratorAltPtr myIter = getIteratorAlt(allData);

  MyDB_RecordPtr outputRec = output->getEmptyRecord();

  size_t pageNum = 0;
  output->operator[](pageNum).clear();

  while (myIter->advance())
  {
    // hash the current record
    myIter->getCurrent(inputRec);

    // if not qualified, skip
    if (!pred()->toBool())
    {
      continue;
    }

    // compute its hash
    size_t hashVal = 0;

    for (auto &f : groupingFuncs)
    {
      hashVal ^= f()->hash();
    }

    // if theere is no grouping, then every time we need to create a new row
    if (myHash.count(hashVal) == 0)
    {
      counts[hashVal] = 1;

      // initialze a new row
      int i = 0;
      for (auto &func : groupingFuncs)
      {
        outputRec->getAtt(i++)->set(func());
      }
      // cout << "rec att size " << i << endl;
      for (auto pair : aggPairs)
      {
        switch (pair.first)
        {
        case sum:
        {
          outputRec->getAtt(i)->set(pair.second());
          break;
        }
        case avg:
        {

          outputRec->getAtt(i)->set(pair.second());
          break;
        }
        case cnt:
        {
          auto att = make_shared<MyDB_IntAttVal>();
          att->set(1);
          outputRec->getAtt(i)->set(att);
          break;
        }
        default:
          break;
        }
        i++;
      }

      MyDB_PageReaderWriter page = output->operator[](pageNum);
      void *pointer = page.appendAndReturnLocation(outputRec);
      if (pointer == nullptr)
      {
        pageNum++;
        page = output->operator[](pageNum);
        pointer = page.appendAndReturnLocation(outputRec);
      }
      myHash[hashVal] = pointer;
    }

    else
    {
      // we get the original rec
      outputRec->fromBinary(myHash[hashVal]);

      counts[hashVal]++;

      // update the aggregate rows
      int i = groupingFuncs.size();

      for (auto pair : aggPairs)
      {
        switch (pair.first)
        {
        case sum:
        {
          double origin = outputRec->getAtt(i)->toDouble();
          double current = origin + pair.second()->toDouble();
          auto att = make_shared<MyDB_DoubleAttVal>();
          att->set(current);
          outputRec->getAtt(i)->set(att);
          break;
        }
        case avg:
        {
          double origin = outputRec->getAtt(i)->toDouble();
          int count = counts[hashVal];
          double current = (origin * (count - 1) + pair.second()->toDouble()) / count;
          auto att = make_shared<MyDB_DoubleAttVal>();
          att->set(current);
          outputRec->getAtt(i)->set(att);
          break;
        }
        case cnt:
        {
          int origin = outputRec->getAtt(i)->toInt();
          int current = origin + 1;
          auto att = make_shared<MyDB_IntAttVal>();
          att->set(current);
          outputRec->getAtt(i)->set(att);
          break;
        }
        default:
          break;
        }
        i++;
      }
    }
    cout << outputRec << endl;
    outputRec->recordContentHasChanged();
    outputRec->toBinary(myHash[hashVal]);
  }

  cout << "total page " << output->getNumPages() << endl;

  cout << ((PageMeta *)output->operator[](0).getBytes())->numBytesUsed << endl;

  auto temp = output->getEmptyRecord();
  auto iter = output->getIteratorAlt();

  cout << "Now we count the records.";
  cout << "\nThe output should be 413:\n";
  while (iter->advance())
  {
    iter->getCurrent(temp);
    cout << "here" << temp << "\n";
  }
}

#endif
