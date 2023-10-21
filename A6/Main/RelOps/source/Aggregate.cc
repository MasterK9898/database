
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
}
void Aggregate ::run()
{
  unordered_map<size_t, void *> myHash;

  MyDB_RecordPtr inputRec = input->getEmptyRecord();

  MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();
  for (auto p : output->getTable()->getSchema()->getAtts())
    mySchemaOut->appendAtt(p);
  MyDB_RecordPtr outputRec = make_shared<MyDB_Record>(mySchemaOut);

  vector<func> groupFunc;
  for (auto g : groupings)
  {
    groupFunc.push_back(inputRec->compileComputation(g));
  }

  vector<pair<MyDB_AggType, func>> aggTuple;
  for (auto agg : aggsToCompute)
  {
    aggTuple.push_back(make_pair(agg.first, inputRec->compileComputation(agg.second)));
  }

  func pred = inputRec->compileComputation(selectionPredicate);

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

  MyDB_RecordIteratorAltPtr myIter = getIteratorAlt(allData);

  // we need to remember the sum, because the number on record may be truncated, so info will lost
  unordered_map<size_t, vector<double>> sums;
  unordered_map<size_t, int> counts;
  // we can use count and sum to calculate the average

  int pageIndex = 0;
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
    for (auto f : groupFunc)
    {
      hashVal ^= f()->hash();
    }

    if (myHash.count(hashVal) == 0)
    {
      counts[hashVal] = 1;

      // run all of the computations
      int i = 0;
      for (auto f : groupFunc)
      {
        outputRec->getAtt(i++)->set(f());
      }

      i = groupFunc.size();
      for (auto p : aggTuple)
      {
        switch (p.first)
        {
        case MyDB_AggType::sum:
        // the beiginning part of sum and avg are the same
        case MyDB_AggType::avg:
        {
          outputRec->getAtt(i)->set(p.second());
          sums[hashVal].push_back(outputRec->getAtt(i)->toDouble());
          break;
        }
        case MyDB_AggType::cnt:
        {
          MyDB_IntAttValPtr att = make_shared<MyDB_IntAttVal>();
          att->set(1);
          outputRec->getAtt(i)->set(att);
          break;
        }
        default:
          break;
        }
        i++;
      }

      outputRec->recordContentHasChanged();

      MyDB_PageReaderWriter currentPage = output->getPinned(pageIndex);
      void *ret = currentPage.appendAndReturnLocation(outputRec);

      if (ret == nullptr)
      {
        currentPage = output->getPinned(++pageIndex);
        myHash[hashVal] = currentPage.appendAndReturnLocation(outputRec);
      }

      myHash[hashVal] = ret;
    }
    else
    {
      outputRec->fromBinary(myHash[hashVal]);

      counts[hashVal]++;

      int count = counts[hashVal];

      // if I dont use reference, then hash table will give me a clone
      // I debugged it for 30 minutes
      vector<double> &sum = sums[hashVal];
      int sumIndex = 0;

      int i = groupFunc.size();
      for (auto p : aggTuple)
      {
        // the logic of avg and sum are about the same
        // only part is different is that avg need to be devided by the count
        // we can use the fall back nature of switch to perform a twist
        int denominator = 1;
        switch (p.first)
        {
        case MyDB_AggType::avg:
          // for average, we need to divide the sum by the count
          // than fall back to sum
          denominator = count;
        case MyDB_AggType::sum:
        {
          sum[sumIndex] += p.second()->toDouble();
          double current = sum[sumIndex] / denominator;
          sumIndex++;

          MyDB_DoubleAttValPtr newAtt = make_shared<MyDB_DoubleAttVal>();
          newAtt->set(current);
          outputRec->getAtt(i)->set(newAtt);
          break;
        }
        case MyDB_AggType::cnt:
        {
          MyDB_IntAttValPtr newAtt = make_shared<MyDB_IntAttVal>();
          newAtt->set(count);
          outputRec->getAtt(i)->set(newAtt);
          break;
        }
        }
        i++;
      }

      outputRec->recordContentHasChanged();
      outputRec->toBinary(myHash[hashVal]);
    }
  }
}

#endif
// 100|10000.000000|8968.420000|32|