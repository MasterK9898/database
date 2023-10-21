
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"
#include <cstring>
#include <string>

SortMergeJoin ::SortMergeJoin(MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
                              MyDB_TableReaderWriterPtr output, string finalSelectionPredicate,
                              vector<string> projections,
                              pair<string, string> equalityCheck, string leftSelectionPredicate,
                              string rightSelectionPredicate)
    : leftInput(leftInput), rightInput(rightInput), output(output),
      finalSelectionPredicate(finalSelectionPredicate), projections(projections),
      equalityCheck(equalityCheck), leftSelectionPredicate(leftSelectionPredicate), rightSelectionPredicate(rightSelectionPredicate) {}

void SortMergeJoin ::run()
{
  MyDB_RecordPtr leftInputRec = leftInput->getEmptyRecord();
  MyDB_RecordPtr leftInputRec2 = leftInput->getEmptyRecord();
  function<bool()> leftComparator = buildRecordComparator(leftInputRec, leftInputRec2, equalityCheck.first);
  MyDB_RecordIteratorAltPtr leftIterator = buildItertorOverSortedRuns(leftInput->getBufferMgr()->getPageSize() / 2, *leftInput,
                                                                      leftComparator, leftInputRec, leftInputRec2, leftSelectionPredicate);
  MyDB_RecordPtr rightInputRec = rightInput->getEmptyRecord();
  MyDB_RecordPtr rightInputRec2 = rightInput->getEmptyRecord();
  function<bool()> rightComparator = buildRecordComparator(rightInputRec, rightInputRec2, equalityCheck.second);
  MyDB_RecordIteratorAltPtr rightIterator = buildItertorOverSortedRuns(leftInput->getBufferMgr()->getPageSize() / 2, *rightInput,
                                                                       rightComparator, rightInputRec, rightInputRec2, rightSelectionPredicate);

  // and get the schema that results from combining the left and right records
  MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();
  for (auto &p : leftInput->getTable()->getSchema()->getAtts())
    mySchemaOut->appendAtt(p);
  for (auto &p : rightInput->getTable()->getSchema()->getAtts())
    mySchemaOut->appendAtt(p);

  // get the combined record
  MyDB_RecordPtr combinedRec = make_shared<MyDB_Record>(mySchemaOut);
  combinedRec->buildFrom(leftInputRec, rightInputRec);

  // now, get the final predicate over it
  func finalPredicate = combinedRec->compileComputation(finalSelectionPredicate);

  // and get the final set of computatoins that will be used to buld the output record
  vector<func> finalComputations;
  for (string s : projections)
  {
    finalComputations.push_back(combinedRec->compileComputation(s));
  }

  // this is the output record
  MyDB_RecordPtr outputRec = output->getEmptyRecord();

  // we have [l_suppkey] and [r_suppkey]
  // we need == ([l_suppkey], [r_suppkey])
  auto buildPredicate = [this](string prefix, MyDB_RecordPtr combinedRec)
  {
    return combinedRec->compileComputation(prefix + " (" + equalityCheck.first + ", " + equalityCheck.second + ")");
  };
  func less = buildPredicate("<", combinedRec);
  func greater = buildPredicate(">", combinedRec);
  func equal = buildPredicate("==", combinedRec);

  // helper comparing for left
  MyDB_RecordPtr combinedRec2 = make_shared<MyDB_Record>(mySchemaOut);
  combinedRec2->buildFrom(leftInputRec2, rightInputRec);
  func equal2 = buildPredicate("==", combinedRec2);

  // helper 2 comparing for right
  MyDB_RecordPtr combinedRec3 = make_shared<MyDB_Record>(mySchemaOut);
  combinedRec3->buildFrom(leftInputRec, rightInputRec2);
  func equal3 = buildPredicate("==", combinedRec3);

  // nothing to iterate
  if (!leftIterator->advance() || !rightIterator->advance())
  {
    return;
  }

  // go one by one, if there's equalify, join all the equal ones
  while (true)
  {
    bool finished = false;
    leftIterator->getCurrent(leftInputRec);
    rightIterator->getCurrent(rightInputRec);

    if (less()->toBool())
    // if left is smaller, advance left
    {
      if (!leftIterator->advance())
        finished = true;
    }
    else if (greater()->toBool())
    // if right is smaller, advance right
    {
      if (!rightIterator->advance())
        finished = true;
    }
    else if (equal()->toBool())
    // the meat, basically, join all the equal ones
    {
      // hold all equal record in the left
      vector<MyDB_PageReaderWriter> leftPool;
      MyDB_PageReaderWriter leftPage(true, *(leftInput->getBufferMgr()));
      leftPool.push_back(leftPage);
      // hold all equal record in the right
      vector<MyDB_PageReaderWriter> rightPool;
      MyDB_PageReaderWriter rightPage(true, *(rightInput->getBufferMgr()));
      rightPool.push_back(rightPage);

      // find all equalities in left
      while (true)
      {
        leftIterator->getCurrent(leftInputRec2);
        if (equal2()->toBool())
        {
          if (!leftPage.append(leftInputRec2))
          // if failed, create a new page
          {
            leftPage = MyDB_PageReaderWriter(true, *(leftInput->getBufferMgr()));
            leftPool.push_back(leftPage);
            leftPage.append(leftInputRec2);
          }
        }
        else
        {
          break;
        }
        if (!leftIterator->advance())
        {
          finished = true;
          break;
        }
      }

      // find all equalities in right
      while (true)
      {
        rightIterator->getCurrent(rightInputRec2);
        // the latter is clearly greater or equal to the prev
        // so we need to check if rec2 is not greater than rec1
        if (equal3()->toBool())
        {
          if (!rightPage.append(rightInputRec2))
          // if failed, create a new page
          {
            rightPage = MyDB_PageReaderWriter(true, *(rightInput->getBufferMgr()));
            rightPool.push_back(rightPage);
            rightPage.append(rightInputRec2);
          }
        }
        else
        {
          break;
        }
        if (!rightIterator->advance())
        {
          finished = true;
          break;
        }
      }
      // merge all the matches and output
      MyDB_RecordIteratorAltPtr IteratorAltLeft = getIteratorAlt(leftPool);

      while (IteratorAltLeft->advance())
      {
        IteratorAltLeft->getCurrent(leftInputRec);

        MyDB_RecordIteratorAltPtr IteratorAltRight = getIteratorAlt(rightPool);

        while (IteratorAltRight->advance())
        {
          IteratorAltRight->getCurrent(rightInputRec);
          // check final predicate
          if (finalPredicate()->toBool())
          {

            // run all of the computations
            int i = 0;
            for (auto &f : finalComputations)
            {
              outputRec->getAtt(i++)->set(f());
            }

            // the record's content has changed because it
            // is now a composite of two records whose content
            // has changed via a read... we have to tell it this,
            // or else the record's internal buffer may cause it
            // to write old values
            outputRec->recordContentHasChanged();
            output->append(outputRec);
          }
        }
      }
    }
    if (finished)
      break;
  }
}

#endif
