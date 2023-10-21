
#ifndef BPLUS_SELECTION_C
#define BPLUS_SELECTION_C

#include "BPlusSelection.h"

using namespace std;

BPlusSelection ::BPlusSelection(MyDB_BPlusTreeReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                                MyDB_AttValPtr low, MyDB_AttValPtr high,
                                string selectionPredicate, vector<string> projections) : input(input), output(output), low(low), high(high), selectionPredicate(selectionPredicate), projections(projections)
{
}

void BPlusSelection ::run()
{
  MyDB_RecordPtr inputRec = input->getEmptyRecord();
  MyDB_RecordPtr outputRec = output->getEmptyRecord();

  vector<func> projectionFuncs;
  for (auto &projection : projections)
  {
    projectionFuncs.push_back(inputRec->compileComputation(projection));
  }

  // get the qualifier
  func pred = inputRec->compileComputation(selectionPredicate);

  MyDB_RecordIteratorAltPtr myIter = input->getRangeIteratorAlt(low, high);

  while (myIter->advance())
  {
    myIter->getCurrent(inputRec);

    // failed, next one
    if (!pred()->toBool())
    {
      continue;
    }

    // copy each attribute
    int i = 0;
    for (auto &f : projectionFuncs)
    {
      outputRec->getAtt(i++)->set(f());
    }

    outputRec->recordContentHasChanged();
    output->append(outputRec);
  }
}

#endif
