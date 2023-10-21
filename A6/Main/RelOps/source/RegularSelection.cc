
#ifndef REG_SELECTION_C
#define REG_SELECTION_C

#include "RegularSelection.h"

RegularSelection ::RegularSelection(MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                                    string selectionPredicate, vector<string> projections) : input(input), output(output), selectionPredicate(selectionPredicate), projections(projections) {}

void RegularSelection ::run()
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

  MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt();

  // basically, it's just iterate through and put all qualified target onto the output
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
