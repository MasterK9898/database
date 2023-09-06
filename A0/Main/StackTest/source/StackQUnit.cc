
#ifndef STACK_TEST_CC
#define STACK_TEST_CC

#include "Stack.h"
#include "QUnit.h"
#include <memory>
#include <iostream>
#include <vector>

int main()
{

	QUnit::UnitTest qunit(std ::cerr, QUnit ::verbose);

	// UNIT TEST 1
	// just make sure we can create an empty stack, and that it is empty
	{

		Stack<int> myStack;
		QUNIT_IS_EQUAL(myStack.isEmpty(), true);
	}

	// UNIT TEST 2
	// make sure we can push/pop a bunch of items
	{

		Stack<int> myStack;
		for (int i = 0; i < 10; i++)
		{
			myStack.push(i);
		}

		// now, pop everything off
		for (int i = 0; i < 10; i++)
		{
			QUNIT_IS_EQUAL(myStack.pop(), 9 - i);
		}
	}

	// UNIT TEST 3
	// make sure that the destructor works correctly... unless the stack correctly deallocates
	// all of the items it contains, this test will fail
	{
		static int temp = 0;
		struct Tester
		{
			Tester() { temp++; }
			~Tester() { temp--; }
		};

		{
			Stack<std ::shared_ptr<Tester>> myStack;
			std ::shared_ptr<Tester> myGuy = std ::make_shared<Tester>();
			for (int i = 0; i < 10; i++)
			{
				myStack.push(myGuy);
			}
		}

		QUNIT_IS_EQUAL(temp, 0);
	}

	// UNIT TEST 4
	// make sure we can push/pop a bunch of items and then do it again
	{

		Stack<int> myStack;
		for (int i = 0; i < 10; i++)
		{
			myStack.push(i);
		}

		for (int i = 0; i < 5; i++)
		{
			QUNIT_IS_EQUAL(myStack.pop(), 9 - i);
		}

		for (int i = 5; i < 20; i++)
		{
			myStack.push(i);
		}

		for (int i = 0; i < 20; i++)
		{
			QUNIT_IS_EQUAL(myStack.pop(), 19 - i);
		}

		QUNIT_IS_EQUAL(myStack.isEmpty(), true);
	}

	// UNIT TEST 5
	// check for non-trival types
	{
		Stack<std::vector<int>> myStack;
		std::vector<int> vec(10, 42);
		myStack.push(vec);
		std::vector<int> test = myStack.pop();
		QUNIT_IS_EQUAL(test.size(), 10);
		QUNIT_IS_EQUAL(test.at(0), 42);
	}
}

#endif
