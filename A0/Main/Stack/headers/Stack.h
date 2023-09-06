
#ifndef STACK_H
#define STACK_H

#include <iostream>
// #include <memory>

// this is the node class used to build up the LIFO stack
template <class Data>
class Node
{

private:
	Data holdMe;
	Node *next;

public:
	Node(Data data, Node<Data> *nxt = nullptr) : holdMe(data), next(nxt) {}
	// {
	// 	std::cout << "Node created with value: " << holdMe << std::endl;
	// }

	Data getData()
	{
		return this->holdMe;
	}

	void setData(Data data)
	{
		this->holdMe = data;
	}

	Node *getNext()
	{
		return this->next;
	}

	void setNext(Node *next)
	{
		this->next = next;
	}
};

// a simple LIFO stack
template <class Data>
class Stack
{

	Node<Data> *head;

public:
	// destroys the stack
	~Stack()
	{ // throw everything off the stack, to delete them
		while (!this->isEmpty())
		{
			this->pop();
		}
	}

	// creates an empty stack
	Stack() : head(nullptr) {}

	// adds data to the top of the stack
	void push(Data data)
	{ /* your code here */
		Node<Data> *next = new Node<Data>(data, head);
		this->head = next;
	}

	// return true if there are not any items in the stack
	bool isEmpty()
	{ /* replace with your code */
		return this->head == nullptr;
	}

	// pops the item on the top of the stack off, returning it...
	// if the stack is empty, the behavior is undefined
	Data pop()
	{ /* replace with your code */
		if (this->head == nullptr)
		{
			throw std::runtime_error("Empty Stack");
		}
		Node<Data> *target = this->head;
		Data result = target->getData();
		Node<Data> *next = this->head->getNext();
		this->head = next;
		// no need for this pointer any more, say goodbye to it
		delete target;
		return result;
	}
};

#endif
