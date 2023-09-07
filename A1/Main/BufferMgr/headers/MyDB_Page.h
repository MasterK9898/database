
#ifndef PAGE_H
#define PAGE_H

#include <iostream>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
// a page object that stores the meat
class MyDB_Page
{

public:
  // when first created, the page is guaranteened for the area of space
  MyDB_Page(void *buffer, size_t pageSize, bool pinned, long index, string filename)
      : pageIndex(index), filename(filename), pinned(pinned), buffer(buffer), pageSize(pageSize)
  {
    int fd = open(this->filename.c_str(), O_CREAT | O_RDONLY | S_IRUSR | O_SYNC);
    lseek(fd, this->getOffset(), SEEK_SET);
    read(fd, this->buffer, this->pageSize);
    close(fd);
  }
  // whipe out the data when destroyed
  ~MyDB_Page()
  {
    if (this->dirty)
    // wrtie back if dirty
    {
      int fd = open(this->filename.c_str(), O_CREAT | O_RDWR | S_IWUSR | S_IRUSR | O_SYNC);
      lseek(fd, this->getOffset(), SEEK_SET);
      write(fd, this->buffer, this->pageSize);
      close(fd);
    }

    // wipe out the page
    memset(this->buffer, 0, this->pageSize);
  }

  // DATA METHODS

  // get the data
  void *getBytes()
  {

    std::cout << "getting the target buffer at page" << std::endl;
    std::cout << this->buffer << "buffer in page" << std::endl;

    std::cout << std::endl;
    return this->buffer;
  }
  // REFERENCE COUNT METHODS

  // increment the reference count
  void addRef()
  {
    this->ref++;
  }

  // decrement the reference count
  void removeRef()
  {
    this->ref--;
  }

  // get the reference count
  int getRef()
  {
    return this->ref;
  }

  // PINNED METHODS

  // set the pinned status
  void setPinned(bool pinned)
  {
    this->pinned = pinned;
  }

  // get the pinned status
  bool getPinned()
  {
    return this->pinned;
  }

  // DIRTY METHODS

  // set dirty bit
  void setDirty(bool dirty)
  {
    this->dirty = dirty;
  }

  // get dirty bit
  bool getDirty()
  {
    return this->dirty;
  }

  // TIME STAMP METHODS

  // get the last accessed time
  size_t getTimeStamp()
  {
    return this->timeStamp;
  }

  // set the last modified time
  void setTimeStamp(size_t timeStamp)
  {
    this->timeStamp = timeStamp;
  }

  // TABLE AND INDEX METHOD

  // get current page index
  long getPageIndex()
  {
    return this->pageIndex;
  }

  // set the page index
  // void setPageIndex(long index)
  // {
  //   this->pageIndex = index;
  // }

  // get the filename
  string getPageFilename()
  {
    return this->filename;
  }

private:
  // the index of the page, to calculate offset
  long pageIndex;
  // the table this page belongs to
  string filename;
  // pinned or not
  bool pinned;
  // dirty or not
  bool dirty;
  // the meat
  void *buffer;
  // reference count
  int ref;
  // last modifed
  size_t timeStamp;
  // page size
  size_t pageSize;

  // calc the index to read or wrtie from the disk file
  size_t getOffset()
  {
    return this->pageIndex * this->pageSize;
  }
};

#endif