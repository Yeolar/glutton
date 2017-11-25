/*
 * Copyright (C) 2017, Yeolar
 */

#include "Journal.h"
#include "BrokenItemException.h"

#include <fstream>
#include <iostream>
#include <string>
#include <assert.h>

using namespace std;

namespace rdd {

void Journal::roll(int xid, deque<QItem *>& openItems, deque<QItem *>& queue) {
  fclose(writer_);
  writer_ = nullptr;
  char tbuf[16];
  sprintf(tbuf, "%lu", time(nullptr));
  string tmpFile = queueFile_ + "~~" + tbuf;
  open(tmpFile);
  size_ = 0;
  for(deque<QItem *>::iterator it = openItems.begin(); it != openItems.end(); ++it) {
    addWithXid(**it);
    removeTentative(false);
  }
  saveXid(xid);
  for(deque<QItem *>::iterator it = queue.begin(); it != queue.end(); ++it) {
    add(false, **it);
  }

  fclose(writer_);
  writer_ = nullptr;
  rename(tmpFile.c_str(), queueFile_.c_str());
  open();
}

void Journal::fillReadBehind(void (*f)(QItem *item, void *context), void *context) {
  long pos = replayer_ ? ftell(replayer_) : ftell(writer_);
  if (ftell(reader_) == pos) {
    fclose(reader_);
    reader_ = nullptr;
  } else {
    char c;
    int xid;
    int size;
    QItem *item = readJournalEntry(reader_, &c, &size, &xid);
    if (item) {
      assert(c == CMD_ADD || c == CMD_ADDX || c == CMD_ADD_XID);
      f(item, context);
    }
  }
}

void Journal::replay(const string& name, void (*f)(char opcode, QItem *item, int xid, void *context), void *context) {
  size_ = 0;
  size_t lastUpdate = 0L;
  size_t TEN_MB = 10L * 1024 * 1024;

  try {
    if ( !(replayer_ = fopen(queueFile_.c_str(), "a+")) ) {
      throw IOException("Can't open " + queueFile_ + ":" + strerror(errno));
    }
    bool done = false;

    char opcode;
    int xid;
    int size;

    do {
      QItem *item = readJournalEntry(replayer_, &opcode, &size, &xid);
      if (size == 0) {
        assert(item == nullptr);
        done = true;
      } else {
        size_ += size;
        f(opcode, item, xid, context);

        if (size_ / TEN_MB > lastUpdate) {
          lastUpdate = size_ / TEN_MB;
          //notice_out( "Continuing to read '%s' journal; %lu MB so far...\n", name.c_str(), lastUpdate * 10);
        }
      }
    } while(!done);
    fclose(replayer_);
    replayer_ = nullptr;
  } catch (BrokenItemException& e) {
    //notice_out( "Exception replaying journal for '%s';\n DATA MAY HAVE BEEN LOST! Truncated entry will be deleted.\n", name.c_str());
    truncateJournal(e.lastValidPosition());
    fclose(replayer_);
    replayer_ = nullptr;
  } catch (IOException& e) {
    //erro_out( "%s", e.what());
    throw;
  }

}

char *Journal::readBlock(FILE *in, int *size) {
  if (fread((char *)size, 4, 1, in) != 1) {
    throw IOException("Failed to read size " + queueFile_);
  }

  assert(*size > 0);
  char *buffer = (char *)malloc(*size);
  assert(buffer);
  if (fread(buffer, *size, 1, in) != 1) {
    free(buffer);
    throw IOException("Failed to read buffer " + queueFile_);
  }

  return buffer;
}

int Journal::readInt(FILE *in) {
  int x;
  if (fread((char *)(&x), 4, 1, in) != 1) {
    throw IOException("Failed to read int " + queueFile_);
  }

  return x;
}

QItem* Journal::readJournalEntry(FILE *in, char *x, int *size, int *xid) {
  long lastPosition = ftell(in);
  *size = 0;

  char *data = nullptr;
  QItem *item = nullptr;

  try {

  if (fread(x, 1, 1, in) != 1) {
    if (feof(in)) {
      return nullptr;
    }
    throw IOException("Failed to read cmd " + queueFile_ + ":" + strerror(ferror(in)) + " pos: " + ltoa(ftell(in)));
  }

  switch(*x) {
  case CMD_ADD:
    item = new QItem();
    //debug_out( "#Journal::ADD# new(\033[31m%p\033[0m)\n", item);
    data = readBlock(in, size);
    item->unpackOldAdd(data, *size);
    *size += 5;
    free(data);
    break;
  case CMD_REMOVE:
    *size = 1;
    break;
  case CMD_ADDX:
    item = new QItem();
    //debug_out( "#Journal::ADDX# new(\033[31m%p\033[0m)\n", item);
    data = readBlock(in, size);
    item->unpack(data, *size);
    *size += 5;
    free(data);
    break;
  case CMD_REMOVE_TENTATIVE:
    *size = 1;
    break;
  case CMD_SAVE_XID:
    *xid = readInt(in);
    *size = 5;
    break;
  case CMD_UNREMOVE:
    *xid = readInt(in);
    *size = 5;
    break;
  case CMD_CONFIRM_REMOVE:
    *xid = readInt(in);
    *size = 5;
    break;
  case CMD_ADD_XID:
    item = new QItem();
    //debug_out( "#Journal::ADD_XID# new(\033[31m%p\033[0m)\n", item);
    *xid = readInt(in);
    data = readBlock(in, size);
    item->unpack(data, *size);
    item->setXid(*xid);
    *size += 9;
    free(data);
    break;
  default:
    throw BrokenItemException(lastPosition, "invalid opcode in journal");
    break;
  }

  } catch (IOException& e) {
    if (item) delete(item);
    throw BrokenItemException(lastPosition, e.what());
  }
  return item;
}

}

