/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <string>
#include <sstream>
#include <deque>
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include "raster/util/Conv.h"
#include "QItem.h"

namespace rdd {

class Journal {
public:
  class IOException : std::exception {
  public:
    explicit IOException(const std::string& w) throw() : what_(w) {}
    ~IOException() throw() {}

    const char *what() const throw() { return what_.c_str(); }

  private:
    std::string what_;
  };

  enum : char {
    CMD_ADD              = 0,
    CMD_REMOVE           = 1,
    CMD_ADDX             = 2,
    CMD_REMOVE_TENTATIVE = 3,
    CMD_SAVE_XID         = 4,
    CMD_UNREMOVE         = 5,
    CMD_CONFIRM_REMOVE   = 6,
    CMD_ADD_XID          = 7,
  };

  Journal(const std::string& queueFile, bool syncJournal)
    : queueFile_(queueFile), syncJournal_(syncJournal) {}

  void open() {
    open(queueFile_);
  }

  void roll(int xid, std::deque<QItem *>& openItems, std::deque<QItem *>& queue);

  void close() {
    if (writer_) {
      std::fclose(writer_);
      writer_ = nullptr;
    }
    if (reader_) {
      std::fclose(reader_);
      reader_ = nullptr;
    }
  }

  void erase() {
    close();
    std::remove(queueFile_.c_str());
  }

  bool inReadBehind() {
    return (reader_ != nullptr);
  }

  bool isReplaying() {
    return (replayer_ != nullptr);
  }

  void add(QItem& item) {
    add(true, item);
  }

  void remove() {
    size_ += write(true, CMD_REMOVE);
  }

  void removeTentative() {
    removeTentative(true);
  }

  void saveXid(int xid) {
    size_ += write(false, CMD_SAVE_XID, xid);
  }

  void unremove(int xid) {
    size_ += write(true, CMD_UNREMOVE, xid);
  }

  void confirmRemove(int xid) {
    size_ += write(true, CMD_CONFIRM_REMOVE, xid);
  }

  void startReadBehind() {
    size_t pos = replayer_ ? ftell(replayer_) : ftell(writer_);
    if ( !(reader_ = fopen(queueFile_.c_str(), "r")) ) {
      assert(0);
    }
    fseek(reader_, pos, SEEK_SET);
  }

  void fillReadBehind(void (*f)(QItem *item, void *context), void *context);

  void replay(const std::string& name,
              void (*f)(char opcode, QItem *item, int xid, void *context),
              void *context);

  size_t size() { return size_; }

private:
  void open(const std::string& file) {
    if ( !(writer_ = fopen(file.c_str(), "a")) ) {
      throw IOException("Can't open " + file + ":" + strerror(errno));
    }
  }

  void add(bool allowSync, QItem& item) {
    int len = 0;
    char *blob = item.pack(CMD_ADDX, false, &len);
    if (fwrite(blob, len, 1, writer_) != 1) {
      throw IOException("Failed to write " + queueFile_ + ":" + strerror(ferror(writer_)));
    }
    if (allowSync && syncJournal_) fflush(writer_);
    free(blob);
    size_ += len;
  }

  void addWithXid(QItem& item) {
    int len = 0;
    char *blob = item.pack(CMD_ADD_XID, true, &len);
    if (fwrite(blob, len, 1, writer_) != 1) {
      throw IOException("Failed to write " + queueFile_ + ":" + strerror(ferror(writer_)));
    }
    free(blob);
    size_ += len;
  }

  void removeTentative(bool allowSync) {
    size_ += write(allowSync, CMD_REMOVE_TENTATIVE);
  }

  char *readBlock(FILE *in, int *sizep);

  int readInt(FILE *in);

  int write(bool allowSync, char b) {
    if (fwrite(&b, 1, 1, writer_) != 1) {
      throw IOException("Failed to write " + queueFile_ + ":" + strerror(ferror(writer_)));
    }
    if (allowSync && syncJournal_) fflush(writer_);
    return 1;
  }

  int write(bool allowSync, char b, int i) {
    if (fwrite(&b, 1, 1, writer_) != 1) {
      throw IOException("Failed to write " + queueFile_ + ":" + strerror(ferror(writer_)));
    }
    if (fwrite((char *)(&i), 4, 1, writer_) != 1) {
      throw IOException("Failed to write " + queueFile_ + ":" + strerror(ferror(writer_)));
    }
    if (allowSync && syncJournal_) fflush(writer_);
    return 5;
  }

  QItem* readJournalEntry(FILE *in, char *x, int *size, int *xid);

  void truncateJournal(int64_t pos) {
    truncate(queueFile_.c_str(), pos);
  }

  std::string ltoa(int64_t d) { return to<std::string>(d); }

  FILE *reader_{nullptr};
  FILE *writer_{nullptr};
  FILE *replayer_{nullptr};
  std::string queueFile_;
  bool syncJournal_;
  size_t size_{0};
};

}

