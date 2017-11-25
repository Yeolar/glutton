/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <string>
#include <ostream>
#include <iostream>
#include <string.h>

namespace rdd {

class QItem {
public:
  QItem() {}
  QItem(int64_t addTime, int64_t expiry, char *data, int length, int xid)
    : addTime_(addTime),
      expiry_(expiry),
      data_(data),
      length_(length),
      xid_(xid) {}

  QItem(const QItem &other) {
    addTime_ = other.addTime_;
    expiry_ = other.expiry_;
    data_ = (char *)malloc(other.length_);
    memcpy(data_, other.data_, other.length_);
    length_ = other.length_;
    xid_ = other.xid_;
  }

  ~QItem() {
    if (data_) {
      free(data_);
      data_ = nullptr;
    }
  }

  char *pack(int8_t opcode, bool withXid, int *len) {
    int headerSize = withXid ? 9 : 5;
    char *buffer = (char *)malloc(length_ + 16 + headerSize);
    int ret = 0;
    buffer[ret++] = opcode;
    if (withXid) {
      memcpy(buffer + ret, &xid_, sizeof(xid_));
      ret += sizeof(xid_);
    }
    int tmp = length_ + 16;
    memcpy(buffer + ret, &tmp, sizeof(int));
    ret += sizeof(int);
    memcpy(buffer + ret, &addTime_, sizeof(int64_t));
    ret += sizeof(int64_t);
    memcpy(buffer + ret, &expiry_, sizeof(int64_t));
    ret += sizeof(int64_t);
    memcpy(buffer + ret, data_, length_);
    ret += length_;
    *len = ret;
    return buffer;
  }

  void unpack(const char *buffer, int len) {
    addTime_ = *((int64_t *)(buffer));
    expiry_ = *((int64_t *)(buffer + sizeof(int64_t)));
    data_ = (char *)malloc(len - sizeof(int64_t) - sizeof(int64_t));
    memcpy(data_, buffer + sizeof(int64_t) + sizeof(int64_t),
           len - sizeof(int64_t) - sizeof(int64_t));
    length_ = len - sizeof(int64_t) - sizeof(int64_t);
    xid_ = 0;
  }

  void unpackOldAdd(const char *buffer, int len) {
    expiry_ = *((int *)buffer);
    if (expiry_)
      expiry_ *= 1000;
    data_ = (char *)malloc(len - sizeof(int));
    memcpy(data_, buffer + sizeof(int), len - sizeof(int));
    length_ = len - sizeof(int);
    addTime_ = time(nullptr);
    xid_ = 0;
  }

  void setXid(int xid) { xid_ = xid; }
  int getXid() { return xid_; }

  int64_t getAddTime() const { return addTime_; }
  int64_t getExpiry() const { return expiry_; }
  char *getData() const { return data_; }
  int getLength() const { return length_; }
  int getXid() const { return xid_; }

  friend std::ostream &operator<<(std::ostream &os, QItem &item);

private:
  int64_t addTime_{0};
  int64_t expiry_{0};
  char *data_{nullptr};
  int length_{0};
  int xid_{0};
};

inline std::ostream& operator<<(std::ostream& os, QItem& item) {
  os << "addTime: " << item.addTime_ << std::endl;
  os << "expiry: " << item.expiry_ << std::endl;
  os << "data: " << item.data_ << std::endl;
  os << "xid: " << item.xid_ << std::endl;
  return os;
}

}

