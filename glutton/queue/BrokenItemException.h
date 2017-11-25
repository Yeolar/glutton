/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <string>
#include <exception>

namespace rdd {

class BrokenItemException : std::exception {
public:
  explicit BrokenItemException(int64_t pos, const std::string& w) throw()
    : pos_(pos), what_(w) {}

  ~BrokenItemException() throw() {}

  const char *what() const throw() { return what_.c_str(); }

  int64_t lastValidPosition() throw() { return pos_; }

private:
  int64_t pos_;
  std::string what_;
};

}

