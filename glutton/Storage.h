/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <string>
#include <raster/io/FSUtil.h>
#include <raster/util/Range.h>

namespace glutton {

class Storage {
public:
  virtual bool open(const fs::path& dir) = 0;

  virtual bool get(rdd::StringPiece key, std::string& value) = 0;
  virtual bool put(rdd::StringPiece key, rdd::StringPiece value) = 0;
  virtual bool del(rdd::StringPiece key) = 0;
};

} // namespace glutton
