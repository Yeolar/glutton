/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include "glutton/MemoryFlatDict.h"
#include "glutton/Storage.h"

namespace glutton {

class CacheManager {
public:
  CacheManager() {}

  bool initialize(size_t maxSize, const std::string& dbDir);

  bool get(const std::string& key, std::string& value);
  bool put(const std::string& key, rdd::ByteRange value);
  bool erase(const std::string& key);

private:
  std::unique_ptr<MemoryFlatDict<std::string>> dict_;
  std::unique_ptr<Storage> storage_;
};

} // namespace glutton
