/*
 * Copyright (C) 2017, Yeolar
 */

#include "glutton/CacheManager.h"
#include "LevelDBStorage.h"

namespace glutton {

bool CacheManager::initialize(size_t maxSize, const fs::path& dbDir) {
  dict_.reset(new MemoryFlatDict<std::string>(maxSize));
  storage_.reset(new LevelDBStorage());
  if (storage_->open(dbDir)) {
    return true;
  }
  return false;
}

bool CacheManager::get(const std::string& key, std::string& value) {
  auto block = dict_->get(key);
  if (block) {
    value.assign((char*)block.data.data(), block.data.size());
    return true;
  }
  return false;
}

bool CacheManager::put(const std::string& key, rdd::ByteRange value) {
  if (storage_->put(key, rdd::StringPiece(value))) {
    dict_->update(key, value);
    return true;
  }
  return false;
}

bool CacheManager::erase(const std::string& key) {
  if (storage_->del(key)) {
    dict_->erase(key);
    return true;
  }
  return false;
}

} // namespace glutton
