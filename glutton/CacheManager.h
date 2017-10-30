/*
 * Copyright (C) 2017, Yeolar
 */

#include "LevelDBStorage.h"
#include "glutton/MemoryFlatDict.h"
#include "glutton/Storage.h"

namespace glutton {

class CacheManager {
public:
  CacheManager() {}

  bool initialize(size_t maxSize, const fs::path& dbDir) {
    dict_.reset(new MemoryFlatDict<std::string>(maxSize));
    storage_.reset(new LevelDBStorage());
    if (storage_->open(dbDir)) {
      return true;
    }
    return false;
  }

  bool get(const std::string& key, std::string& value) {
    auto block = dict_->get(key);
    if (block) {
      value.assign((char*)block.data.data(), block.data.size());
      return true;
    }
    return false;
  }

  bool put(const std::string& key, rdd::ByteRange value) {
    if (storage_->put(key, rdd::StringPiece(value))) {
      dict_->update(key, value);
      return true;
    }
    return false;
  }

  bool erase(const std::string& key) {
    if (storage_->del(key)) {
      dict_->erase(key);
      return true;
    }
    return false;
  }

private:
  std::unique_ptr<MemoryFlatDict<std::string>> dict_;
  std::unique_ptr<Storage> storage_;
};

} // namespace glutton
