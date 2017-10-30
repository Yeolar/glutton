/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <functional>
#include <raster/io/FSUtil.h>
#include "glutton/Storage.h"
#include "db/db.h"

namespace glutton {

// https://rawgit.com/google/leveldb/master/doc/index.html

class LevelDBStorage : public Storage {
public:
  static bool destroy(const fs::path& dir);

  LevelDBStorage() {}
  virtual ~LevelDBStorage() {}

  virtual bool open(const fs::path& dir);

  operator bool() const {
    return db_ != nullptr;
  }

  leveldb::DB* getDB() const {
    return db_.get();
  }

  virtual bool get(rdd::StringPiece key, std::string& value);
  virtual bool put(rdd::StringPiece key, rdd::StringPiece value);
  virtual bool del(rdd::StringPiece key);

private:
  std::unique_ptr<leveldb::DB> db_;
};

} // namespace glutton
