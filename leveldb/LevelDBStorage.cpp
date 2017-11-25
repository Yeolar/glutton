/*
 * Copyright (C) 2017, Yeolar
 */

#include "LevelDBStorage.h"
#include <raster/util/Logging.h>
#include "glutton/Helper.h"

namespace glutton {

bool LevelDBStorage::destroy(const std::string& dir) {
  leveldb::Status s = leveldb::DestroyDB(dir, leveldb::Options());
  if (!s.ok()) {
    RDDLOG(ERROR) << "leveldb: destroy " << dir << " error: " << s.ToString();
    return false;
  }
  return true;
}

bool LevelDBStorage::open(const std::string& dir) {
  leveldb::Options opt;
  opt.create_if_missing = true;
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(opt, dir, &db);
  if (!s.ok()) {
    RDDLOG(ERROR) << "leveldb: open " << dir << " error: " << s.ToString();
    return false;
  }
  db_.reset(db);
  return true;
}

bool LevelDBStorage::get(rdd::StringPiece key, std::string& value) {
  RDDCHECK(db_) << "leveldb: db not opened";
  leveldb::ReadOptions opt;
  leveldb::Status s = db_->Get(opt, slice(key), &value);
  if (!s.ok()) {
    RDDLOG(ERROR) << "leveldb: get (" << key << ")"
      << " error: " << s.ToString();
    return false;
  }
  return true;
}

bool LevelDBStorage::put(rdd::StringPiece key, rdd::StringPiece value) {
  RDDCHECK(db_) << "leveldb: db not opened";
  leveldb::WriteOptions opt;
  opt.sync = true;
  leveldb::Status s = db_->Put(opt, slice(key), slice(value));
  if (!s.ok()) {
    RDDLOG(ERROR) << "leveldb: put (" << key << ")"
      << " error: " << s.ToString();
    return false;
  }
  return true;
}

bool LevelDBStorage::del(rdd::StringPiece key) {
  RDDCHECK(db_) << "leveldb: db not opened";
  leveldb::WriteOptions opt;
  opt.sync = true;
  leveldb::Status s = db_->Delete(opt, slice(key));
  if (!s.ok()) {
    RDDLOG(ERROR) << "leveldb: delete (" << key << ")"
      << " error: " << s.ToString();
    return false;
  }
  return true;
}

} // namespace glutton
