/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <raster/io/Cursor.h>
#include <raster/io/TypedIOBuf.h>
#include <raster/util/AtomicUnorderedMap.h>
#include <raster/util/RWSpinLock.h>

namespace glutton {

namespace detail {

struct MutableLockedIOBuf {
  explicit MutableLockedIOBuf(std::unique_ptr<rdd::IOBuf>&& init)
    : buf_(std::move(init)) {}

  std::unique_ptr<rdd::IOBuf> clone() const {
    rdd::RWSpinLock::ReadHolder guard(lock_);
    return buf_ ? buf_->cloneOne() : nullptr;
  }

  void reset(std::unique_ptr<rdd::IOBuf>&& other) const {
    rdd::RWSpinLock::WriteHolder guard(lock_);
    buf_ = std::move(other);
  }

private:
  mutable std::unique_ptr<rdd::IOBuf> buf_;
  mutable rdd::RWSpinLock lock_;
};

} // namespace detail

template <
  typename Key,
  typename Hash = std::hash<Key>,
  typename KeyEqual = std::equal_to<Key>,
  typename IndexType = uint64_t,
  typename Allocator = rdd::MMapAlloc>
class MemoryFlatDict {
public:
  struct Block {
    Block(std::unique_ptr<rdd::IOBuf>&& buf) : buf_(std::move(buf)) {
      if (buf_) {
        rdd::io::Cursor cursor(buf_.get());
        ts = cursor.read<uint64_t>();
        data.reset(cursor.data(), cursor.length());
      }
    }

    uint64_t ts{0};
    rdd::ByteRange data;

    explicit operator bool() const {
      return buf_.get() != nullptr;
    }

    static constexpr size_t kHeadSize = sizeof(uint64_t);

  private:
    std::unique_ptr<rdd::IOBuf> buf_;
  };

public:
  explicit MemoryFlatDict(size_t maxSize)
    : map_(maxSize) {}

  ~MemoryFlatDict() {}

  Block get(Key key) {
    std::unique_ptr<rdd::IOBuf> buf;
    auto it = map_.find(key);
    if (it != map_.cend()) {
      buf = std::move(it->second.clone());
    }
    return Block(std::move(buf));
  }

  void erase(Key key) {
    std::unique_ptr<rdd::IOBuf> buf;
    auto it = map_.find(key);
    if (it != map_.cend()) {
      it->second.reset(std::move(buf));
    }
  }

  void update(Key key, rdd::ByteRange range,
              uint64_t ts = rdd::timestampNow()) {
    std::unique_ptr<rdd::IOBuf> buf(
        rdd::IOBuf::createCombined(Block::kHeadSize + range.size()));
    rdd::io::Appender appender(buf.get(), 0);
    appender.write(ts);
    appender.push(range);
    update(key, std::move(buf));
  }

  size_t size() {
    size_t n = 0;
    auto it = map_.cbegin();
    while (it != map_.cend()) {
      ++n;
      ++it;
    }
    return n;
  }

private:
  void update(Key key, std::unique_ptr<rdd::IOBuf>&& buf) {
    auto p = map_.emplace(key, std::move(buf));
    if (!p.second) {
      p.first->second.reset(std::move(buf));
    }
  }

  rdd::AtomicUnorderedInsertMap<
    Key,
    detail::MutableLockedIOBuf,
    Hash,
    KeyEqual,
    (boost::has_trivial_destructor<Key>::value &&
     boost::has_trivial_destructor<detail::MutableLockedIOBuf>::value),
    IndexType,
    Allocator> map_;
};

} // namespace glutton
