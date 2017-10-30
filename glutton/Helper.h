/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <raster/util/Range.h>
#include "util/slice.h"
#include "flatbuffers/flatbuffers.h"

namespace glutton {

template <class T>
bool verifyFlatbuffer(T* object, const rdd::ByteRange& range) {
  flatbuffers::Verifier verifier(range.data(), range.size());
  return object->Verify(verifier);
}

inline leveldb::Slice slice(const rdd::StringPiece& range) {
  return leveldb::Slice(range.data(), range.size());
}

} // namespace glutton
