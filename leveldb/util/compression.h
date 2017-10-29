// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstddef>
#include <string>
#ifdef HAVE_SNAPPY
#include <snappy.h>
#endif  // defined(HAVE_SNAPPY)

namespace leveldb {

inline bool Snappy_Compress(const char* input, size_t length,
                            ::std::string* output) {
#ifdef HAVE_SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#endif  // defined(HAVE_SNAPPY)

  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#ifdef HAVE_SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  return false;
#endif  // defined(HAVE_SNAPPY)
}

inline bool Snappy_Uncompress(const char* input, size_t length,
                              char* output) {
#ifdef HAVE_SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  return false;
#endif  // defined(HAVE_SNAPPY)
}

}  // namespace leveldb
