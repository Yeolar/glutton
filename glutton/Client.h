/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <raster/net/NetUtil.h>
#include <raster/protocol/binary/AsyncClient.h>
#include <raster/protocol/binary/SyncClient.h>
#include "Helper.h"
#include "glutton_generated.h"

namespace glutton {

template <class C>
class Client {
public:
  Client(rdd::ClientOption opt)
    : client_(opt) {
  }

  bool connect() {
    return client_.connect();
  }

  bool get(rdd::StringPiece key, std::string& value) {
    ::flatbuffers::FlatBufferBuilder fbb;
    fbb.Finish(
        CreateQuery(fbb,
                    fbb.CreateString("rddg"),
                    fbs::Action_GET,
                    fbb.CreateString(key.data(), key.size()),
                    fbb.CreateString("")));
    return fetch(fbb, [&](const fbs::Result* res) {
      value.assign(res->value()->data(), res->value()->size());
    });
  }

  bool put(rdd::StringPiece key, rdd::StringPiece value) {
    ::flatbuffers::FlatBufferBuilder fbb;
    fbb.Finish(
        CreateQuery(fbb,
                    fbb.CreateString("rddg"),
                    fbs::Action_PUT,
                    fbb.CreateString(key.data(), key.size()),
                    fbb.CreateString(value.data(), value.size())));
    return fetch(fbb, nullptr);
  }

  bool erase(rdd::StringPiece key) {
    ::flatbuffers::FlatBufferBuilder fbb;
    fbb.Finish(
        CreateQuery(fbb,
                    fbb.CreateString("rddg"),
                    fbs::Action_DELETE,
                    fbb.CreateString(key.data(), key.size()),
                    fbb.CreateString("")));
    return fetch(fbb, nullptr);
  }

private:
  bool fetch(const ::flatbuffers::FlatBufferBuilder& fbb,
             std::function<void(const fbs::Result*)>&& handle) {
    try {
      rdd::ByteRange req(fbb.GetBufferPointer(), fbb.GetSize());
      rdd::ByteRange data;
      client_.fetch(data, req);
      auto res = ::flatbuffers::GetRoot<fbs::Result>(data.data());
      DCHECK(verifyFlatbuffer(res, data));
      if (res->code() != 0) {
        return false;
      }
      if (handle) {
        handle(res);
      }
    }
    catch (...) {
      return false;
    }
    return true;
  }

  C client_;
};

typedef Client<rdd::BinaryAsyncClient> AsyncClient;
typedef Client<rdd::BinarySyncClient> SyncClient;

} // namespace glutton
