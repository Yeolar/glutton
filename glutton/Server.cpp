/*
 * Copyright (C) 2017, Yeolar
 */

#include <gflags/gflags.h>
#include <raster/framework/Config.h>
#include <raster/framework/Monitor.h>
#include <raster/framework/Signal.h>
#include <raster/net/Actor.h>
#include <raster/protocol/binary/AsyncServer.h>
#include <raster/util/Logging.h>
#include <raster/util/Uuid.h>
#include "CacheManager.h"
#include "Config.h"
#include "Helper.h"
#include "glutton_generated.h"

static const char* VERSION = "1.0.0";

DEFINE_string(conf, "server.json", "Server config file");

using namespace rdd;

namespace glutton {

class Glutton : public BinaryProcessor<> {
public:
  Glutton() {
    RDDLOG(DEBUG) << "Glutton init";
  }

  bool process(ByteRange& response, const ByteRange& request) {
    uint64_t t0 = rdd::timestampNow();

    auto query = ::flatbuffers::GetRoot<fbs::Query>(request.data());
    DCHECK(verifyFlatbuffer(query, request));

    auto traceid = query->traceid()->str();
    auto key = query->key()->str();
    auto value = query->value()->str();

    if (!StringPiece(traceid).startsWith("rdd")) {
      RDDLOG(INFO) << "untrusted request: [" << query->traceid() << "]";
      ::flatbuffers::FlatBufferBuilder fbb;
      fbb.Finish(CreateResult(fbb, 0, fbs::ResultCode_E_SOURCE__UNTRUSTED));
      response.reset(fbb.GetBufferPointer(), fbb.GetSize());
      return true;
    }

    traceid = generateUuid(traceid, "rddg");

    auto cache = Singleton<CacheManager>::get();
    fbs::ResultCode code = fbs::ResultCode_OK;

    switch (query->action()) {
      case fbs::Action_GET: {
          if (!cache->get(key, value)) {
            code = fbs::ResultCode_E_VALUE__NOTFOUND;
          }
        }
        break;
      case fbs::Action_PUT: {
          if (!cache->put(key, range(value))) {
            code = fbs::ResultCode_E_ACTION__FAILED;
          }
        }
        break;
      case fbs::Action_DELETE: {
          if (!cache->erase(key)) {
            code = fbs::ResultCode_E_ACTION__FAILED;
          }
        }
        break;
      case fbs::Action_NONE:
        break;
    }

    RDDTLOG(INFO, traceid) << "key: \"" << key << "\""
      << " code=" << code;
    ::flatbuffers::FlatBufferBuilder fbb;
    fbb.Finish(CreateResult(fbb,
                            fbb.CreateString(traceid),
                            fbb.CreateString(value),
                            code));
    response.reset(fbb.GetBufferPointer(), fbb.GetSize());

    RDDMON_CNT(rdd::to<std::string>(actionName(query)));
    RDDMON_CNT(rdd::to<std::string>(actionName(query), ".", code));
    RDDMON_AVG(rdd::to<std::string>(actionName(query), ".cost"),
               timestampNow() - t0);
    return true;
  }

private:
  const char* actionName(const fbs::Query* query) const {
    switch (query->action()) {
      case fbs::Action_GET: return "GET";
      case fbs::Action_PUT: return "PUT";
      case fbs::Action_DELETE: return "DELETE";
      case fbs::Action_NONE: return "NONE";
    }
    return "";
  }
};

}

using namespace glutton;

int main(int argc, char* argv[]) {
  google::SetVersionString(VERSION);
  google::SetUsageMessage("Usage : ./glutton");
  google::ParseCommandLineFlags(&argc, &argv, true);

  setupIgnoreSignal(SIGPIPE);
  setupShutdownSignal(SIGINT);
  setupShutdownSignal(SIGTERM);

  std::shared_ptr<Service> glutton(
      new BinaryAsyncServer<Glutton>());
  Singleton<Actor>::get()->addService("Glutton", glutton);

  config(FLAGS_conf.c_str(), {
         {configLogging, "logging"},
         {configActor, "actor"},
         {configService, "service"},
         {configThreadPool, "thread"},
         {configNetCopy, "net.copy"},
         {configMonitor, "monitor"},
         {configGlutton, "glutton"}
         });

  RDDLOG(INFO) << "rdd start ... ^_^";
  Singleton<Actor>::get()->start();

  google::ShutDownCommandLineFlags();

  return 0;
}
