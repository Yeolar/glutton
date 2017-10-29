/*
 * Copyright (C) 2017, Yeolar
 */

#include <gflags/gflags.h>
#include <raster/framework/Config.h>
#include <raster/framework/Monitor.h>
#include <raster/net/Actor.h>
#include <raster/protocol/binary/AsyncServer.h>
#include <raster/util/Logging.h>
#include <raster/util/ScopeGuard.h>
#include <raster/util/Signal.h>
#include <raster/util/Uuid.h>
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
    auto query = ::flatbuffers::GetRoot<fbs::Query>(request.data());
    DCHECK(verifyFlatbuffer(query, request));

    if (!StringPiece(query->traceid()->str()).startsWith("rdd")) {
      RDDLOG(INFO) << "untrusted request: [" << query->traceid() << "]";
      ::flatbuffers::FlatBufferBuilder fbb;
      fbb.Finish(CreateResult(fbb, 0, fbs::ResultCode_E_SOURCE__UNTRUSTED));
      response.reset(fbb.GetBufferPointer(), fbb.GetSize());
      return true;
    }

    auto traceid = generateUuid(query->traceid()->str(), "rddg");
    std::string value;
    fbs::ResultCode code = fbs::ResultCode_OK;

    RDDTLOG(INFO, traceid) << "key: \"" << query->key()->str() << "\""
      << " code=" << code;
    ::flatbuffers::FlatBufferBuilder fbb;
    fbb.Finish(CreateResult(fbb,
                            fbb.CreateString(traceid),
                            fbb.CreateString(value),
                            code));
    response.reset(fbb.GetBufferPointer(), fbb.GetSize());
    return true;
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
         //{configGlutton, "glutton"}
         });

  RDDLOG(INFO) << "rdd start ... ^_^";
  Singleton<Actor>::get()->start();

  google::ShutDownCommandLineFlags();

  return 0;
}
