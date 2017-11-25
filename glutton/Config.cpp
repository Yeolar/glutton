/*
 * Copyright (C) 2017, Yeolar
 */

#include "glutton/Config.h"
#include <raster/io/FSUtil.h>
#include <raster/util/Logging.h>
#include "glutton/CacheManager.h"

namespace glutton {

void configGlutton(const rdd::dynamic& j, bool reload) {
  if (reload) return;
  if (!j.isObject()) {
    RDDLOG(FATAL) << "config glutton error: " << j;
    return;
  }
  RDDLOG(INFO) << "config glutton";
  auto root = rdd::json::get(j, "root", "data/glutton/");
  auto size = rdd::json::get(j, "size", 1000000);
  auto cache = rdd::Singleton<CacheManager>::get();
  cache->initialize(size, root);
}

}
