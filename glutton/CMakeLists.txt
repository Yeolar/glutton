# Copyright (C) 2017, Yeolar

add_executable(glutton
    CacheManager.cpp
    Config.cpp
    Server.cpp
)
target_link_libraries(glutton
    raster
    glutton_leveldb
    boost_filesystem
    boost_system
)

add_executable(glutton-bench
    Bench.cpp
)
target_link_libraries(glutton-bench
    raster
    glutton_leveldb
    boost_filesystem
    boost_system
)
