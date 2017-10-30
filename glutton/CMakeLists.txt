# Copyright (C) 2017, Yeolar

add_executable(glutton
    Config.cpp
    Server.cpp
)
target_link_libraries(glutton
    raster
    glutton_leveldb
    boost_filesystem
    boost_system
)
