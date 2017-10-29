# Copyright (C) 2017, Yeolar

# Setup compiler
#set(CMAKE_C_COMPILER gcc)
#set(CMAKE_CXX_COMPILER g++)

# Define project
project(glutton)

cmake_minimum_required(VERSION 2.8.8)

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake-scripts")

# Set project version
set(VERSION_MAJOR 1)
set(VERSION_MINOR 0)
set(VERSION_PATCH 0)

# Required packages
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
find_package(Threads REQUIRED)
find_package(GFlags REQUIRED)
find_package(Raster REQUIRED)

# Optional packages
find_package(GTest)

# Setup environment
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_VERBOSE_MAKEFILE OFF)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -std=c++11 -fPIC")

# Link libraries
link_libraries(
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARIES}
    ${RASTER_LIBRARIES}
)

include_directories(
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/leveldb
)

# Work on all sub directories
add_subdirectory(leveldb)
add_subdirectory(glutton)

# Test
if(GTEST_FOUND)
    enable_testing()
endif()

