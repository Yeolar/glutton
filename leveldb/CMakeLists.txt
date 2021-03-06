# Copyright (C) 2017, Yeolar

set(GLUTTON_LEVELDB_SRCS
    LevelDBStorage.cpp
    db/builder.cc
    db/dbformat.cc
    db/db_impl.cc
    db/db_iter.cc
    db/dumpfile.cc
    db/filename.cc
    db/log_reader.cc
    db/log_writer.cc
    db/memtable.cc
    db/repair.cc
    db/table_cache.cc
    db/version_edit.cc
    db/version_set.cc
    db/write_batch.cc
    table/block_builder.cc
    table/block.cc
    table/filter_block.cc
    table/format.cc
    table/iterator.cc
    table/merger.cc
    table/table_builder.cc
    table/table.cc
    table/two_level_iterator.cc
    util/arena.cc
    util/bloom.cc
    util/cache.cc
    util/coding.cc
    util/comparator.cc
    util/env.cc
    util/env_posix.cc
    util/hash.cc
    util/histogram.cc
    util/logging.cc
    util/options.cc
    util/status.cc
    util/testutil.cc
)
add_library(glutton_leveldb STATIC ${GLUTTON_LEVELDB_SRCS})

add_executable(leveldb-util
    db/leveldbutil.cc
)
target_link_libraries(leveldb-util
    raster
    glutton_leveldb
)

add_executable(leveldb-bench
    db/db_bench.cc
)
target_link_libraries(leveldb-bench
    raster
    glutton_leveldb
)
