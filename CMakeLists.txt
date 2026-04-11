cmake_minimum_required(VERSION 3.16)
project(ScDFS VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Platform-specific flags
if(APPLE)
    # macOS doesn't support MSG_NOSIGNAL; we use SO_NOSIGPIPE instead
    add_compile_definitions(MSG_NOSIGNAL=0)
endif()

# Options
option(SCDFS_USE_CASSANDRA "Build with Cassandra support (requires cpp-driver)" OFF)
option(SCDFS_BUILD_TESTS "Build tests" ON)
option(SCDFS_BUILD_BENCHMARKS "Build benchmarks" ON)

# Core library sources
set(SCDFS_SOURCES
    src/hashing/murmur3.cpp
    src/hashing/consistent_hash_ring.cpp
    src/threading/thread_pool.cpp
    src/network/tcp_server.cpp
    src/network/tcp_client.cpp
    src/storage/chunk_store.cpp
    src/storage/storage_node.cpp
    src/metadata/memory_metadata_store.cpp
    src/metadata/cassandra_metadata_store.cpp
    src/replication/replication_manager.cpp
    src/replication/recovery_worker.cpp
    src/coordinator/coordinator.cpp
    src/client/scdfs_client.cpp
)

add_library(scdfs_core STATIC ${SCDFS_SOURCES})
target_include_directories(scdfs_core PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_compile_options(scdfs_core PRIVATE -Wall -Wextra -O2)
target_link_libraries(scdfs_core PUBLIC pthread)

if(SCDFS_USE_CASSANDRA)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(CASSANDRA REQUIRED cassandra)
    target_include_directories(scdfs_core PRIVATE ${CASSANDRA_INCLUDE_DIRS})
    target_link_libraries(scdfs_core PUBLIC ${CASSANDRA_LIBRARIES})
    target_compile_definitions(scdfs_core PUBLIC SCDFS_USE_CASSANDRA)
endif()

# Storage node executable
add_executable(scdfs_storage src/storage_main.cpp)
target_link_libraries(scdfs_storage PRIVATE scdfs_core)

# Coordinator / interactive shell
add_executable(scdfs_coordinator src/coordinator_main.cpp)
target_link_libraries(scdfs_coordinator PRIVATE scdfs_core)

# Tests
if(SCDFS_BUILD_TESTS)
    enable_testing()

    add_executable(test_murmur3 tests/test_murmur3.cpp)
    target_link_libraries(test_murmur3 PRIVATE scdfs_core)
    add_test(NAME Murmur3 COMMAND test_murmur3)

    add_executable(test_consistent_hash tests/test_consistent_hash.cpp)
    target_link_libraries(test_consistent_hash PRIVATE scdfs_core)
    add_test(NAME ConsistentHash COMMAND test_consistent_hash)

    add_executable(test_thread_pool tests/test_thread_pool.cpp)
    target_link_libraries(test_thread_pool PRIVATE scdfs_core)
    add_test(NAME ThreadPool COMMAND test_thread_pool)

    add_executable(test_chunk_store tests/test_chunk_store.cpp)
    target_link_libraries(test_chunk_store PRIVATE scdfs_core)
    add_test(NAME ChunkStore COMMAND test_chunk_store)

    add_executable(test_integration tests/test_integration.cpp)
    target_link_libraries(test_integration PRIVATE scdfs_core)
    add_test(NAME Integration COMMAND test_integration)
endif()

# Benchmarks
if(SCDFS_BUILD_BENCHMARKS)
    add_executable(scdfs_benchmark benchmarks/throughput_benchmark.cpp)
    target_link_libraries(scdfs_benchmark PRIVATE scdfs_core)
endif()
