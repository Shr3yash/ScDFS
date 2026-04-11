#pragma once

#include <string>
#include <cstdint>
#include "common/types.hpp"

namespace scdfs {

struct Config {
    size_t chunk_size = DEFAULT_CHUNK_SIZE;
    int replication_factor = DEFAULT_REPLICATION_FACTOR;
    int write_quorum = DEFAULT_WRITE_QUORUM;
    int read_quorum = DEFAULT_READ_QUORUM;
    int virtual_nodes = DEFAULT_VIRTUAL_NODES;
    int thread_pool_size = DEFAULT_THREAD_POOL_SIZE;
    int heartbeat_interval_ms = DEFAULT_HEARTBEAT_INTERVAL_MS;
    int heartbeat_timeout_ms = DEFAULT_HEARTBEAT_TIMEOUT_MS;

    std::string coordinator_address = "127.0.0.1";
    uint16_t coordinator_port = DEFAULT_COORDINATOR_PORT;

    std::string cassandra_contact_points = "127.0.0.1";
    uint16_t cassandra_port = 9042;
    std::string cassandra_keyspace = "scdfs";

    std::string data_directory = "./data";

    bool use_cassandra = false; // false = in-memory metadata store
};

} // namespace scdfs
