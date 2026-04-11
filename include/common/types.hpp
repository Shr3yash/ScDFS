#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <chrono>
#include <functional>

namespace scdfs {

using ChunkId = std::string;
using NodeId = std::string;
using FilePath = std::string;
using Checksum = std::string;
using Version = uint64_t;
using Timestamp = std::chrono::system_clock::time_point;

constexpr size_t DEFAULT_CHUNK_SIZE = 64 * 1024 * 1024; // 64 MB
constexpr int DEFAULT_REPLICATION_FACTOR = 3;
constexpr int DEFAULT_WRITE_QUORUM = 2;
constexpr int DEFAULT_READ_QUORUM = 1;
constexpr int DEFAULT_VIRTUAL_NODES = 128;
constexpr int DEFAULT_THREAD_POOL_SIZE = 8;
constexpr int DEFAULT_HEARTBEAT_INTERVAL_MS = 2000;
constexpr int DEFAULT_HEARTBEAT_TIMEOUT_MS = 6000;
constexpr uint16_t DEFAULT_COORDINATOR_PORT = 9100;
constexpr uint16_t DEFAULT_STORAGE_PORT_BASE = 9200;

enum class ChunkStatus : uint8_t {
    PENDING = 0,
    COMMITTED = 1,
    DELETED = 2
};

enum class NodeStatus : uint8_t {
    ACTIVE = 0,
    FAILED = 1,
    DECOMMISSIONING = 2
};

enum class MessageType : uint8_t {
    STORE_CHUNK       = 1,
    FETCH_CHUNK       = 2,
    DELETE_CHUNK      = 3,
    HEARTBEAT         = 4,
    REPLICATE_CHUNK   = 5,
    STORE_CHUNK_ACK   = 6,
    FETCH_CHUNK_RESP  = 7,
    DELETE_CHUNK_ACK  = 8,
    HEARTBEAT_ACK     = 9,
    REPLICATE_ACK     = 10,
    ERROR_RESP        = 255
};

inline const char* to_string(ChunkStatus s) {
    switch (s) {
        case ChunkStatus::PENDING:   return "PENDING";
        case ChunkStatus::COMMITTED: return "COMMITTED";
        case ChunkStatus::DELETED:   return "DELETED";
        default:                     return "UNKNOWN";
    }
}

inline const char* to_string(NodeStatus s) {
    switch (s) {
        case NodeStatus::ACTIVE:            return "ACTIVE";
        case NodeStatus::FAILED:            return "FAILED";
        case NodeStatus::DECOMMISSIONING:   return "DECOMMISSIONING";
        default:                            return "UNKNOWN";
    }
}

struct ChunkMetadata {
    ChunkId chunk_id;
    FilePath file_path;
    int chunk_index;
    size_t chunk_size;
    Checksum checksum;
    Version version;
    ChunkStatus status;
    std::vector<NodeId> replica_nodes;
};

struct FileMetadata {
    FilePath file_path;
    size_t file_size;
    int chunk_count;
    Version version;
    Timestamp created_at;
    Timestamp updated_at;
};

struct NodeInfo {
    NodeId node_id;
    std::string address;
    uint16_t port;
    NodeStatus status;
    Timestamp last_heartbeat;
    size_t capacity_bytes;
    size_t used_bytes;
};

struct WireMessage {
    MessageType type;
    uint32_t request_id;
    std::vector<uint8_t> payload;
};

} // namespace scdfs
