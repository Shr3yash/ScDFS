#pragma once

#include <string>
#include <memory>
#include <atomic>
#include "common/types.hpp"
#include "common/config.hpp"
#include "storage/chunk_store.hpp"
#include "network/tcp_server.hpp"
#include "network/tcp_client.hpp"

namespace scdfs {

// A storage node that stores chunk data and serves requests over TCP.
// Handles STORE_CHUNK, FETCH_CHUNK, DELETE_CHUNK, HEARTBEAT, REPLICATE_CHUNK.
class StorageNode {
public:
    StorageNode(const NodeId& node_id, const std::string& address, uint16_t port,
                const Config& config);
    ~StorageNode();

    void start();
    void stop();

    const NodeId& id() const { return node_id_; }
    const std::string& address() const { return address_; }
    uint16_t port() const { return port_; }

    ChunkStore& store() { return *chunk_store_; }

private:
    NodeId node_id_;
    std::string address_;
    uint16_t port_;
    Config config_;

    std::unique_ptr<ChunkStore> chunk_store_;
    std::unique_ptr<TcpServer> server_;
    std::atomic<bool> running_{false};

    WireMessage handle_message(const WireMessage& msg, int client_fd);

    WireMessage handle_store_chunk(const WireMessage& msg);
    WireMessage handle_fetch_chunk(const WireMessage& msg);
    WireMessage handle_delete_chunk(const WireMessage& msg);
    WireMessage handle_heartbeat(const WireMessage& msg);
    WireMessage handle_replicate_chunk(const WireMessage& msg);

    // Pipeline-style: forward chunk data to the next replica in chain.
    bool forward_to_replica(const ChunkId& chunk_id, const std::vector<uint8_t>& data,
                            const std::vector<NodeId>& remaining_replicas);
};

} // namespace scdfs
