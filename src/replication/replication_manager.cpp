#include "replication/replication_manager.hpp"
#include "network/tcp_client.hpp"
#include "common/serialization.hpp"
#include "common/logger.hpp"

namespace scdfs {

ReplicationManager::ReplicationManager(std::shared_ptr<ConsistentHashRing> ring,
                                       std::shared_ptr<MetadataStore> metadata,
                                       const Config& config)
    : ring_(std::move(ring)), metadata_(std::move(metadata)), config_(config) {}

std::vector<NodeId> ReplicationManager::get_replica_nodes(const ChunkId& chunk_id) const {
    return ring_->get_nodes(chunk_id, config_.replication_factor);
}

std::vector<NodeId> ReplicationManager::replicate_chunk(const ChunkId& chunk_id,
                                                         const uint8_t* data, size_t size) {
    auto target_nodes = get_replica_nodes(chunk_id);

    if (target_nodes.empty()) {
        LOG_ERROR("No target nodes for chunk ", chunk_id);
        return {};
    }

    // One RPC to the primary. It is supposed to forward `downstream`.
    // A true return means the primary stored the chunk, not that every hop did.
    std::vector<NodeId> downstream(target_nodes.begin() + 1, target_nodes.end());

    bool ok = send_chunk_to_node(chunk_id, data, size, target_nodes[0], downstream);

    if (ok) {
        LOG_DEBUG("Replicated chunk ", chunk_id, " to ", target_nodes.size(), " nodes");
        return target_nodes;
    }

    // Primary RPC failed. Write each replica from here and count real ACKs.
    LOG_WARN("Pipeline replication failed, falling back to direct writes for ", chunk_id);
    std::vector<NodeId> acked;
    for (const auto& node : target_nodes) {
        if (send_chunk_to_node(chunk_id, data, size, node, {})) {
            acked.push_back(node);
        }
    }
    return acked;
}

bool ReplicationManager::re_replicate_chunk(const ChunkId& chunk_id,
                                             const NodeId& source_node,
                                             const NodeId& target_node) {
    std::vector<uint8_t> data;
    if (!fetch_from_node(chunk_id, source_node, data)) {
        LOG_ERROR("Failed to fetch chunk ", chunk_id, " from ", source_node);
        return false;
    }

    if (!send_chunk_to_node(chunk_id, data.data(), data.size(), target_node, {})) {
        LOG_ERROR("Failed to re-replicate chunk ", chunk_id, " to ", target_node);
        return false;
    }

    LOG_INFO("Re-replicated chunk ", chunk_id, " from ", source_node, " to ", target_node);
    return true;
}

bool ReplicationManager::delete_from_node(const ChunkId& chunk_id, const NodeId& node_id) {
    auto [host, port] = resolve_node(node_id);

    TcpClient client;
    if (!client.connect(host, port)) return false;

    ByteBuffer payload;
    payload.write_string(chunk_id);

    WireMessage req;
    req.type = MessageType::DELETE_CHUNK;
    req.request_id = 0;
    req.payload = std::move(payload.data());

    WireMessage resp;
    bool ok = client.send_receive(req, resp);
    client.disconnect();
    return ok;
}

bool ReplicationManager::fetch_chunk(const ChunkId& chunk_id,
                                      const std::vector<NodeId>& replicas,
                                      std::vector<uint8_t>& out) {
    for (const auto& node : replicas) {
        if (fetch_from_node(chunk_id, node, out)) {
            return true;
        }
        LOG_WARN("Failed to fetch chunk ", chunk_id, " from ", node, ", trying next replica");
    }
    LOG_ERROR("All replicas failed for chunk ", chunk_id);
    return false;
}

bool ReplicationManager::send_chunk_to_node(const ChunkId& chunk_id,
                                             const uint8_t* data, size_t size,
                                             const NodeId& node_id,
                                             const std::vector<NodeId>& downstream) {
    auto [host, port] = resolve_node(node_id);

    TcpClient client;
    if (!client.connect(host, port)) {
        LOG_ERROR("Cannot connect to storage node ", node_id);
        return false;
    }

    // Storage nodes forward with host:port strings, not node ids.
    ByteBuffer payload;
    payload.write_string(chunk_id);
    payload.write_u32(static_cast<uint32_t>(downstream.size()));
    for (const auto& r : downstream) {
        auto [rhost, rport] = resolve_node(r);
        payload.write_string(rhost + ":" + std::to_string(rport));
    }
    payload.write_bytes(data, size);

    WireMessage req;
    req.type = MessageType::STORE_CHUNK;
    req.request_id = 0;
    req.payload = std::move(payload.data());

    WireMessage resp;
    bool ok = client.send_receive(req, resp);
    client.disconnect();

    if (ok && resp.type == MessageType::STORE_CHUNK_ACK) {
        ByteBuffer resp_buf(resp.payload);
        uint8_t success = resp_buf.read_u8();
        return success == 1;
    }
    return false;
}

bool ReplicationManager::fetch_from_node(const ChunkId& chunk_id,
                                          const NodeId& node_id,
                                          std::vector<uint8_t>& out) {
    auto [host, port] = resolve_node(node_id);

    TcpClient client;
    if (!client.connect(host, port)) return false;

    ByteBuffer payload;
    payload.write_string(chunk_id);

    WireMessage req;
    req.type = MessageType::FETCH_CHUNK;
    req.request_id = 0;
    req.payload = std::move(payload.data());

    WireMessage resp;
    bool ok = client.send_receive(req, resp);
    client.disconnect();

    if (ok && resp.type == MessageType::FETCH_CHUNK_RESP) {
        ByteBuffer resp_buf(resp.payload);
        uint8_t success = resp_buf.read_u8();
        if (success) {
            out = resp_buf.read_bytes_vec();
            return true;
        }
    }
    return false;
}

std::pair<std::string, uint16_t> ReplicationManager::resolve_node(const NodeId& node_id) const {
    auto info = metadata_->get_node(node_id);
    if (info) {
        return {info->address, info->port};
    }

    // Forwarded replica lists are already "host:port".
    auto colon = node_id.find(':');
    if (colon != std::string::npos) {
        return {node_id.substr(0, colon),
                static_cast<uint16_t>(std::stoi(node_id.substr(colon + 1)))};
    }

    return {"127.0.0.1", DEFAULT_STORAGE_PORT_BASE};
}

} // namespace scdfs
