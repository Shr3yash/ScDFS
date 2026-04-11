#include "storage/storage_node.hpp"
#include "common/logger.hpp"
#include "common/serialization.hpp"

namespace scdfs {

StorageNode::StorageNode(const NodeId& node_id, const std::string& address,
                         uint16_t port, const Config& config)
    : node_id_(node_id), address_(address), port_(port), config_(config) {

    std::string data_dir = config_.data_directory + "/" + node_id;
    chunk_store_ = std::make_unique<ChunkStore>(data_dir);
    server_ = std::make_unique<TcpServer>(address_, port_, config_.thread_pool_size);
}

StorageNode::~StorageNode() {
    stop();
}

void StorageNode::start() {
    if (running_) return;
    running_ = true;

    server_->set_handler([this](const WireMessage& msg, int fd) {
        return handle_message(msg, fd);
    });
    server_->start();
    LOG_INFO("StorageNode ", node_id_, " started on ", address_, ":", port_);
}

void StorageNode::stop() {
    if (!running_) return;
    running_ = false;
    server_->stop();
    LOG_INFO("StorageNode ", node_id_, " stopped");
}

WireMessage StorageNode::handle_message(const WireMessage& msg, int /*client_fd*/) {
    switch (msg.type) {
        case MessageType::STORE_CHUNK:     return handle_store_chunk(msg);
        case MessageType::FETCH_CHUNK:     return handle_fetch_chunk(msg);
        case MessageType::DELETE_CHUNK:    return handle_delete_chunk(msg);
        case MessageType::HEARTBEAT:       return handle_heartbeat(msg);
        case MessageType::REPLICATE_CHUNK: return handle_replicate_chunk(msg);
        default: {
            WireMessage resp;
            resp.type = MessageType::ERROR_RESP;
            resp.request_id = msg.request_id;
            return resp;
        }
    }
}

WireMessage StorageNode::handle_store_chunk(const WireMessage& msg) {
    // Payload: [chunk_id_len:4][chunk_id][num_replicas:4][replica_ids...][chunk_data]
    ByteBuffer buf(msg.payload);

    std::string chunk_id = buf.read_string();
    uint32_t num_replicas = buf.read_u32();

    std::vector<NodeId> remaining_replicas;
    for (uint32_t i = 0; i < num_replicas; i++) {
        remaining_replicas.push_back(buf.read_string());
    }

    std::vector<uint8_t> chunk_data = buf.read_bytes_vec();

    bool stored = chunk_store_->store_chunk(chunk_id, chunk_data);

    // Pipeline replication: forward to remaining replicas
    if (stored && !remaining_replicas.empty()) {
        forward_to_replica(chunk_id, chunk_data, remaining_replicas);
    }

    WireMessage resp;
    resp.type = MessageType::STORE_CHUNK_ACK;
    resp.request_id = msg.request_id;

    ByteBuffer resp_buf;
    resp_buf.write_u8(stored ? 1 : 0);
    resp_buf.write_string(node_id_);
    resp.payload = std::move(resp_buf.data());

    return resp;
}

WireMessage StorageNode::handle_fetch_chunk(const WireMessage& msg) {
    ByteBuffer buf(msg.payload);
    std::string chunk_id = buf.read_string();

    std::vector<uint8_t> data;
    size_t size = chunk_store_->read_chunk(chunk_id, data);

    WireMessage resp;
    resp.type = MessageType::FETCH_CHUNK_RESP;
    resp.request_id = msg.request_id;

    ByteBuffer resp_buf;
    resp_buf.write_u8(size > 0 ? 1 : 0);
    resp_buf.write_bytes(data.data(), data.size());
    resp.payload = std::move(resp_buf.data());

    return resp;
}

WireMessage StorageNode::handle_delete_chunk(const WireMessage& msg) {
    ByteBuffer buf(msg.payload);
    std::string chunk_id = buf.read_string();

    bool deleted = chunk_store_->delete_chunk(chunk_id);

    WireMessage resp;
    resp.type = MessageType::DELETE_CHUNK_ACK;
    resp.request_id = msg.request_id;

    ByteBuffer resp_buf;
    resp_buf.write_u8(deleted ? 1 : 0);
    resp.payload = std::move(resp_buf.data());

    return resp;
}

WireMessage StorageNode::handle_heartbeat(const WireMessage& msg) {
    WireMessage resp;
    resp.type = MessageType::HEARTBEAT_ACK;
    resp.request_id = msg.request_id;

    ByteBuffer resp_buf;
    resp_buf.write_string(node_id_);
    resp_buf.write_u64(chunk_store_->total_stored_bytes());
    resp_buf.write_u64(chunk_store_->chunk_count());
    resp.payload = std::move(resp_buf.data());

    return resp;
}

WireMessage StorageNode::handle_replicate_chunk(const WireMessage& msg) {
    // Same format as STORE_CHUNK but used for re-replication during recovery
    return handle_store_chunk(msg);
}

bool StorageNode::forward_to_replica(const ChunkId& chunk_id, const std::vector<uint8_t>& data,
                                     const std::vector<NodeId>& remaining_replicas) {
    if (remaining_replicas.empty()) return true;

    // Next replica is first in the list; pass the rest downstream
    // We parse "node_id" as "address:port" for simplicity in this implementation
    const NodeId& next = remaining_replicas[0];
    auto colon = next.find(':');
    if (colon == std::string::npos) {
        LOG_ERROR("Invalid replica node format: ", next);
        return false;
    }

    std::string host = next.substr(0, colon);
    uint16_t port = static_cast<uint16_t>(std::stoi(next.substr(colon + 1)));

    TcpClient client;
    if (!client.connect(host, port)) {
        LOG_WARN("Failed to connect to replica ", next, " for chunk ", chunk_id);
        return false;
    }

    // Build payload for downstream: chunk_id + remaining replicas (minus current) + data
    ByteBuffer payload;
    payload.write_string(chunk_id);

    std::vector<NodeId> downstream(remaining_replicas.begin() + 1, remaining_replicas.end());
    payload.write_u32(static_cast<uint32_t>(downstream.size()));
    for (const auto& r : downstream) {
        payload.write_string(r);
    }
    payload.write_bytes(data.data(), data.size());

    WireMessage fwd_msg;
    fwd_msg.type = MessageType::REPLICATE_CHUNK;
    fwd_msg.request_id = 0;
    fwd_msg.payload = std::move(payload.data());

    WireMessage resp;
    bool ok = client.send_receive(fwd_msg, resp);
    client.disconnect();

    if (!ok) {
        LOG_WARN("Pipeline replication failed for chunk ", chunk_id, " -> ", next);
    }
    return ok;
}

} // namespace scdfs
