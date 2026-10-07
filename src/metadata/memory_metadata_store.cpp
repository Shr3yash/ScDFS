#include "metadata/memory_metadata_store.hpp"
#include "common/logger.hpp"
#include <algorithm>

namespace scdfs {

bool MemoryMetadataStore::put_file(const FileMetadata& file) {
    std::unique_lock lock(file_mutex_);
    files_[file.file_path] = file;
    return true;
}

std::optional<FileMetadata> MemoryMetadataStore::get_file(const FilePath& path) {
    std::shared_lock lock(file_mutex_);
    auto it = files_.find(path);
    if (it == files_.end()) return std::nullopt;
    return it->second;
}

bool MemoryMetadataStore::delete_file(const FilePath& path) {
    std::unique_lock lock(file_mutex_);
    return files_.erase(path) > 0;
}

std::vector<FileMetadata> MemoryMetadataStore::list_files(const std::string& prefix) {
    std::shared_lock lock(file_mutex_);
    std::vector<FileMetadata> result;
    for (const auto& [path, meta] : files_) {
        if (prefix.empty() || path.substr(0, prefix.size()) == prefix) {
            result.push_back(meta);
        }
    }
    return result;
}

bool MemoryMetadataStore::put_chunk(const ChunkMetadata& chunk) {
    std::unique_lock lock(chunk_mutex_);
    chunks_[chunk_key(chunk.file_path, chunk.chunk_index)] = chunk;
    return true;
}

std::optional<ChunkMetadata> MemoryMetadataStore::get_chunk(const FilePath& file_path, int chunk_index) {
    std::shared_lock lock(chunk_mutex_);
    auto it = chunks_.find(chunk_key(file_path, chunk_index));
    if (it == chunks_.end()) return std::nullopt;
    return it->second;
}

std::vector<ChunkMetadata> MemoryMetadataStore::get_chunks_for_file(const FilePath& file_path) {
    std::shared_lock lock(chunk_mutex_);
    std::vector<ChunkMetadata> result;
    std::string prefix = file_path + ":";
    for (const auto& [key, chunk] : chunks_) {
        if (key.substr(0, prefix.size()) == prefix) {
            result.push_back(chunk);
        }
    }
    std::sort(result.begin(), result.end(),
              [](const ChunkMetadata& a, const ChunkMetadata& b) {
                  return a.chunk_index < b.chunk_index;
              });
    return result;
}

bool MemoryMetadataStore::update_chunk_status(const FilePath& file_path, int chunk_index, ChunkStatus status) {
    std::unique_lock lock(chunk_mutex_);
    auto it = chunks_.find(chunk_key(file_path, chunk_index));
    if (it == chunks_.end()) return false;
    it->second.status = status;
    return true;
}

bool MemoryMetadataStore::update_chunk_replicas(const FilePath& file_path, int chunk_index,
                                                 const std::vector<NodeId>& replicas) {
    std::unique_lock lock(chunk_mutex_);
    auto it = chunks_.find(chunk_key(file_path, chunk_index));
    if (it == chunks_.end()) return false;
    it->second.replica_nodes = replicas;
    return true;
}

std::vector<ChunkMetadata> MemoryMetadataStore::get_chunks_on_node(const NodeId& node_id) {
    std::shared_lock lock(chunk_mutex_);
    std::vector<ChunkMetadata> result;
    for (const auto& [_, chunk] : chunks_) {
        for (const auto& replica : chunk.replica_nodes) {
            if (replica == node_id) {
                result.push_back(chunk);
                break;
            }
        }
    }
    return result;
}

bool MemoryMetadataStore::register_node(const NodeInfo& node) {
    std::unique_lock lock(node_mutex_);
    nodes_[node.node_id] = node;
    LOG_INFO("Registered node ", node.node_id, " at ", node.address, ":", node.port);
    return true;
}

bool MemoryMetadataStore::update_node_status(const NodeId& node_id, NodeStatus status) {
    std::unique_lock lock(node_mutex_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return false;
    it->second.status = status;
    LOG_INFO("Node ", node_id, " status -> ", to_string(status));
    return true;
}

bool MemoryMetadataStore::update_heartbeat(const NodeId& node_id) {
    std::unique_lock lock(node_mutex_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return false;
    it->second.last_heartbeat = std::chrono::system_clock::now();
    return true;
}

std::optional<NodeInfo> MemoryMetadataStore::get_node(const NodeId& node_id) {
    std::shared_lock lock(node_mutex_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return std::nullopt;
    return it->second;
}

std::vector<NodeInfo> MemoryMetadataStore::get_all_nodes() {
    std::shared_lock lock(node_mutex_);
    std::vector<NodeInfo> result;
    for (const auto& [_, node] : nodes_) result.push_back(node);
    return result;
}

std::vector<NodeInfo> MemoryMetadataStore::get_active_nodes() {
    std::shared_lock lock(node_mutex_);
    std::vector<NodeInfo> result;
    for (const auto& [_, node] : nodes_) {
        if (node.status == NodeStatus::ACTIVE) result.push_back(node);
    }
    return result;
}

std::vector<NodeInfo> MemoryMetadataStore::get_failed_nodes() {
    std::shared_lock lock(node_mutex_);
    std::vector<NodeInfo> result;
    for (const auto& [_, node] : nodes_) {
        if (node.status == NodeStatus::FAILED) result.push_back(node);
    }
    return result;
}

} // namespace scdfs
