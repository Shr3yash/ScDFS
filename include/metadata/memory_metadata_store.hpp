#pragma once

#include "metadata/metadata_store.hpp"
#include <unordered_map>
#include <shared_mutex>
#include <map>

namespace scdfs {

// Thread-safe in-memory metadata store for local development and testing.
class MemoryMetadataStore : public MetadataStore {
public:
    MemoryMetadataStore() = default;

    bool put_file(const FileMetadata& file) override;
    std::optional<FileMetadata> get_file(const FilePath& path) override;
    bool delete_file(const FilePath& path) override;
    std::vector<FileMetadata> list_files(const std::string& prefix) override;

    bool put_chunk(const ChunkMetadata& chunk) override;
    std::optional<ChunkMetadata> get_chunk(const FilePath& file_path, int chunk_index) override;
    std::vector<ChunkMetadata> get_chunks_for_file(const FilePath& file_path) override;
    bool update_chunk_status(const FilePath& file_path, int chunk_index, ChunkStatus status) override;
    bool update_chunk_replicas(const FilePath& file_path, int chunk_index,
                               const std::vector<NodeId>& replicas) override;
    std::vector<ChunkMetadata> get_chunks_on_node(const NodeId& node_id) override;

    bool register_node(const NodeInfo& node) override;
    bool update_node_status(const NodeId& node_id, NodeStatus status) override;
    bool update_heartbeat(const NodeId& node_id) override;
    std::optional<NodeInfo> get_node(const NodeId& node_id) override;
    std::vector<NodeInfo> get_all_nodes() override;
    std::vector<NodeInfo> get_active_nodes() override;
    std::vector<NodeInfo> get_failed_nodes() override;

private:
    mutable std::shared_mutex file_mutex_;
    mutable std::shared_mutex chunk_mutex_;
    mutable std::shared_mutex node_mutex_;

    std::unordered_map<FilePath, FileMetadata> files_;

    // key: "filepath:chunk_index"
    std::map<std::string, ChunkMetadata> chunks_;

    std::unordered_map<NodeId, NodeInfo> nodes_;

    static std::string chunk_key(const FilePath& path, int idx) {
        return path + ":" + std::to_string(idx);
    }
};

} // namespace scdfs
