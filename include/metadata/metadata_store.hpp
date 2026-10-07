#pragma once

#include <string>
#include <vector>
#include <optional>
#include "common/types.hpp"

namespace scdfs {

// File, chunk, and node rows.
// MemoryMetadataStore is the default. CassandraMetadataStore needs the driver.
class MetadataStore {
public:
    virtual ~MetadataStore() = default;

    virtual bool put_file(const FileMetadata& file) = 0;
    virtual std::optional<FileMetadata> get_file(const FilePath& path) = 0;
    virtual bool delete_file(const FilePath& path) = 0;
    virtual std::vector<FileMetadata> list_files(const std::string& prefix = "") = 0;

    virtual bool put_chunk(const ChunkMetadata& chunk) = 0;
    virtual std::optional<ChunkMetadata> get_chunk(const FilePath& file_path, int chunk_index) = 0;
    virtual std::vector<ChunkMetadata> get_chunks_for_file(const FilePath& file_path) = 0;
    virtual bool update_chunk_status(const FilePath& file_path, int chunk_index, ChunkStatus status) = 0;
    virtual bool update_chunk_replicas(const FilePath& file_path, int chunk_index,
                                       const std::vector<NodeId>& replicas) = 0;

    virtual std::vector<ChunkMetadata> get_chunks_on_node(const NodeId& node_id) = 0;

    virtual bool register_node(const NodeInfo& node) = 0;
    virtual bool update_node_status(const NodeId& node_id, NodeStatus status) = 0;
    virtual bool update_heartbeat(const NodeId& node_id) = 0;
    virtual std::optional<NodeInfo> get_node(const NodeId& node_id) = 0;
    virtual std::vector<NodeInfo> get_all_nodes() = 0;
    virtual std::vector<NodeInfo> get_active_nodes() = 0;
    virtual std::vector<NodeInfo> get_failed_nodes() = 0;
};

} // namespace scdfs
