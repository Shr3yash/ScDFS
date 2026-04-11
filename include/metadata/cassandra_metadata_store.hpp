#pragma once

#include "metadata/metadata_store.hpp"
#include "common/config.hpp"

#ifdef SCDFS_USE_CASSANDRA
#include <cassandra.h>
#endif

#include <memory>
#include <string>

namespace scdfs {

// Production metadata store backed by Apache Cassandra.
// Requires the DataStax C++ driver and a running Cassandra cluster.
// Compile with -DSCDFS_USE_CASSANDRA to enable.
class CassandraMetadataStore : public MetadataStore {
public:
    explicit CassandraMetadataStore(const Config& config);
    ~CassandraMetadataStore() override;

    bool initialize();

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
    Config config_;

#ifdef SCDFS_USE_CASSANDRA
    CassCluster* cluster_ = nullptr;
    CassSession* session_ = nullptr;

    bool execute_query(const std::string& query);
    CassFuture* execute_statement(CassStatement* stmt);
    bool create_schema();
    std::string join_list(const std::vector<std::string>& items) const;
    std::vector<std::string> parse_list(const CassValue* value) const;
#endif
};

} // namespace scdfs
