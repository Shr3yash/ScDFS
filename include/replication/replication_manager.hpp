#pragma once

#include <memory>
#include <vector>
#include "common/types.hpp"
#include "common/config.hpp"
#include "hashing/consistent_hash_ring.hpp"
#include "metadata/metadata_store.hpp"
#include "threading/thread_pool.hpp"

namespace scdfs {

// Manages chunk replication: determines replica placement on the hash ring
// and orchestrates pipeline-style writes to storage nodes.
class ReplicationManager {
public:
    ReplicationManager(std::shared_ptr<ConsistentHashRing> ring,
                       std::shared_ptr<MetadataStore> metadata,
                       const Config& config);

    // Determine which nodes should hold replicas for a chunk.
    std::vector<NodeId> get_replica_nodes(const ChunkId& chunk_id) const;

    // Replicate a chunk to its designated nodes using pipeline-style writes.
    // Returns the list of nodes that acknowledged the write.
    std::vector<NodeId> replicate_chunk(const ChunkId& chunk_id,
                                         const uint8_t* data, size_t size);

    // Re-replicate a chunk from a surviving replica to a new target.
    bool re_replicate_chunk(const ChunkId& chunk_id,
                            const NodeId& source_node,
                            const NodeId& target_node);

    // Delete a chunk from a specific node.
    bool delete_from_node(const ChunkId& chunk_id, const NodeId& node_id);

    // Fetch a chunk from one of the available replicas.
    bool fetch_chunk(const ChunkId& chunk_id, const std::vector<NodeId>& replicas,
                     std::vector<uint8_t>& out);

private:
    std::shared_ptr<ConsistentHashRing> ring_;
    std::shared_ptr<MetadataStore> metadata_;
    Config config_;

    bool send_chunk_to_node(const ChunkId& chunk_id, const uint8_t* data, size_t size,
                            const NodeId& node_id, const std::vector<NodeId>& downstream);
    bool fetch_from_node(const ChunkId& chunk_id, const NodeId& node_id,
                         std::vector<uint8_t>& out);

    // Parse "address:port" from node_id or from metadata
    std::pair<std::string, uint16_t> resolve_node(const NodeId& node_id) const;
};

} // namespace scdfs
