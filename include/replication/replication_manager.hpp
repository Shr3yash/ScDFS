#pragma once

#include <memory>
#include <vector>
#include "common/types.hpp"
#include "common/config.hpp"
#include "hashing/consistent_hash_ring.hpp"
#include "metadata/metadata_store.hpp"
#include "threading/thread_pool.hpp"

namespace scdfs {

// Replica sets come from the ring.
// replicate_chunk tries the primary first. If that call fails it writes
// each node from here and returns the ones that ACKed.
class ReplicationManager {
public:
    ReplicationManager(std::shared_ptr<ConsistentHashRing> ring,
                       std::shared_ptr<MetadataStore> metadata,
                       const Config& config);

    std::vector<NodeId> get_replica_nodes(const ChunkId& chunk_id) const;

    // Pipeline success returns every target, not the hops that stored the bytes.
    std::vector<NodeId> replicate_chunk(const ChunkId& chunk_id,
                                         const uint8_t* data, size_t size);

    bool re_replicate_chunk(const ChunkId& chunk_id,
                            const NodeId& source_node,
                            const NodeId& target_node);

    bool delete_from_node(const ChunkId& chunk_id, const NodeId& node_id);

    // First replica that answers. read_quorum is not applied.
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

    std::pair<std::string, uint16_t> resolve_node(const NodeId& node_id) const;
};

} // namespace scdfs
