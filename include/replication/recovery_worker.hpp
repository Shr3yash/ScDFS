#pragma once

#include <memory>
#include <thread>
#include <atomic>
#include <functional>
#include "common/types.hpp"
#include "common/config.hpp"
#include "hashing/consistent_hash_ring.hpp"
#include "metadata/metadata_store.hpp"
#include "replication/replication_manager.hpp"

namespace scdfs {

// Background worker that:
// 1. Monitors storage node heartbeats
// 2. Detects node failures
// 3. Identifies under-replicated chunks
// 4. Runs the merge/recovery protocol to restore replication factor
//
// Merge protocol for write-in-flight failures:
//   - If metadata shows PENDING (uncommitted): discard partial replicas, surface previous version
//   - If metadata shows COMMITTED: re-replicate missing copies from survivors
class RecoveryWorker {
public:
    RecoveryWorker(std::shared_ptr<ConsistentHashRing> ring,
                   std::shared_ptr<MetadataStore> metadata,
                   std::shared_ptr<ReplicationManager> replication,
                   const Config& config);
    ~RecoveryWorker();

    void start();
    void stop();

    // Manually trigger recovery for a failed node (also called automatically by heartbeat monitor).
    void recover_node(const NodeId& failed_node);

    // Check and resolve a single chunk's replication status.
    void resolve_chunk(const ChunkMetadata& chunk);

    size_t chunks_recovered() const { return chunks_recovered_.load(); }
    size_t recovery_cycles() const { return recovery_cycles_.load(); }

private:
    std::shared_ptr<ConsistentHashRing> ring_;
    std::shared_ptr<MetadataStore> metadata_;
    std::shared_ptr<ReplicationManager> replication_;
    Config config_;

    std::thread monitor_thread_;
    std::atomic<bool> running_{false};
    std::atomic<size_t> chunks_recovered_{0};
    std::atomic<size_t> recovery_cycles_{0};

    void heartbeat_monitor_loop();
    void check_node_health();

    // Merge protocol: handle chunks that were in-flight during failure
    void merge_protocol(const ChunkMetadata& chunk, const NodeId& failed_node);

    // Find a new target node not already holding this chunk
    NodeId find_new_target(const ChunkMetadata& chunk) const;
};

} // namespace scdfs
