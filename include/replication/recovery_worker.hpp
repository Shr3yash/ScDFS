#pragma once

#include <memory>
#include <thread>
#include <atomic>
#include "common/types.hpp"
#include "common/config.hpp"
#include "hashing/consistent_hash_ring.hpp"
#include "metadata/metadata_store.hpp"
#include "replication/replication_manager.hpp"

namespace scdfs {

// Times out a node whose last_heartbeat is stale, then repairs its chunks.
// PENDING is deleted. COMMITTED is copied from a survivor until the factor is met.
// last_heartbeat is only set in register_storage_node, so this fires on its own
// if the process outlives heartbeat_timeout_ms. Tests call recover_node instead.
class RecoveryWorker {
public:
    RecoveryWorker(std::shared_ptr<ConsistentHashRing> ring,
                   std::shared_ptr<MetadataStore> metadata,
                   std::shared_ptr<ReplicationManager> replication,
                   const Config& config);
    ~RecoveryWorker();

    void start();
    void stop();

    void recover_node(const NodeId& failed_node);

    // Same repair as recover_node, for one chunk. No caller yet.
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

    void merge_protocol(const ChunkMetadata& chunk, const NodeId& failed_node);
    NodeId find_new_target(const ChunkMetadata& chunk) const;
};

} // namespace scdfs
