#include "replication/recovery_worker.hpp"
#include "common/logger.hpp"
#include <algorithm>

namespace scdfs {

RecoveryWorker::RecoveryWorker(std::shared_ptr<ConsistentHashRing> ring,
                               std::shared_ptr<MetadataStore> metadata,
                               std::shared_ptr<ReplicationManager> replication,
                               const Config& config)
    : ring_(std::move(ring)), metadata_(std::move(metadata)),
      replication_(std::move(replication)), config_(config) {}

RecoveryWorker::~RecoveryWorker() {
    stop();
}

void RecoveryWorker::start() {
    if (running_) return;
    running_ = true;
    monitor_thread_ = std::thread(&RecoveryWorker::heartbeat_monitor_loop, this);
    LOG_INFO("RecoveryWorker started");
}

void RecoveryWorker::stop() {
    if (!running_) return;
    running_ = false;
    if (monitor_thread_.joinable()) {
        monitor_thread_.join();
    }
    LOG_INFO("RecoveryWorker stopped, recovered ", chunks_recovered_.load(), " chunks in ",
             recovery_cycles_.load(), " cycles");
}

void RecoveryWorker::heartbeat_monitor_loop() {
    while (running_) {
        check_node_health();
        recovery_cycles_++;

        std::this_thread::sleep_for(
            std::chrono::milliseconds(config_.heartbeat_interval_ms));
    }
}

void RecoveryWorker::check_node_health() {
    auto all_nodes = metadata_->get_all_nodes();
    auto now = std::chrono::system_clock::now();

    for (const auto& node : all_nodes) {
        if (node.status != NodeStatus::ACTIVE) continue;

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - node.last_heartbeat).count();

        if (elapsed > config_.heartbeat_timeout_ms) {
            LOG_WARN("Node ", node.node_id, " heartbeat timeout (",
                     elapsed, "ms > ", config_.heartbeat_timeout_ms, "ms)");

            metadata_->update_node_status(node.node_id, NodeStatus::FAILED);
            ring_->remove_node(node.node_id);

            recover_node(node.node_id);
        }
    }
}

void RecoveryWorker::recover_node(const NodeId& failed_node) {
    LOG_INFO("Starting recovery for failed node: ", failed_node);

    auto affected_chunks = metadata_->get_chunks_on_node(failed_node);
    LOG_INFO("Found ", affected_chunks.size(), " chunks on failed node ", failed_node);

    for (auto& chunk : affected_chunks) {
        if (!running_) break;
        merge_protocol(chunk, failed_node);
    }

    LOG_INFO("Recovery complete for node ", failed_node,
             ", total chunks recovered: ", chunks_recovered_.load());
}

void RecoveryWorker::merge_protocol(const ChunkMetadata& chunk, const NodeId& failed_node) {
    // Core merge protocol:
    // 1. Check metadata commit status
    // 2. If PENDING (write was in-flight): discard partial replicas, don't re-replicate
    // 3. If COMMITTED: re-replicate from survivors to restore replication factor

    if (chunk.status == ChunkStatus::PENDING) {
        // Write was in-flight when node failed. Metadata never committed.
        // Discard this chunk version — client will need to retry the write.
        LOG_WARN("Chunk ", chunk.chunk_id, " was PENDING during failure, discarding partial replicas");

        for (const auto& node : chunk.replica_nodes) {
            if (node != failed_node) {
                replication_->delete_from_node(chunk.chunk_id, node);
            }
        }

        metadata_->update_chunk_status(chunk.file_path, chunk.chunk_index, ChunkStatus::DELETED);
        return;
    }

    if (chunk.status == ChunkStatus::DELETED) return;

    // COMMITTED — need to restore replication factor
    // Remove the failed node from replica list
    std::vector<NodeId> survivors;
    for (const auto& node : chunk.replica_nodes) {
        if (node != failed_node) survivors.push_back(node);
    }

    if (survivors.empty()) {
        LOG_ERROR("CRITICAL: No surviving replicas for chunk ", chunk.chunk_id);
        return;
    }

    int deficit = config_.replication_factor - static_cast<int>(survivors.size());
    if (deficit <= 0) {
        // Already at or above replication factor (failed node wasn't actually holding a replica)
        metadata_->update_chunk_replicas(chunk.file_path, chunk.chunk_index, survivors);
        return;
    }

    LOG_INFO("Re-replicating chunk ", chunk.chunk_id, " (deficit=", deficit, ")");

    for (int i = 0; i < deficit; i++) {
        NodeId target = find_new_target(chunk);
        if (target.empty()) {
            LOG_WARN("Not enough nodes to restore replication for chunk ", chunk.chunk_id);
            break;
        }

        // Pick the first survivor as source
        if (replication_->re_replicate_chunk(chunk.chunk_id, survivors[0], target)) {
            survivors.push_back(target);
            chunks_recovered_++;
        } else {
            LOG_ERROR("Failed to re-replicate chunk ", chunk.chunk_id, " to ", target);
        }
    }

    metadata_->update_chunk_replicas(chunk.file_path, chunk.chunk_index, survivors);
}

void RecoveryWorker::resolve_chunk(const ChunkMetadata& chunk) {
    if (chunk.status != ChunkStatus::COMMITTED) return;

    int active_replicas = 0;
    NodeId failed_node;
    for (const auto& node : chunk.replica_nodes) {
        auto info = metadata_->get_node(node);
        if (info && info->status == NodeStatus::ACTIVE) {
            active_replicas++;
        } else {
            failed_node = node;
        }
    }

    if (active_replicas < config_.replication_factor && !failed_node.empty()) {
        merge_protocol(chunk, failed_node);
    }
}

NodeId RecoveryWorker::find_new_target(const ChunkMetadata& chunk) const {
    auto active_nodes = metadata_->get_active_nodes();

    for (const auto& node : active_nodes) {
        bool already_holds = false;
        for (const auto& replica : chunk.replica_nodes) {
            if (replica == node.node_id) {
                already_holds = true;
                break;
            }
        }
        if (!already_holds) return node.node_id;
    }

    return "";
}

} // namespace scdfs
