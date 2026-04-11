#pragma once

#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include "common/types.hpp"
#include "common/config.hpp"
#include "hashing/consistent_hash_ring.hpp"
#include "metadata/metadata_store.hpp"
#include "replication/replication_manager.hpp"
#include "replication/recovery_worker.hpp"
#include "threading/thread_pool.hpp"

namespace scdfs {

struct UploadResult {
    bool success;
    FilePath file_path;
    size_t file_size;
    int chunk_count;
    Version version;
    double elapsed_ms;
};

struct DownloadResult {
    bool success;
    std::vector<uint8_t> data;
    size_t file_size;
    double elapsed_ms;
};

// Central coordinator that manages the distributed file system.
// Handles file splitting, chunk placement, replication, and recovery.
class Coordinator {
public:
    explicit Coordinator(const Config& config);
    ~Coordinator();

    void start();
    void stop();

    // Register a storage node.
    bool register_storage_node(const NodeId& node_id, const std::string& address, uint16_t port);

    // Upload a file: split into chunks, distribute with replication.
    UploadResult upload_file(const FilePath& path, const uint8_t* data, size_t size);

    // Download a file: fetch all chunks in parallel, reassemble.
    DownloadResult download_file(const FilePath& path);

    // Download using sequential fetching (for benchmark comparison).
    DownloadResult download_file_sequential(const FilePath& path);

    // Delete a file and all its chunks.
    bool delete_file(const FilePath& path);

    // List files.
    std::vector<FileMetadata> list_files(const std::string& prefix = "");

    // Get file info.
    std::optional<FileMetadata> get_file_info(const FilePath& path);

    // Manual recovery trigger.
    void trigger_recovery(const NodeId& failed_node);

    // Access components for testing.
    ConsistentHashRing& ring() { return *ring_; }
    MetadataStore& metadata() { return *metadata_; }
    RecoveryWorker& recovery() { return *recovery_; }

private:
    Config config_;

    std::shared_ptr<ConsistentHashRing> ring_;
    std::shared_ptr<MetadataStore> metadata_;
    std::shared_ptr<ReplicationManager> replication_;
    std::shared_ptr<RecoveryWorker> recovery_;
    std::unique_ptr<ThreadPool> pool_;

    std::atomic<uint32_t> next_request_id_{0};
    std::atomic<bool> running_{false};

    // Split file into fixed-size chunks.
    struct ChunkData {
        int index;
        ChunkId id;
        std::vector<uint8_t> data;
        Checksum checksum;
    };
    std::vector<ChunkData> split_file(const FilePath& path, const uint8_t* data, size_t size);

    ChunkId generate_chunk_id(const FilePath& path, int index) const;
};

} // namespace scdfs
