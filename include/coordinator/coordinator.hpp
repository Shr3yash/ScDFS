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

// Holds the ring, metadata, replication, and the recovery thread.
// upload/download run in this process. Storage nodes are the TCP peers.
class Coordinator {
public:
    explicit Coordinator(const Config& config);
    ~Coordinator();

    void start();
    void stop();

    bool register_storage_node(const NodeId& node_id, const std::string& address, uint16_t port);

    UploadResult upload_file(const FilePath& path, const uint8_t* data, size_t size);
    DownloadResult download_file(const FilePath& path);

    // One chunk at a time, for the benchmark.
    DownloadResult download_file_sequential(const FilePath& path);

    bool delete_file(const FilePath& path);

    std::vector<FileMetadata> list_files(const std::string& prefix = "");
    std::optional<FileMetadata> get_file_info(const FilePath& path);

    void trigger_recovery(const NodeId& failed_node);

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
