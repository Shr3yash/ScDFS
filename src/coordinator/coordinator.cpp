#include "coordinator/coordinator.hpp"
#include "metadata/memory_metadata_store.hpp"
#include "metadata/cassandra_metadata_store.hpp"
#include "hashing/murmur3.hpp"
#include "common/logger.hpp"
#include "storage/chunk_store.hpp"

#include <chrono>
#include <future>
#include <algorithm>

namespace scdfs {

Coordinator::Coordinator(const Config& config) : config_(config) {
    ring_ = std::make_shared<ConsistentHashRing>(config_.virtual_nodes);

    if (config_.use_cassandra) {
        auto cass_store = std::make_shared<CassandraMetadataStore>(config_);
        if (!cass_store->initialize()) {
            LOG_ERROR("Failed to initialize Cassandra, falling back to in-memory store");
            metadata_ = std::make_shared<MemoryMetadataStore>();
        } else {
            metadata_ = cass_store;
        }
    } else {
        metadata_ = std::make_shared<MemoryMetadataStore>();
    }

    replication_ = std::make_shared<ReplicationManager>(ring_, metadata_, config_);
    recovery_ = std::make_shared<RecoveryWorker>(ring_, metadata_, replication_, config_);
    pool_ = std::make_unique<ThreadPool>(config_.thread_pool_size);
}

Coordinator::~Coordinator() {
    stop();
}

void Coordinator::start() {
    if (running_) return;
    running_ = true;
    recovery_->start();
    LOG_INFO("Coordinator started (chunk_size=", config_.chunk_size,
             ", replication=", config_.replication_factor,
             ", threads=", config_.thread_pool_size, ")");
}

void Coordinator::stop() {
    if (!running_) return;
    running_ = false;
    recovery_->stop();
    pool_->shutdown();
    LOG_INFO("Coordinator stopped");
}

bool Coordinator::register_storage_node(const NodeId& node_id, const std::string& address,
                                         uint16_t port) {
    ring_->add_node(node_id);

    NodeInfo info;
    info.node_id = node_id;
    info.address = address;
    info.port = port;
    info.status = NodeStatus::ACTIVE;
    info.last_heartbeat = std::chrono::system_clock::now();
    info.capacity_bytes = 0;
    info.used_bytes = 0;

    return metadata_->register_node(info);
}

UploadResult Coordinator::upload_file(const FilePath& path, const uint8_t* data, size_t size) {
    auto start_time = std::chrono::high_resolution_clock::now();

    UploadResult result{};
    result.file_path = path;
    result.file_size = size;

    auto chunks = split_file(path, data, size);
    result.chunk_count = static_cast<int>(chunks.size());

    LOG_INFO("Uploading ", path, " (", size, " bytes, ", chunks.size(), " chunks)");

    auto existing = metadata_->get_file(path);
    Version version = existing ? existing->version + 1 : 1;
    result.version = version;

    for (const auto& chunk : chunks) {
        auto target_nodes = replication_->get_replica_nodes(chunk.id);

        ChunkMetadata meta;
        meta.chunk_id = chunk.id;
        meta.file_path = path;
        meta.chunk_index = chunk.index;
        meta.chunk_size = chunk.data.size();
        meta.checksum = chunk.checksum;
        meta.version = version;
        meta.status = ChunkStatus::PENDING;
        meta.replica_nodes = target_nodes;

        metadata_->put_chunk(meta);
    }

    std::vector<std::future<std::vector<NodeId>>> futures;
    futures.reserve(chunks.size());

    for (const auto& chunk : chunks) {
        futures.push_back(pool_->submit([this, &chunk]() {
            return replication_->replicate_chunk(chunk.id, chunk.data.data(), chunk.data.size());
        }));
    }

    // Pipeline success hands back every target, so this only fails
    // when the direct-write fallback missed write_quorum.
    bool all_committed = true;
    for (size_t i = 0; i < futures.size(); i++) {
        auto acked_nodes = futures[i].get();

        if (static_cast<int>(acked_nodes.size()) >= config_.write_quorum) {
            metadata_->update_chunk_status(path, chunks[i].index, ChunkStatus::COMMITTED);
            metadata_->update_chunk_replicas(path, chunks[i].index, acked_nodes);
        } else {
            LOG_ERROR("Chunk ", chunks[i].id, " failed quorum: ", acked_nodes.size(),
                      " < ", config_.write_quorum);
            all_committed = false;
        }
    }

    if (all_committed) {
        FileMetadata file_meta;
        file_meta.file_path = path;
        file_meta.file_size = size;
        file_meta.chunk_count = static_cast<int>(chunks.size());
        file_meta.version = version;
        file_meta.created_at = existing ? existing->created_at : std::chrono::system_clock::now();
        file_meta.updated_at = std::chrono::system_clock::now();

        metadata_->put_file(file_meta);
        result.success = true;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    result.elapsed_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    LOG_INFO("Upload ", (result.success ? "succeeded" : "FAILED"), " for ", path,
             " in ", result.elapsed_ms, "ms");

    return result;
}

DownloadResult Coordinator::download_file(const FilePath& path) {
    auto start_time = std::chrono::high_resolution_clock::now();

    DownloadResult result{};

    auto file_meta = metadata_->get_file(path);
    if (!file_meta) {
        LOG_WARN("File not found: ", path);
        return result;
    }

    auto chunks = metadata_->get_chunks_for_file(path);
    if (chunks.empty()) {
        LOG_WARN("No chunks found for file: ", path);
        return result;
    }

    LOG_INFO("Downloading ", path, " (", chunks.size(), " chunks, parallel)");

    std::vector<std::future<std::pair<int, std::vector<uint8_t>>>> futures;
    futures.reserve(chunks.size());

    for (const auto& chunk : chunks) {
        if (chunk.status != ChunkStatus::COMMITTED) continue;

        futures.push_back(pool_->submit([this, chunk]() -> std::pair<int, std::vector<uint8_t>> {
            std::vector<uint8_t> data;
            if (replication_->fetch_chunk(chunk.chunk_id, chunk.replica_nodes, data)) {
                return {chunk.chunk_index, std::move(data)};
            }
            return {chunk.chunk_index, {}};
        }));
    }

    std::vector<std::pair<int, std::vector<uint8_t>>> chunk_results;
    chunk_results.reserve(futures.size());

    for (auto& f : futures) {
        chunk_results.push_back(f.get());
    }

    std::sort(chunk_results.begin(), chunk_results.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    result.data.reserve(file_meta->file_size);
    for (const auto& [idx, data] : chunk_results) {
        if (data.empty()) {
            LOG_ERROR("Missing chunk ", idx, " for file ", path);
            return result;
        }
        result.data.insert(result.data.end(), data.begin(), data.end());
    }

    result.success = true;
    result.file_size = result.data.size();

    auto end_time = std::chrono::high_resolution_clock::now();
    result.elapsed_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    LOG_INFO("Download succeeded for ", path, " in ", result.elapsed_ms, "ms");
    return result;
}

DownloadResult Coordinator::download_file_sequential(const FilePath& path) {
    auto start_time = std::chrono::high_resolution_clock::now();

    DownloadResult result{};

    auto file_meta = metadata_->get_file(path);
    if (!file_meta) return result;

    auto chunks = metadata_->get_chunks_for_file(path);
    if (chunks.empty()) return result;

    LOG_INFO("Downloading ", path, " (", chunks.size(), " chunks, sequential)");

    result.data.reserve(file_meta->file_size);

    for (const auto& chunk : chunks) {
        if (chunk.status != ChunkStatus::COMMITTED) continue;

        std::vector<uint8_t> data;
        if (!replication_->fetch_chunk(chunk.chunk_id, chunk.replica_nodes, data)) {
            LOG_ERROR("Failed to fetch chunk ", chunk.chunk_index, " for file ", path);
            return result;
        }
        result.data.insert(result.data.end(), data.begin(), data.end());
    }

    result.success = true;
    result.file_size = result.data.size();

    auto end_time = std::chrono::high_resolution_clock::now();
    result.elapsed_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    LOG_INFO("Sequential download for ", path, " in ", result.elapsed_ms, "ms");
    return result;
}

bool Coordinator::delete_file(const FilePath& path) {
    auto chunks = metadata_->get_chunks_for_file(path);

    for (const auto& chunk : chunks) {
        for (const auto& node : chunk.replica_nodes) {
            replication_->delete_from_node(chunk.chunk_id, node);
        }
        metadata_->update_chunk_status(path, chunk.chunk_index, ChunkStatus::DELETED);
    }

    return metadata_->delete_file(path);
}

std::vector<FileMetadata> Coordinator::list_files(const std::string& prefix) {
    return metadata_->list_files(prefix);
}

std::optional<FileMetadata> Coordinator::get_file_info(const FilePath& path) {
    return metadata_->get_file(path);
}

void Coordinator::trigger_recovery(const NodeId& failed_node) {
    recovery_->recover_node(failed_node);
}

std::vector<Coordinator::ChunkData> Coordinator::split_file(const FilePath& path,
                                                              const uint8_t* data, size_t size) {
    std::vector<ChunkData> chunks;
    int index = 0;
    size_t offset = 0;

    // FNV only. ChunkStore also creates this directory and scans it for *.chunk.
    ChunkStore checksum_helper("/tmp");

    while (offset < size) {
        size_t chunk_size = std::min(config_.chunk_size, size - offset);

        ChunkData cd;
        cd.index = index;
        cd.id = generate_chunk_id(path, index);
        cd.data.assign(data + offset, data + offset + chunk_size);
        cd.checksum = checksum_helper.compute_checksum(cd.data.data(), cd.data.size());

        chunks.push_back(std::move(cd));
        offset += chunk_size;
        index++;
    }

    return chunks;
}

ChunkId Coordinator::generate_chunk_id(const FilePath& path, int index) const {
    std::string key = path + ":" + std::to_string(index);
    return Murmur3::hash_to_hex(key);
}

} // namespace scdfs
