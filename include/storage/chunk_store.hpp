#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include "common/types.hpp"

namespace scdfs {

// Local on-disk chunk storage for a single storage node.
// Stores chunks as flat files under a data directory: data/<chunk_id>.chunk
class ChunkStore {
public:
    explicit ChunkStore(const std::string& data_dir);

    bool store_chunk(const ChunkId& chunk_id, const uint8_t* data, size_t size);
    bool store_chunk(const ChunkId& chunk_id, const std::vector<uint8_t>& data);

    // Reads the chunk into the output buffer. Returns actual size, or 0 on failure.
    size_t read_chunk(const ChunkId& chunk_id, std::vector<uint8_t>& out) const;

    bool delete_chunk(const ChunkId& chunk_id);
    bool has_chunk(const ChunkId& chunk_id) const;

    size_t chunk_size(const ChunkId& chunk_id) const;
    std::vector<ChunkId> list_chunks() const;

    size_t total_stored_bytes() const { return total_bytes_.load(); }
    size_t chunk_count() const;

    Checksum compute_checksum(const uint8_t* data, size_t size) const;

private:
    std::string data_dir_;
    mutable std::mutex mutex_;
    std::atomic<size_t> total_bytes_{0};

    std::string chunk_path(const ChunkId& chunk_id) const;
    void ensure_directory() const;
};

} // namespace scdfs
