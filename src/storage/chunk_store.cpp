#include "storage/chunk_store.hpp"
#include "common/logger.hpp"

#include <fstream>
#include <filesystem>
#include <sstream>
#include <iomanip>

namespace fs = std::filesystem;

namespace scdfs {

ChunkStore::ChunkStore(const std::string& data_dir) : data_dir_(data_dir) {
    ensure_directory();

    // Count bytes already on disk so the total is not zero after a restart.
    if (fs::exists(data_dir_)) {
        for (const auto& entry : fs::directory_iterator(data_dir_)) {
            if (entry.path().extension() == ".chunk") {
                total_bytes_ += entry.file_size();
            }
        }
    }
    LOG_INFO("ChunkStore initialized at ", data_dir_, ", existing bytes=", total_bytes_.load());
}

bool ChunkStore::store_chunk(const ChunkId& chunk_id, const uint8_t* data, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string path = chunk_path(chunk_id);
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file) {
        LOG_ERROR("Failed to open file for writing: ", path);
        return false;
    }

    file.write(reinterpret_cast<const char*>(data), size);
    if (!file.good()) {
        LOG_ERROR("Failed to write chunk ", chunk_id, " (", size, " bytes)");
        return false;
    }

    total_bytes_ += size;
    LOG_DEBUG("Stored chunk ", chunk_id, " (", size, " bytes)");
    return true;
}

bool ChunkStore::store_chunk(const ChunkId& chunk_id, const std::vector<uint8_t>& data) {
    return store_chunk(chunk_id, data.data(), data.size());
}

size_t ChunkStore::read_chunk(const ChunkId& chunk_id, std::vector<uint8_t>& out) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string path = chunk_path(chunk_id);
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file) {
        LOG_WARN("Chunk not found: ", chunk_id);
        return 0;
    }

    size_t size = file.tellg();
    file.seekg(0);
    out.resize(size);
    file.read(reinterpret_cast<char*>(out.data()), size);

    return file.good() ? size : 0;
}

bool ChunkStore::delete_chunk(const ChunkId& chunk_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string path = chunk_path(chunk_id);
    if (!fs::exists(path)) return false;

    size_t size = fs::file_size(path);
    if (fs::remove(path)) {
        total_bytes_ -= size;
        LOG_DEBUG("Deleted chunk ", chunk_id);
        return true;
    }
    return false;
}

bool ChunkStore::has_chunk(const ChunkId& chunk_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return fs::exists(chunk_path(chunk_id));
}

size_t ChunkStore::chunk_size(const ChunkId& chunk_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string path = chunk_path(chunk_id);
    if (!fs::exists(path)) return 0;
    return fs::file_size(path);
}

std::vector<ChunkId> ChunkStore::list_chunks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ChunkId> chunks;

    if (!fs::exists(data_dir_)) return chunks;

    for (const auto& entry : fs::directory_iterator(data_dir_)) {
        if (entry.path().extension() == ".chunk") {
            chunks.push_back(entry.path().stem().string());
        }
    }
    return chunks;
}

size_t ChunkStore::chunk_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    if (!fs::exists(data_dir_)) return 0;
    for (const auto& entry : fs::directory_iterator(data_dir_)) {
        if (entry.path().extension() == ".chunk") ++count;
    }
    return count;
}

Checksum ChunkStore::compute_checksum(const uint8_t* data, size_t size) const {
    // FNV-1a 64. Stored on the chunk row; reads never compare it.
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < size; i++) {
        hash ^= data[i];
        hash *= 1099511628211ULL;
    }
    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(16) << hash;
    return oss.str();
}

std::string ChunkStore::chunk_path(const ChunkId& chunk_id) const {
    return data_dir_ + "/" + chunk_id + ".chunk";
}

void ChunkStore::ensure_directory() const {
    if (!fs::exists(data_dir_)) {
        fs::create_directories(data_dir_);
    }
}

} // namespace scdfs
