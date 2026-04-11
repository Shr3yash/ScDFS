#pragma once

#include <string>
#include <vector>
#include <memory>
#include "common/types.hpp"
#include "common/config.hpp"
#include "coordinator/coordinator.hpp"

namespace scdfs {

// High-level client API for interacting with ScDFS.
// In a production system this would communicate with the coordinator over RPC;
// here it operates in-process for demonstration and benchmarking.
class ScDFSClient {
public:
    explicit ScDFSClient(std::shared_ptr<Coordinator> coordinator);

    // Upload a file from the local filesystem to ScDFS.
    UploadResult put(const std::string& local_path, const FilePath& remote_path);

    // Download a file from ScDFS to the local filesystem.
    bool get(const FilePath& remote_path, const std::string& local_path);

    // Delete a file from ScDFS.
    bool remove(const FilePath& remote_path);

    // List files in ScDFS.
    std::vector<FileMetadata> list(const std::string& prefix = "");

    // Get file info.
    std::optional<FileMetadata> info(const FilePath& remote_path);

    // Upload raw data (for benchmarking without disk I/O).
    UploadResult put_data(const FilePath& remote_path, const uint8_t* data, size_t size);

    // Download raw data (for benchmarking without disk I/O).
    DownloadResult get_data(const FilePath& remote_path);

    // Sequential download (for benchmark comparison).
    DownloadResult get_data_sequential(const FilePath& remote_path);

private:
    std::shared_ptr<Coordinator> coordinator_;
};

} // namespace scdfs
