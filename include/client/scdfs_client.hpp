#pragma once

#include <string>
#include <vector>
#include <memory>
#include "common/types.hpp"
#include "common/config.hpp"
#include "coordinator/coordinator.hpp"

namespace scdfs {

// In-process wrapper around Coordinator. put/get touch the local disk.
// put_data/get_data stay in memory so the benchmark is not measuring fread.
class ScDFSClient {
public:
    explicit ScDFSClient(std::shared_ptr<Coordinator> coordinator);

    UploadResult put(const std::string& local_path, const FilePath& remote_path);
    bool get(const FilePath& remote_path, const std::string& local_path);
    bool remove(const FilePath& remote_path);
    std::vector<FileMetadata> list(const std::string& prefix = "");
    std::optional<FileMetadata> info(const FilePath& remote_path);

    UploadResult put_data(const FilePath& remote_path, const uint8_t* data, size_t size);
    DownloadResult get_data(const FilePath& remote_path);
    DownloadResult get_data_sequential(const FilePath& remote_path);

private:
    std::shared_ptr<Coordinator> coordinator_;
};

} // namespace scdfs
