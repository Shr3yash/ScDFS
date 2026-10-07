#include "client/scdfs_client.hpp"
#include "common/logger.hpp"

#include <fstream>
#include <filesystem>

namespace fs = std::filesystem;

namespace scdfs {

ScDFSClient::ScDFSClient(std::shared_ptr<Coordinator> coordinator)
    : coordinator_(std::move(coordinator)) {}

UploadResult ScDFSClient::put(const std::string& local_path, const FilePath& remote_path) {
    if (!fs::exists(local_path)) {
        LOG_ERROR("Local file not found: ", local_path);
        return {false, remote_path, 0, 0, 0, 0};
    }

    size_t file_size = fs::file_size(local_path);
    std::vector<uint8_t> data(file_size);

    std::ifstream file(local_path, std::ios::binary);
    if (!file) {
        LOG_ERROR("Cannot open file: ", local_path);
        return {false, remote_path, 0, 0, 0, 0};
    }

    file.read(reinterpret_cast<char*>(data.data()), file_size);
    return coordinator_->upload_file(remote_path, data.data(), data.size());
}

bool ScDFSClient::get(const FilePath& remote_path, const std::string& local_path) {
    auto result = coordinator_->download_file(remote_path);
    if (!result.success) return false;

    auto parent = fs::path(local_path).parent_path();
    if (!parent.empty() && !fs::exists(parent)) {
        fs::create_directories(parent);
    }

    std::ofstream file(local_path, std::ios::binary | std::ios::trunc);
    if (!file) {
        LOG_ERROR("Cannot create local file: ", local_path);
        return false;
    }

    file.write(reinterpret_cast<const char*>(result.data.data()), result.data.size());
    return file.good();
}

bool ScDFSClient::remove(const FilePath& remote_path) {
    return coordinator_->delete_file(remote_path);
}

std::vector<FileMetadata> ScDFSClient::list(const std::string& prefix) {
    return coordinator_->list_files(prefix);
}

std::optional<FileMetadata> ScDFSClient::info(const FilePath& remote_path) {
    return coordinator_->get_file_info(remote_path);
}

UploadResult ScDFSClient::put_data(const FilePath& remote_path, const uint8_t* data, size_t size) {
    return coordinator_->upload_file(remote_path, data, size);
}

DownloadResult ScDFSClient::get_data(const FilePath& remote_path) {
    return coordinator_->download_file(remote_path);
}

DownloadResult ScDFSClient::get_data_sequential(const FilePath& remote_path) {
    return coordinator_->download_file_sequential(remote_path);
}

} // namespace scdfs
