#include "coordinator/coordinator.hpp"
#include "storage/storage_node.hpp"
#include "client/scdfs_client.hpp"
#include "common/logger.hpp"

#include <cassert>
#include <iostream>
#include <random>
#include <thread>
#include <algorithm>

using namespace scdfs;

class TestCluster {
public:
    TestCluster(int num_nodes = 5, size_t chunk_size = 1024 * 1024) {
        config_.chunk_size = chunk_size;
        config_.replication_factor = 3;
        config_.write_quorum = 2;
        config_.thread_pool_size = 4;
        config_.data_directory = "/tmp/scdfs_integration";

        coordinator_ = std::make_shared<Coordinator>(config_);

        for (int i = 0; i < num_nodes; i++) {
            std::string id = "node_" + std::to_string(i);
            uint16_t port = 9300 + i;
            auto node = std::make_unique<StorageNode>(id, "127.0.0.1", port, config_);
            node->start();
            coordinator_->register_storage_node(id, "127.0.0.1", port);
            nodes_.push_back(std::move(node));
        }

        coordinator_->start();
        client_ = std::make_unique<ScDFSClient>(coordinator_);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    ~TestCluster() {
        coordinator_->stop();
        for (auto& n : nodes_) n->stop();
    }

    ScDFSClient& client() { return *client_; }
    Coordinator& coordinator() { return *coordinator_; }
    std::vector<std::unique_ptr<StorageNode>>& nodes() { return nodes_; }

private:
    Config config_;
    std::shared_ptr<Coordinator> coordinator_;
    std::vector<std::unique_ptr<StorageNode>> nodes_;
    std::unique_ptr<ScDFSClient> client_;
};

std::vector<uint8_t> random_data(size_t size, uint32_t seed = 42) {
    std::vector<uint8_t> data(size);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<uint8_t> dist(0, 255);
    for (auto& b : data) b = dist(rng);
    return data;
}

void test_upload_download() {
    TestCluster cluster;

    auto data = random_data(5 * 1024 * 1024); // 5 MB = 5 chunks

    auto up = cluster.client().put_data("/test/file1.bin", data.data(), data.size());
    assert(up.success);
    assert(up.chunk_count == 5);
    assert(up.version == 1);

    auto down = cluster.client().get_data("/test/file1.bin");
    assert(down.success);
    assert(down.data.size() == data.size());
    assert(down.data == data);

    std::cout << "  [PASS] Upload/download round-trip (5 MB, " << up.elapsed_ms << "ms up, "
              << down.elapsed_ms << "ms down)\n";
}

void test_file_listing() {
    TestCluster cluster;

    auto d = random_data(1024);
    cluster.client().put_data("/files/a.txt", d.data(), d.size());
    cluster.client().put_data("/files/b.txt", d.data(), d.size());
    cluster.client().put_data("/other/c.txt", d.data(), d.size());

    auto all = cluster.client().list();
    assert(all.size() == 3);

    auto filtered = cluster.client().list("/files/");
    assert(filtered.size() == 2);

    std::cout << "  [PASS] File listing with prefix filter\n";
}

void test_file_overwrite() {
    TestCluster cluster;

    auto d1 = random_data(2 * 1024 * 1024, 1);
    auto d2 = random_data(3 * 1024 * 1024, 2);

    auto r1 = cluster.client().put_data("/test/overwrite.bin", d1.data(), d1.size());
    assert(r1.success);
    assert(r1.version == 1);

    auto r2 = cluster.client().put_data("/test/overwrite.bin", d2.data(), d2.size());
    assert(r2.success);
    assert(r2.version == 2);

    auto down = cluster.client().get_data("/test/overwrite.bin");
    assert(down.success);
    assert(down.data.size() == d2.size());
    assert(down.data == d2);

    std::cout << "  [PASS] File overwrite with versioning\n";
}

void test_delete() {
    TestCluster cluster;

    auto d = random_data(1024);
    cluster.client().put_data("/test/deleteme.txt", d.data(), d.size());

    auto info = cluster.client().info("/test/deleteme.txt");
    assert(info.has_value());

    cluster.client().remove("/test/deleteme.txt");

    info = cluster.client().info("/test/deleteme.txt");
    assert(!info.has_value());

    std::cout << "  [PASS] File deletion\n";
}

void test_small_file() {
    TestCluster cluster;

    std::vector<uint8_t> tiny = {72, 101, 108, 108, 111}; // "Hello"
    auto up = cluster.client().put_data("/test/tiny.txt", tiny.data(), tiny.size());
    assert(up.success);
    assert(up.chunk_count == 1);

    auto down = cluster.client().get_data("/test/tiny.txt");
    assert(down.success);
    assert(down.data == tiny);

    std::cout << "  [PASS] Small file (5 bytes, 1 chunk)\n";
}

void test_parallel_vs_sequential() {
    TestCluster cluster(5, 256 * 1024); // 256 KB chunks for more chunks per file

    auto data = random_data(8 * 1024 * 1024); // 8 MB = 32 chunks
    cluster.client().put_data("/test/parallel_test.bin", data.data(), data.size());

    auto par = cluster.client().get_data("/test/parallel_test.bin");
    assert(par.success);
    assert(par.data == data);

    auto seq = cluster.client().get_data_sequential("/test/parallel_test.bin");
    assert(seq.success);
    assert(seq.data == data);

    double improvement = ((seq.elapsed_ms - par.elapsed_ms) / seq.elapsed_ms) * 100;

    std::cout << "  [PASS] Parallel vs Sequential: par=" << par.elapsed_ms
              << "ms, seq=" << seq.elapsed_ms << "ms, improvement=" << improvement << "%\n";
}

void test_concurrent_uploads() {
    TestCluster cluster;

    int num_clients = 5;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < num_clients; i++) {
        threads.emplace_back([&cluster, &success_count, i]() {
            auto data = random_data(2 * 1024 * 1024, i);
            std::string path = "/concurrent/file_" + std::to_string(i);
            auto r = cluster.client().put_data(path, data.data(), data.size());
            if (r.success) success_count++;
        });
    }

    for (auto& t : threads) t.join();
    assert(success_count == num_clients);

    auto files = cluster.client().list("/concurrent/");
    assert(static_cast<int>(files.size()) == num_clients);

    std::cout << "  [PASS] Concurrent uploads: " << success_count << "/" << num_clients << "\n";
}

void test_node_failure_recovery() {
    TestCluster cluster;

    auto data = random_data(3 * 1024 * 1024);
    cluster.client().put_data("/recovery/test.bin", data.data(), data.size());

    cluster.nodes()[0]->stop();
    cluster.coordinator().trigger_recovery("node_0");

    auto result = cluster.client().get_data("/recovery/test.bin");
    assert(result.success);
    assert(result.data == data);

    std::cout << "  [PASS] Node failure recovery, data still readable\n";
}

void test_metadata_consistency() {
    TestCluster cluster;

    auto data = random_data(4 * 1024 * 1024);
    auto up = cluster.client().put_data("/meta/test.bin", data.data(), data.size());
    assert(up.success);

    auto info = cluster.client().info("/meta/test.bin");
    assert(info.has_value());
    assert(info->file_size == data.size());
    assert(info->chunk_count == 4);
    assert(info->version == 1);

    std::cout << "  [PASS] Metadata consistency check\n";
}

int main() {
    Logger::instance().set_level(LogLevel::WARN);

    std::cout << "=== Integration Tests ===\n";
    test_upload_download();
    test_file_listing();
    test_file_overwrite();
    test_delete();
    test_small_file();
    test_parallel_vs_sequential();
    test_concurrent_uploads();
    test_node_failure_recovery();
    test_metadata_consistency();
    std::cout << "All integration tests passed.\n\n";
    return 0;
}
