#include "coordinator/coordinator.hpp"
#include "storage/storage_node.hpp"
#include "client/scdfs_client.hpp"
#include "common/logger.hpp"

#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <random>
#include <algorithm>
#include <numeric>
#include <iomanip>
#include <cmath>

using namespace scdfs;

struct BenchResult {
    double total_ms;
    double throughput_mbps;
    std::vector<double> latencies_ms;

    double avg_latency() const {
        return std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0) / latencies_ms.size();
    }

    double p95_latency() const {
        if (latencies_ms.empty()) return 0;
        auto sorted = latencies_ms;
        std::sort(sorted.begin(), sorted.end());
        return sorted[static_cast<size_t>(sorted.size() * 0.95)];
    }

    double p99_latency() const {
        if (latencies_ms.empty()) return 0;
        auto sorted = latencies_ms;
        std::sort(sorted.begin(), sorted.end());
        return sorted[static_cast<size_t>(sorted.size() * 0.99)];
    }
};

std::vector<uint8_t> generate_random_data(size_t size) {
    std::vector<uint8_t> data(size);
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint8_t> dist(0, 255);
    for (auto& b : data) b = dist(rng);
    return data;
}

void print_separator() {
    std::cout << std::string(80, '=') << "\n";
}

void print_result(const std::string& label, const BenchResult& r) {
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  " << std::setw(30) << std::left << label
              << "  Total: " << std::setw(10) << r.total_ms << " ms"
              << "  Throughput: " << std::setw(10) << r.throughput_mbps << " MB/s"
              << "  Avg lat: " << std::setw(10) << r.avg_latency() << " ms"
              << "  p95: " << std::setw(10) << r.p95_latency() << " ms"
              << "\n";
}

BenchResult benchmark_upload(ScDFSClient& client, const std::string& prefix,
                              const uint8_t* data, size_t size, int num_files) {
    BenchResult result;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_files; i++) {
        std::string path = prefix + "/file_" + std::to_string(i);
        auto t0 = std::chrono::high_resolution_clock::now();

        auto r = client.put_data(path, data, size);

        auto t1 = std::chrono::high_resolution_clock::now();
        double latency = std::chrono::duration<double, std::milli>(t1 - t0).count();
        result.latencies_ms.push_back(latency);
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.total_ms = std::chrono::duration<double, std::milli>(end - start).count();

    double total_mb = (static_cast<double>(size) * num_files) / (1024.0 * 1024.0);
    result.throughput_mbps = total_mb / (result.total_ms / 1000.0);

    return result;
}

BenchResult benchmark_download_parallel(ScDFSClient& client, const std::string& prefix,
                                         int num_files) {
    BenchResult result;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_files; i++) {
        std::string path = prefix + "/file_" + std::to_string(i);
        auto t0 = std::chrono::high_resolution_clock::now();

        auto r = client.get_data(path);

        auto t1 = std::chrono::high_resolution_clock::now();
        double latency = std::chrono::duration<double, std::milli>(t1 - t0).count();
        result.latencies_ms.push_back(latency);
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.total_ms = std::chrono::duration<double, std::milli>(end - start).count();

    double total_bytes = 0;
    auto files = client.list(prefix);
    for (const auto& f : files) total_bytes += f.file_size;
    result.throughput_mbps = (total_bytes / (1024.0 * 1024.0)) / (result.total_ms / 1000.0);

    return result;
}

BenchResult benchmark_download_sequential(ScDFSClient& client, const std::string& prefix,
                                            int num_files) {
    BenchResult result;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_files; i++) {
        std::string path = prefix + "/file_" + std::to_string(i);
        auto t0 = std::chrono::high_resolution_clock::now();

        auto r = client.get_data_sequential(path);

        auto t1 = std::chrono::high_resolution_clock::now();
        double latency = std::chrono::duration<double, std::milli>(t1 - t0).count();
        result.latencies_ms.push_back(latency);
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.total_ms = std::chrono::duration<double, std::milli>(end - start).count();

    double total_bytes = 0;
    auto files = client.list(prefix);
    for (const auto& f : files) total_bytes += f.file_size;
    result.throughput_mbps = (total_bytes / (1024.0 * 1024.0)) / (result.total_ms / 1000.0);

    return result;
}

BenchResult benchmark_concurrent_clients(std::shared_ptr<Coordinator> coord,
                                          int num_clients, size_t file_size) {
    BenchResult result;

    auto data = generate_random_data(file_size);
    std::vector<std::thread> threads;
    std::mutex latency_mutex;

    auto start = std::chrono::high_resolution_clock::now();

    for (int c = 0; c < num_clients; c++) {
        threads.emplace_back([&, c]() {
            ScDFSClient client(coord);
            std::string path = "/concurrent/client_" + std::to_string(c);

            auto t0 = std::chrono::high_resolution_clock::now();
            client.put_data(path, data.data(), data.size());
            auto t1 = std::chrono::high_resolution_clock::now();

            double latency = std::chrono::duration<double, std::milli>(t1 - t0).count();
            std::lock_guard<std::mutex> lock(latency_mutex);
            result.latencies_ms.push_back(latency);
        });
    }

    for (auto& t : threads) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    result.total_ms = std::chrono::duration<double, std::milli>(end - start).count();

    double total_mb = (static_cast<double>(file_size) * num_clients) / (1024.0 * 1024.0);
    result.throughput_mbps = total_mb / (result.total_ms / 1000.0);

    return result;
}

int main(int argc, char** argv) {
    Logger::instance().set_level(LogLevel::WARN);

    size_t chunk_size = 1 * 1024 * 1024; // small, so a 32 MB file is many chunks
    int num_nodes = 5;
    int thread_pool_size = 8;

    if (argc > 1) chunk_size = std::stoull(argv[1]);
    if (argc > 2) num_nodes = std::stoi(argv[2]);
    if (argc > 3) thread_pool_size = std::stoi(argv[3]);

    std::cout << "\n";
    print_separator();
    std::cout << "  ScDFS THROUGHPUT BENCHMARK\n";
    print_separator();
    std::cout << "  Chunk size:    " << chunk_size / 1024 << " KB\n"
              << "  Storage nodes: " << num_nodes << "\n"
              << "  Thread pool:   " << thread_pool_size << "\n"
              << "  Replication:   3x\n";
    print_separator();

    Config config;
    config.chunk_size = chunk_size;
    config.thread_pool_size = thread_pool_size;
    config.replication_factor = 3;
    config.write_quorum = 2;

    auto coordinator = std::make_shared<Coordinator>(config);

    std::vector<std::unique_ptr<StorageNode>> storage_nodes;
    for (int i = 0; i < num_nodes; i++) {
        std::string node_id = "node_" + std::to_string(i);
        uint16_t port = DEFAULT_STORAGE_PORT_BASE + i;
        std::string data_dir = "/tmp/scdfs_bench/node_" + std::to_string(i);

        auto node = std::make_unique<StorageNode>(node_id, "127.0.0.1", port, config);
        node->start();
        storage_nodes.push_back(std::move(node));

        coordinator->register_storage_node(node_id, "127.0.0.1", port);
    }

    coordinator->start();
    ScDFSClient client(coordinator);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cout << "\n[1] UPLOAD THROUGHPUT\n";
    {
        size_t file_size = 32 * 1024 * 1024; // 32 MB
        auto data = generate_random_data(file_size);

        auto result = benchmark_upload(client, "/bench/upload", data.data(), data.size(), 5);
        print_result("32 MB x 5 files", result);
    }

    std::cout << "\n[2] DOWNLOAD: PARALLEL vs SEQUENTIAL\n";
    {
        size_t file_size = 32 * 1024 * 1024; // 32 MB = 32 chunks at 1MB each
        auto data = generate_random_data(file_size);

        for (int i = 0; i < 3; i++) {
            client.put_data("/bench/download/file_" + std::to_string(i), data.data(), data.size());
        }

        auto parallel = benchmark_download_parallel(client, "/bench/download", 3);
        auto sequential = benchmark_download_sequential(client, "/bench/download", 3);

        print_result("Parallel (thread pool)", parallel);
        print_result("Sequential", sequential);

        double improvement = ((sequential.total_ms - parallel.total_ms) / sequential.total_ms) * 100;
        std::cout << "\n  >> Parallel is " << std::fixed << std::setprecision(1)
                  << improvement << "% faster than sequential\n";
    }

    std::cout << "\n[3] CONCURRENT CLIENT UPLOADS\n";
    {
        size_t file_size = 16 * 1024 * 1024; // 16 MB

        for (int clients : {1, 3, 5, 10}) {
            auto result = benchmark_concurrent_clients(coordinator, clients, file_size);
            std::string label = std::to_string(clients) + " concurrent clients";
            print_result(label, result);
        }
    }

    std::cout << "\n[4] VARYING FILE SIZES\n";
    {
        for (size_t mb : {1, 8, 32, 64, 128}) {
            size_t file_size = mb * 1024 * 1024;
            auto data = generate_random_data(file_size);
            std::string path = "/bench/size/file_" + std::to_string(mb) + "mb";

            auto t0 = std::chrono::high_resolution_clock::now();
            auto r = client.put_data(path, data.data(), data.size());
            auto t1 = std::chrono::high_resolution_clock::now();

            double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
            double mbps = (static_cast<double>(file_size) / (1024.0 * 1024.0)) / (ms / 1000.0);

            std::cout << "  " << std::setw(6) << mb << " MB:  "
                      << std::setw(10) << std::fixed << std::setprecision(2) << ms << " ms  "
                      << std::setw(10) << mbps << " MB/s  "
                      << r.chunk_count << " chunks\n";
        }
    }

    std::cout << "\n[5] FAILURE RECOVERY\n";
    {
        size_t file_size = 16 * 1024 * 1024;
        auto data = generate_random_data(file_size);
        client.put_data("/bench/recovery/testfile", data.data(), data.size());

        auto t0 = std::chrono::high_resolution_clock::now();

        storage_nodes[0]->stop();
        coordinator->trigger_recovery("node_0");

        auto t1 = std::chrono::high_resolution_clock::now();
        double recovery_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        std::cout << "  Recovery time after node failure: " << std::fixed
                  << std::setprecision(2) << recovery_ms << " ms\n";
        std::cout << "  Chunks recovered: " << coordinator->recovery().chunks_recovered() << "\n";

        auto result = client.get_data("/bench/recovery/testfile");
        std::cout << "  Data accessible after recovery: " << (result.success ? "YES" : "NO") << "\n";
        if (result.success) {
            bool match = (result.data.size() == data.size()) &&
                         std::equal(result.data.begin(), result.data.end(), data.begin());
            std::cout << "  Data integrity check: " << (match ? "PASS" : "FAIL") << "\n";
        }
    }

    print_separator();
    std::cout << "  BENCHMARK COMPLETE\n";
    print_separator();

    coordinator->stop();
    for (auto& node : storage_nodes) {
        node->stop();
    }

    return 0;
}
