#include "coordinator/coordinator.hpp"
#include "client/scdfs_client.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"

#include <iostream>
#include <csignal>
#include <atomic>
#include <sstream>

static std::atomic<bool> g_running{true};

void signal_handler(int) {
    g_running = false;
}

void print_usage() {
    std::cout << "ScDFS Coordinator — Interactive Shell\n"
              << "Commands:\n"
              << "  register <node_id> <address> <port>  — Register a storage node\n"
              << "  put <local_path> <remote_path>       — Upload a file\n"
              << "  get <remote_path> <local_path>       — Download a file\n"
              << "  rm <remote_path>                     — Delete a file\n"
              << "  ls [prefix]                          — List files\n"
              << "  info <remote_path>                   — File info\n"
              << "  recover <node_id>                    — Trigger recovery\n"
              << "  status                               — Cluster status\n"
              << "  quit                                 — Exit\n";
}

int main(int argc, char** argv) {
    scdfs::Config config;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--cassandra" && i + 1 < argc) {
            config.use_cassandra = true;
            config.cassandra_contact_points = argv[++i];
        } else if (arg == "--chunk-size" && i + 1 < argc) {
            config.chunk_size = std::stoull(argv[++i]);
        } else if (arg == "--threads" && i + 1 < argc) {
            config.thread_pool_size = std::stoi(argv[++i]);
        } else if (arg == "--replication" && i + 1 < argc) {
            config.replication_factor = std::stoi(argv[++i]);
        } else if (arg == "--data-dir" && i + 1 < argc) {
            config.data_directory = argv[++i];
        } else if (arg == "--debug") {
            scdfs::Logger::instance().set_level(scdfs::LogLevel::DEBUG);
        }
    }

    scdfs::Logger::instance().set_level(scdfs::LogLevel::INFO);

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    auto coordinator = std::make_shared<scdfs::Coordinator>(config);
    coordinator->start();

    scdfs::ScDFSClient client(coordinator);

    print_usage();
    std::cout << "\nscdfs> ";

    std::string line;
    while (g_running && std::getline(std::cin, line)) {
        std::istringstream iss(line);
        std::string cmd;
        if (!(iss >> cmd)) {
            std::cout << "scdfs> ";
            continue;
        }

        if (cmd == "register") {
            std::string node_id, address;
            uint16_t port;
            if (iss >> node_id >> address >> port) {
                if (coordinator->register_storage_node(node_id, address, port)) {
                    std::cout << "Registered node '" << node_id << "'\n";
                } else {
                    std::cout << "Failed to register node\n";
                }
            } else {
                std::cout << "Usage: register <node_id> <address> <port>\n";
            }
        } else if (cmd == "put") {
            std::string local_path, remote_path;
            if (iss >> local_path >> remote_path) {
                auto result = client.put(local_path, remote_path);
                if (result.success) {
                    std::cout << "Uploaded " << remote_path << " (" << result.file_size
                              << " bytes, " << result.chunk_count << " chunks, v"
                              << result.version << ") in " << result.elapsed_ms << "ms\n";
                } else {
                    std::cout << "Upload failed\n";
                }
            }
        } else if (cmd == "get") {
            std::string remote_path, local_path;
            if (iss >> remote_path >> local_path) {
                if (client.get(remote_path, local_path)) {
                    std::cout << "Downloaded " << remote_path << " -> " << local_path << "\n";
                } else {
                    std::cout << "Download failed\n";
                }
            }
        } else if (cmd == "rm") {
            std::string remote_path;
            if (iss >> remote_path) {
                client.remove(remote_path);
                std::cout << "Deleted " << remote_path << "\n";
            }
        } else if (cmd == "ls") {
            std::string prefix;
            iss >> prefix;
            auto files = client.list(prefix);
            for (const auto& f : files) {
                std::cout << "  " << f.file_path << "  " << f.file_size
                          << " bytes  v" << f.version << "  " << f.chunk_count << " chunks\n";
            }
            if (files.empty()) std::cout << "  (no files)\n";
        } else if (cmd == "info") {
            std::string remote_path;
            if (iss >> remote_path) {
                auto info = client.info(remote_path);
                if (info) {
                    std::cout << "  Path: " << info->file_path << "\n"
                              << "  Size: " << info->file_size << " bytes\n"
                              << "  Chunks: " << info->chunk_count << "\n"
                              << "  Version: " << info->version << "\n";
                } else {
                    std::cout << "File not found\n";
                }
            }
        } else if (cmd == "recover") {
            std::string node_id;
            if (iss >> node_id) {
                coordinator->trigger_recovery(node_id);
                std::cout << "Recovery triggered for " << node_id << "\n";
            }
        } else if (cmd == "status") {
            auto nodes = coordinator->metadata().get_all_nodes();
            std::cout << "Cluster status: " << nodes.size() << " nodes\n";
            for (const auto& n : nodes) {
                std::cout << "  " << n.node_id << " @ " << n.address << ":" << n.port
                          << "  [" << scdfs::to_string(n.status) << "]\n";
            }
            std::cout << "Ring: " << coordinator->ring().node_count() << " physical nodes\n";
        } else if (cmd == "quit" || cmd == "exit") {
            break;
        } else {
            std::cout << "Unknown command: " << cmd << "\n";
            print_usage();
        }

        std::cout << "scdfs> ";
    }

    coordinator->stop();
    return 0;
}
