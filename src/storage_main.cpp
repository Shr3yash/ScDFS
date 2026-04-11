#include "storage/storage_node.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"

#include <iostream>
#include <csignal>
#include <atomic>

static std::atomic<bool> g_running{true};

void signal_handler(int) {
    g_running = false;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <address> <port> [data_dir]\n";
        std::cerr << "Example: " << argv[0] << " node1 127.0.0.1 9201 ./data\n";
        return 1;
    }

    std::string node_id = argv[1];
    std::string address = argv[2];
    uint16_t port = static_cast<uint16_t>(std::stoi(argv[3]));

    scdfs::Config config;
    if (argc > 4) {
        config.data_directory = argv[4];
    }

    scdfs::Logger::instance().set_level(scdfs::LogLevel::INFO);

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    scdfs::StorageNode node(node_id, address, port, config);
    node.start();

    std::cout << "Storage node '" << node_id << "' running on "
              << address << ":" << port << "\n"
              << "Press Ctrl+C to stop.\n";

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    node.stop();
    return 0;
}
