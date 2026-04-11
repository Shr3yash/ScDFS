#pragma once

#include <string>
#include <functional>
#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include "common/types.hpp"
#include "threading/thread_pool.hpp"

namespace scdfs {

// Callback invoked for each received message; returns a response message.
using MessageHandler = std::function<WireMessage(const WireMessage&, int client_fd)>;

class TcpServer {
public:
    TcpServer(const std::string& bind_addr, uint16_t port, int thread_count = 4);
    ~TcpServer();

    void set_handler(MessageHandler handler);
    void start();
    void stop();
    bool is_running() const { return running_; }

private:
    std::string bind_addr_;
    uint16_t port_;
    int server_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
    std::unique_ptr<ThreadPool> pool_;
    MessageHandler handler_;
    int thread_count_;

    void accept_loop();
    void handle_client(int client_fd);

    static bool send_message(int fd, const WireMessage& msg);
    static bool recv_message(int fd, WireMessage& msg);
};

} // namespace scdfs
