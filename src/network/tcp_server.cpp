#include "network/tcp_server.hpp"
#include "common/logger.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>

namespace scdfs {

TcpServer::TcpServer(const std::string& bind_addr, uint16_t port, int thread_count)
    : bind_addr_(bind_addr), port_(port), thread_count_(thread_count) {}

TcpServer::~TcpServer() {
    stop();
}

void TcpServer::set_handler(MessageHandler handler) {
    handler_ = std::move(handler);
}

void TcpServer::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        LOG_ERROR("Failed to create socket: ", strerror(errno));
        return;
    }

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    inet_pton(AF_INET, bind_addr_.c_str(), &addr.sin_addr);

    if (bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        LOG_ERROR("Bind failed on ", bind_addr_, ":", port_, " - ", strerror(errno));
        close(server_fd_);
        server_fd_ = -1;
        return;
    }

    if (listen(server_fd_, 128) < 0) {
        LOG_ERROR("Listen failed: ", strerror(errno));
        close(server_fd_);
        server_fd_ = -1;
        return;
    }

    pool_ = std::make_unique<ThreadPool>(thread_count_);
    running_ = true;

    LOG_INFO("TCP server listening on ", bind_addr_, ":", port_);
    accept_thread_ = std::thread(&TcpServer::accept_loop, this);
}

void TcpServer::stop() {
    if (!running_) return;
    running_ = false;

    if (server_fd_ >= 0) {
        ::shutdown(server_fd_, SHUT_RDWR);
        close(server_fd_);
        server_fd_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    pool_.reset();
    LOG_INFO("TCP server stopped");
}

void TcpServer::accept_loop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd_, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);

        if (client_fd < 0) {
            if (running_) LOG_WARN("Accept failed: ", strerror(errno));
            continue;
        }

        pool_->enqueue([this, client_fd] {
            handle_client(client_fd);
        });
    }
}

void TcpServer::handle_client(int client_fd) {
    while (running_) {
        WireMessage request;
        if (!recv_message(client_fd, request)) break;

        if (handler_) {
            WireMessage response = handler_(request, client_fd);
            if (!send_message(client_fd, response)) break;
        }
    }
    close(client_fd);
}

bool TcpServer::send_message(int fd, const WireMessage& msg) {
    // Wire format: [type:1][request_id:4][payload_size:4][payload:N]
    uint8_t type = static_cast<uint8_t>(msg.type);
    uint32_t rid = msg.request_id;
    uint32_t psize = static_cast<uint32_t>(msg.payload.size());

    if (send(fd, &type, 1, MSG_NOSIGNAL) != 1) return false;
    if (send(fd, &rid, 4, MSG_NOSIGNAL) != 4) return false;
    if (send(fd, &psize, 4, MSG_NOSIGNAL) != 4) return false;

    if (psize > 0) {
        size_t sent = 0;
        while (sent < psize) {
            ssize_t n = send(fd, msg.payload.data() + sent, psize - sent, MSG_NOSIGNAL);
            if (n <= 0) return false;
            sent += n;
        }
    }
    return true;
}

bool TcpServer::recv_message(int fd, WireMessage& msg) {
    uint8_t type;
    uint32_t rid, psize;

    auto recv_exact = [](int fd, void* buf, size_t len) -> bool {
        size_t received = 0;
        while (received < len) {
            ssize_t n = recv(fd, static_cast<uint8_t*>(buf) + received, len - received, 0);
            if (n <= 0) return false;
            received += n;
        }
        return true;
    };

    if (!recv_exact(fd, &type, 1)) return false;
    if (!recv_exact(fd, &rid, 4)) return false;
    if (!recv_exact(fd, &psize, 4)) return false;

    msg.type = static_cast<MessageType>(type);
    msg.request_id = rid;
    msg.payload.resize(psize);

    if (psize > 0) {
        if (!recv_exact(fd, msg.payload.data(), psize)) return false;
    }
    return true;
}

} // namespace scdfs
