#include "network/tcp_client.hpp"
#include "common/logger.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cstring>
#include <cerrno>

namespace scdfs {

TcpClient::~TcpClient() {
    disconnect();
}

bool TcpClient::connect(const std::string& host, uint16_t port, int timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex_);

    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        LOG_ERROR("Socket creation failed: ", strerror(errno));
        return false;
    }

    // Non-blocking connect with timeout
    int flags = fcntl(fd_, F_GETFL, 0);
    fcntl(fd_, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    int ret = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        LOG_ERROR("Connect failed to ", host, ":", port, " - ", strerror(errno));
        close(fd_);
        fd_ = -1;
        return false;
    }

    if (ret < 0) {
        pollfd pfd{fd_, POLLOUT, 0};
        int poll_ret = poll(&pfd, 1, timeout_ms);
        if (poll_ret <= 0) {
            LOG_ERROR("Connect timeout to ", host, ":", port);
            close(fd_);
            fd_ = -1;
            return false;
        }

        int err = 0;
        socklen_t len = sizeof(err);
        getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err != 0) {
            LOG_ERROR("Connect error to ", host, ":", port, " - ", strerror(err));
            close(fd_);
            fd_ = -1;
            return false;
        }
    }

    // Restore blocking mode
    fcntl(fd_, F_SETFL, flags);

    LOG_DEBUG("Connected to ", host, ":", port);
    return true;
}

void TcpClient::disconnect() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

bool TcpClient::send_receive(const WireMessage& request, WireMessage& response) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0) return false;
    if (!send_message(fd_, request)) return false;
    return recv_message(fd_, response);
}

bool TcpClient::send_raw(const uint8_t* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd_, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += n;
    }
    return true;
}

bool TcpClient::recv_raw(uint8_t* data, size_t len) {
    size_t received = 0;
    while (received < len) {
        ssize_t n = ::recv(fd_, data + received, len - received, 0);
        if (n <= 0) return false;
        received += n;
    }
    return true;
}

bool TcpClient::send_message(int fd, const WireMessage& msg) {
    uint8_t type = static_cast<uint8_t>(msg.type);
    uint32_t rid = msg.request_id;
    uint32_t psize = static_cast<uint32_t>(msg.payload.size());

    if (::send(fd, &type, 1, MSG_NOSIGNAL) != 1) return false;
    if (::send(fd, &rid, 4, MSG_NOSIGNAL) != 4) return false;
    if (::send(fd, &psize, 4, MSG_NOSIGNAL) != 4) return false;

    if (psize > 0) {
        size_t sent = 0;
        while (sent < psize) {
            ssize_t n = ::send(fd, msg.payload.data() + sent, psize - sent, MSG_NOSIGNAL);
            if (n <= 0) return false;
            sent += n;
        }
    }
    return true;
}

bool TcpClient::recv_message(int fd, WireMessage& msg) {
    auto recv_exact = [](int fd, void* buf, size_t len) -> bool {
        size_t received = 0;
        while (received < len) {
            ssize_t n = ::recv(fd, static_cast<uint8_t*>(buf) + received, len - received, 0);
            if (n <= 0) return false;
            received += n;
        }
        return true;
    };

    uint8_t type;
    uint32_t rid, psize;

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
