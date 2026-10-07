#pragma once

#include <string>
#include <mutex>
#include <memory>
#include "common/types.hpp"

namespace scdfs {

class TcpClient {
public:
    TcpClient() = default;
    ~TcpClient();

    bool connect(const std::string& host, uint16_t port, int timeout_ms = 5000);
    void disconnect();
    bool is_connected() const { return fd_ >= 0; }

    bool send_receive(const WireMessage& request, WireMessage& response);

    static bool send_message(int fd, const WireMessage& msg);
    static bool recv_message(int fd, WireMessage& msg);

private:
    int fd_ = -1;
    std::mutex mutex_;
};

} // namespace scdfs
