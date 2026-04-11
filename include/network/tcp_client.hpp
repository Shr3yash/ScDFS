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

    // Send a message and receive a response (synchronous).
    bool send_receive(const WireMessage& request, WireMessage& response);

    // Send raw data for streaming chunk transfers.
    bool send_raw(const uint8_t* data, size_t len);
    bool recv_raw(uint8_t* data, size_t len);

    static bool send_message(int fd, const WireMessage& msg);
    static bool recv_message(int fd, WireMessage& msg);

private:
    int fd_ = -1;
    std::mutex mutex_;
};

} // namespace scdfs
