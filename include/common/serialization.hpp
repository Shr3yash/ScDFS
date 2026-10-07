#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>

namespace scdfs {

class ByteBuffer {
public:
    ByteBuffer() = default;
    explicit ByteBuffer(const std::vector<uint8_t>& data) : data_(data), pos_(0) {}
    explicit ByteBuffer(std::vector<uint8_t>&& data) : data_(std::move(data)), pos_(0) {}

    void write_u8(uint8_t val)   { data_.push_back(val); }
    void write_u16(uint16_t val) { append_bytes(&val, sizeof(val)); }
    void write_u32(uint32_t val) { append_bytes(&val, sizeof(val)); }
    void write_u64(uint64_t val) { append_bytes(&val, sizeof(val)); }

    void write_string(const std::string& s) {
        write_u32(static_cast<uint32_t>(s.size()));
        data_.insert(data_.end(), s.begin(), s.end());
    }

    void write_bytes(const uint8_t* buf, size_t len) {
        write_u32(static_cast<uint32_t>(len));
        data_.insert(data_.end(), buf, buf + len);
    }

    uint8_t read_u8()   { uint8_t v;  read_bytes(&v, sizeof(v)); return v; }
    uint16_t read_u16() { uint16_t v; read_bytes(&v, sizeof(v)); return v; }
    uint32_t read_u32() { uint32_t v; read_bytes(&v, sizeof(v)); return v; }
    uint64_t read_u64() { uint64_t v; read_bytes(&v, sizeof(v)); return v; }

    std::string read_string() {
        uint32_t len = read_u32();
        std::string s(data_.begin() + pos_, data_.begin() + pos_ + len);
        pos_ += len;
        return s;
    }

    std::vector<uint8_t> read_bytes_vec() {
        uint32_t len = read_u32();
        std::vector<uint8_t> v(data_.begin() + pos_, data_.begin() + pos_ + len);
        pos_ += len;
        return v;
    }

    size_t remaining() const { return data_.size() - pos_; }
    const std::vector<uint8_t>& data() const { return data_; }
    std::vector<uint8_t>& data() { return data_; }
    size_t size() const { return data_.size(); }
    void reset_read() { pos_ = 0; }

private:
    std::vector<uint8_t> data_;
    size_t pos_ = 0;

    void append_bytes(const void* ptr, size_t len) {
        auto* bytes = reinterpret_cast<const uint8_t*>(ptr);
        data_.insert(data_.end(), bytes, bytes + len);
    }

    void read_bytes(void* ptr, size_t len) {
        std::memcpy(ptr, data_.data() + pos_, len);
        pos_ += len;
    }
};

} // namespace scdfs
