#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <array>

namespace scdfs {

// MurmurHash3 128-bit for x64 — same algorithm Cassandra uses as its default partitioner.
class Murmur3 {
public:
    struct Hash128 {
        uint64_t h1;
        uint64_t h2;

        bool operator==(const Hash128& o) const { return h1 == o.h1 && h2 == o.h2; }
        bool operator<(const Hash128& o) const {
            return h1 < o.h1 || (h1 == o.h1 && h2 < o.h2);
        }
    };

    static Hash128 hash128(const void* key, size_t len, uint32_t seed = 0);
    static Hash128 hash128(const std::string& key, uint32_t seed = 0);

    // Returns the upper 64 bits, matching Cassandra's token computation.
    static int64_t hash_to_token(const std::string& key, uint32_t seed = 0);

    static std::string hash_to_hex(const std::string& key, uint32_t seed = 0);

private:
    static uint64_t rotl64(uint64_t x, int8_t r) { return (x << r) | (x >> (64 - r)); }
    static uint64_t fmix64(uint64_t k);
};

} // namespace scdfs
