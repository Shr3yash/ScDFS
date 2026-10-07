#pragma once

#include <cstdint>
#include <cstddef>
#include <string>

namespace scdfs {

// Austin Appleby's x64_128. hash_to_token is h1 as int64, which is what
// Cassandra's Murmur3Partitioner returns. Not checked against their Java code.
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

    static int64_t hash_to_token(const std::string& key, uint32_t seed = 0);

    static std::string hash_to_hex(const std::string& key, uint32_t seed = 0);

private:
    static uint64_t rotl64(uint64_t x, int8_t r) { return (x << r) | (x >> (64 - r)); }
    static uint64_t fmix64(uint64_t k);
};

} // namespace scdfs
