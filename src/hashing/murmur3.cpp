#include "hashing/murmur3.hpp"
#include <cstring>
#include <sstream>
#include <iomanip>

namespace scdfs {

uint64_t Murmur3::fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

Murmur3::Hash128 Murmur3::hash128(const void* key, size_t len, uint32_t seed) {
    const auto* data = static_cast<const uint8_t*>(key);
    const size_t nblocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    const uint64_t c1 = 0x87c37b91114253d5ULL;
    const uint64_t c2 = 0x4cf5ad432745937fULL;

    const auto* blocks = reinterpret_cast<const uint64_t*>(data);
    for (size_t i = 0; i < nblocks; i++) {
        uint64_t k1, k2;
        std::memcpy(&k1, &blocks[i * 2], sizeof(uint64_t));
        std::memcpy(&k2, &blocks[i * 2 + 1], sizeof(uint64_t));

        k1 *= c1; k1 = rotl64(k1, 31); k1 *= c2; h1 ^= k1;
        h1 = rotl64(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;

        k2 *= c2; k2 = rotl64(k2, 33); k2 *= c1; h2 ^= k2;
        h2 = rotl64(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
    }

    const uint8_t* tail = data + nblocks * 16;

    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15) {
        case 15: k2 ^= static_cast<uint64_t>(tail[14]) << 48; [[fallthrough]];
        case 14: k2 ^= static_cast<uint64_t>(tail[13]) << 40; [[fallthrough]];
        case 13: k2 ^= static_cast<uint64_t>(tail[12]) << 32; [[fallthrough]];
        case 12: k2 ^= static_cast<uint64_t>(tail[11]) << 24; [[fallthrough]];
        case 11: k2 ^= static_cast<uint64_t>(tail[10]) << 16; [[fallthrough]];
        case 10: k2 ^= static_cast<uint64_t>(tail[ 9]) << 8;  [[fallthrough]];
        case  9: k2 ^= static_cast<uint64_t>(tail[ 8]) << 0;
                 k2 *= c2; k2 = rotl64(k2, 33); k2 *= c1; h2 ^= k2;
                 [[fallthrough]];
        case  8: k1 ^= static_cast<uint64_t>(tail[ 7]) << 56; [[fallthrough]];
        case  7: k1 ^= static_cast<uint64_t>(tail[ 6]) << 48; [[fallthrough]];
        case  6: k1 ^= static_cast<uint64_t>(tail[ 5]) << 40; [[fallthrough]];
        case  5: k1 ^= static_cast<uint64_t>(tail[ 4]) << 32; [[fallthrough]];
        case  4: k1 ^= static_cast<uint64_t>(tail[ 3]) << 24; [[fallthrough]];
        case  3: k1 ^= static_cast<uint64_t>(tail[ 2]) << 16; [[fallthrough]];
        case  2: k1 ^= static_cast<uint64_t>(tail[ 1]) << 8;  [[fallthrough]];
        case  1: k1 ^= static_cast<uint64_t>(tail[ 0]) << 0;
                 k1 *= c1; k1 = rotl64(k1, 31); k1 *= c2; h1 ^= k1;
    }

    h1 ^= len; h2 ^= len;
    h1 += h2;  h2 += h1;
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 += h2;  h2 += h1;

    return {h1, h2};
}

Murmur3::Hash128 Murmur3::hash128(const std::string& key, uint32_t seed) {
    return hash128(key.data(), key.size(), seed);
}

int64_t Murmur3::hash_to_token(const std::string& key, uint32_t seed) {
    auto h = hash128(key, seed);
    return static_cast<int64_t>(h.h1);
}

std::string Murmur3::hash_to_hex(const std::string& key, uint32_t seed) {
    auto h = hash128(key, seed);
    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(16) << h.h1
        << std::setw(16) << h.h2;
    return oss.str();
}

} // namespace scdfs
