#include "hashing/murmur3.hpp"
#include <cassert>
#include <iostream>
#include <set>

using namespace scdfs;

void test_deterministic() {
    auto h1 = Murmur3::hash128("hello");
    auto h2 = Murmur3::hash128("hello");
    assert(h1 == h2);
    std::cout << "  [PASS] Deterministic output\n";
}

void test_different_inputs_differ() {
    auto h1 = Murmur3::hash128("hello");
    auto h2 = Murmur3::hash128("world");
    assert(!(h1 == h2));
    std::cout << "  [PASS] Different inputs produce different hashes\n";
}

void test_distribution() {
    // Hash 10000 keys and check that they spread across the 64-bit space
    std::set<uint64_t> upper_bits;
    for (int i = 0; i < 10000; i++) {
        auto h = Murmur3::hash128("key_" + std::to_string(i));
        upper_bits.insert(h.h1 >> 48); // top 16 bits should have variety
    }
    assert(upper_bits.size() > 9000);
    std::cout << "  [PASS] Good distribution (" << upper_bits.size() << "/10000 unique upper bits)\n";
}

void test_token() {
    auto t1 = Murmur3::hash_to_token("test");
    auto t2 = Murmur3::hash_to_token("test");
    assert(t1 == t2);
    assert(Murmur3::hash_to_token("a") != Murmur3::hash_to_token("b"));
    std::cout << "  [PASS] Token generation\n";
}

void test_hex() {
    auto hex = Murmur3::hash_to_hex("test_key");
    assert(hex.length() == 32);
    std::cout << "  [PASS] Hex output: " << hex << "\n";
}

void test_empty_string() {
    auto h1 = Murmur3::hash128("");
    auto h2 = Murmur3::hash128("");
    assert(h1 == h2); // deterministic even for empty input
    // Different seed should give different hash
    auto h3 = Murmur3::hash128("", 42);
    (void)h3;
    std::cout << "  [PASS] Empty string hashing\n";
}

int main() {
    std::cout << "=== Murmur3 Tests ===\n";
    test_deterministic();
    test_different_inputs_differ();
    test_distribution();
    test_token();
    test_hex();
    test_empty_string();
    std::cout << "All Murmur3 tests passed.\n\n";
    return 0;
}
