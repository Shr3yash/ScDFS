#include "storage/chunk_store.hpp"
#include <cassert>
#include <iostream>
#include <filesystem>
#include <random>

namespace fs = std::filesystem;
using namespace scdfs;

void cleanup(const std::string& dir) {
    if (fs::exists(dir)) fs::remove_all(dir);
}

void test_store_and_read() {
    std::string dir = "/tmp/scdfs_test_store";
    cleanup(dir);

    ChunkStore store(dir);
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8};

    assert(store.store_chunk("chunk_001", data));
    assert(store.has_chunk("chunk_001"));

    std::vector<uint8_t> out;
    size_t size = store.read_chunk("chunk_001", out);
    assert(size == data.size());
    assert(out == data);

    std::cout << "  [PASS] Store and read\n";
    cleanup(dir);
}

void test_delete() {
    std::string dir = "/tmp/scdfs_test_delete";
    cleanup(dir);

    ChunkStore store(dir);
    std::vector<uint8_t> data = {10, 20, 30};

    store.store_chunk("chunk_del", data);
    assert(store.has_chunk("chunk_del"));

    assert(store.delete_chunk("chunk_del"));
    assert(!store.has_chunk("chunk_del"));
    assert(!store.delete_chunk("chunk_del")); // double delete

    std::cout << "  [PASS] Delete\n";
    cleanup(dir);
}

void test_list_chunks() {
    std::string dir = "/tmp/scdfs_test_list";
    cleanup(dir);

    ChunkStore store(dir);
    std::vector<uint8_t> data = {1};

    store.store_chunk("a", data);
    store.store_chunk("b", data);
    store.store_chunk("c", data);

    auto chunks = store.list_chunks();
    assert(chunks.size() == 3);
    assert(store.chunk_count() == 3);

    std::cout << "  [PASS] List chunks\n";
    cleanup(dir);
}

void test_large_chunk() {
    std::string dir = "/tmp/scdfs_test_large";
    cleanup(dir);

    ChunkStore store(dir);

    size_t size = 4 * 1024 * 1024; // 4 MB
    std::vector<uint8_t> data(size);
    std::mt19937 rng(42);
    for (auto& b : data) b = static_cast<uint8_t>(rng());

    assert(store.store_chunk("large_chunk", data));
    assert(store.chunk_size("large_chunk") == size);

    std::vector<uint8_t> out;
    store.read_chunk("large_chunk", out);
    assert(out == data);
    assert(store.total_stored_bytes() == size);

    std::cout << "  [PASS] Large chunk (4 MB)\n";
    cleanup(dir);
}

void test_checksum() {
    std::string dir = "/tmp/scdfs_test_checksum";
    cleanup(dir);

    ChunkStore store(dir);
    std::vector<uint8_t> data1 = {1, 2, 3};
    std::vector<uint8_t> data2 = {1, 2, 4};

    auto cs1 = store.compute_checksum(data1.data(), data1.size());
    auto cs2 = store.compute_checksum(data1.data(), data1.size());
    auto cs3 = store.compute_checksum(data2.data(), data2.size());

    assert(cs1 == cs2); // deterministic
    assert(cs1 != cs3); // different data

    std::cout << "  [PASS] Checksum: " << cs1 << "\n";
    cleanup(dir);
}

void test_nonexistent_chunk() {
    std::string dir = "/tmp/scdfs_test_noexist";
    cleanup(dir);

    ChunkStore store(dir);
    assert(!store.has_chunk("nonexistent"));
    assert(store.chunk_size("nonexistent") == 0);

    std::vector<uint8_t> out;
    assert(store.read_chunk("nonexistent", out) == 0);

    std::cout << "  [PASS] Nonexistent chunk handling\n";
    cleanup(dir);
}

int main() {
    std::cout << "=== Chunk Store Tests ===\n";
    test_store_and_read();
    test_delete();
    test_list_chunks();
    test_large_chunk();
    test_checksum();
    test_nonexistent_chunk();
    std::cout << "All chunk store tests passed.\n\n";
    return 0;
}
