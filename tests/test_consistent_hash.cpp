#include "hashing/consistent_hash_ring.hpp"
#include <cassert>
#include <iostream>
#include <map>
#include <cmath>

using namespace scdfs;

void test_add_remove_nodes() {
    ConsistentHashRing ring(64);
    ring.add_node("node_a");
    ring.add_node("node_b");
    ring.add_node("node_c");
    assert(ring.node_count() == 3);

    ring.remove_node("node_b");
    assert(ring.node_count() == 2);

    // Re-add
    ring.add_node("node_b");
    assert(ring.node_count() == 3);

    std::cout << "  [PASS] Add/remove nodes\n";
}

void test_consistent_routing() {
    ConsistentHashRing ring(64);
    ring.add_node("node_a");
    ring.add_node("node_b");
    ring.add_node("node_c");

    // Same key should always route to same node
    auto n1 = ring.get_node("my_chunk_001");
    auto n2 = ring.get_node("my_chunk_001");
    assert(n1 == n2);

    std::cout << "  [PASS] Consistent routing\n";
}

void test_minimal_disruption() {
    ConsistentHashRing ring(128);
    ring.add_node("node_a");
    ring.add_node("node_b");
    ring.add_node("node_c");

    // Record mappings
    std::map<std::string, std::string> before;
    int num_keys = 10000;
    for (int i = 0; i < num_keys; i++) {
        std::string key = "key_" + std::to_string(i);
        before[key] = ring.get_node(key);
    }

    // Add a new node
    ring.add_node("node_d");

    int changed = 0;
    for (int i = 0; i < num_keys; i++) {
        std::string key = "key_" + std::to_string(i);
        if (ring.get_node(key) != before[key]) changed++;
    }

    // With consistent hashing, roughly 1/N of keys should move
    double expected = num_keys / 4.0; // 4 nodes
    double ratio = static_cast<double>(changed) / expected;

    std::cout << "  [PASS] Minimal disruption: " << changed << "/" << num_keys
              << " keys moved (expected ~" << static_cast<int>(expected)
              << ", ratio=" << ratio << ")\n";

    assert(ratio < 2.0); // Should not move more than 2x the expected amount
}

void test_replica_placement() {
    ConsistentHashRing ring(64);
    ring.add_node("node_a");
    ring.add_node("node_b");
    ring.add_node("node_c");
    ring.add_node("node_d");
    ring.add_node("node_e");

    auto replicas = ring.get_nodes("test_chunk", 3);
    assert(replicas.size() == 3);

    // All replicas should be distinct
    assert(replicas[0] != replicas[1]);
    assert(replicas[1] != replicas[2]);
    assert(replicas[0] != replicas[2]);

    std::cout << "  [PASS] Replica placement: " << replicas[0] << ", "
              << replicas[1] << ", " << replicas[2] << "\n";
}

void test_load_balance() {
    ConsistentHashRing ring(128);
    int num_nodes = 5;
    for (int i = 0; i < num_nodes; i++) {
        ring.add_node("node_" + std::to_string(i));
    }

    std::map<std::string, int> counts;
    int num_keys = 100000;
    for (int i = 0; i < num_keys; i++) {
        std::string node = ring.get_node("chunk_" + std::to_string(i));
        counts[node]++;
    }

    double expected = static_cast<double>(num_keys) / num_nodes;
    double max_deviation = 0;

    for (const auto& [node, count] : counts) {
        double deviation = std::abs(count - expected) / expected * 100;
        max_deviation = std::max(max_deviation, deviation);
    }

    std::cout << "  [PASS] Load balance (max deviation " << max_deviation << "%):";
    for (const auto& [node, count] : counts) {
        std::cout << " " << node << "=" << count;
    }
    std::cout << "\n";

    // With 128 vnodes, deviation should be under 30%
    assert(max_deviation < 30.0);
}

void test_more_replicas_than_nodes() {
    ConsistentHashRing ring(64);
    ring.add_node("node_a");
    ring.add_node("node_b");

    // Requesting 5 replicas when only 2 nodes exist
    auto replicas = ring.get_nodes("test_key", 5);
    assert(replicas.size() == 2);

    std::cout << "  [PASS] Replica count capped by available nodes\n";
}

int main() {
    std::cout << "=== Consistent Hash Ring Tests ===\n";
    test_add_remove_nodes();
    test_consistent_routing();
    test_minimal_disruption();
    test_replica_placement();
    test_load_balance();
    test_more_replicas_than_nodes();
    std::cout << "All consistent hash tests passed.\n\n";
    return 0;
}
