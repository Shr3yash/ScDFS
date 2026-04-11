#pragma once

#include <map>
#include <vector>
#include <string>
#include <mutex>
#include <cstdint>
#include "common/types.hpp"

namespace scdfs {

// Consistent hash ring using Murmur3 with virtual nodes.
// Each physical node maps to `vnodes_per_node` positions on the ring,
// balancing load and minimizing key movement on membership changes.
class ConsistentHashRing {
public:
    explicit ConsistentHashRing(int vnodes_per_node = DEFAULT_VIRTUAL_NODES);

    void add_node(const NodeId& node_id);
    void remove_node(const NodeId& node_id);

    // Returns the primary node responsible for this key.
    NodeId get_node(const std::string& key) const;

    // Returns `count` distinct physical nodes walking clockwise from the key's position.
    // Used to determine replica placement.
    std::vector<NodeId> get_nodes(const std::string& key, int count) const;

    size_t node_count() const;
    std::vector<NodeId> all_nodes() const;
    bool has_node(const NodeId& node_id) const;

    // For debugging: returns the ring position for a key.
    uint64_t hash_key(const std::string& key) const;

private:
    int vnodes_per_node_;
    std::map<uint64_t, NodeId> ring_;              // hash position -> physical node
    std::map<NodeId, std::vector<uint64_t>> node_positions_; // physical node -> its vnode positions
    mutable std::mutex mutex_;

    uint64_t compute_hash(const std::string& key) const;
};

} // namespace scdfs
