#pragma once

#include <map>
#include <vector>
#include <string>
#include <mutex>
#include <cstdint>
#include "common/types.hpp"

namespace scdfs {

// Murmur3 ring. A physical node owns vnodes_per_node positions.
// get_nodes walks clockwise and skips a vnode whose node is already chosen.
class ConsistentHashRing {
public:
    explicit ConsistentHashRing(int vnodes_per_node = DEFAULT_VIRTUAL_NODES);

    void add_node(const NodeId& node_id);
    void remove_node(const NodeId& node_id);

    NodeId get_node(const std::string& key) const;
    std::vector<NodeId> get_nodes(const std::string& key, int count) const;

    size_t node_count() const;
    std::vector<NodeId> all_nodes() const;
    bool has_node(const NodeId& node_id) const;

private:
    int vnodes_per_node_;
    std::map<uint64_t, NodeId> ring_;              // hash position -> physical node
    std::map<NodeId, std::vector<uint64_t>> node_positions_; // physical node -> its vnode positions
    mutable std::mutex mutex_;

    uint64_t compute_hash(const std::string& key) const;
};

} // namespace scdfs
