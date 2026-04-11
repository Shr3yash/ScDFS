#include "hashing/consistent_hash_ring.hpp"
#include "hashing/murmur3.hpp"
#include "common/logger.hpp"
#include <algorithm>
#include <stdexcept>

namespace scdfs {

ConsistentHashRing::ConsistentHashRing(int vnodes_per_node)
    : vnodes_per_node_(vnodes_per_node) {}

uint64_t ConsistentHashRing::compute_hash(const std::string& key) const {
    auto h = Murmur3::hash128(key);
    return h.h1;
}

void ConsistentHashRing::add_node(const NodeId& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (node_positions_.count(node_id)) return;

    std::vector<uint64_t> positions;
    positions.reserve(vnodes_per_node_);

    for (int i = 0; i < vnodes_per_node_; i++) {
        std::string vnode_key = node_id + ":" + std::to_string(i);
        uint64_t pos = compute_hash(vnode_key);
        ring_[pos] = node_id;
        positions.push_back(pos);
    }

    node_positions_[node_id] = std::move(positions);
    LOG_INFO("Added node ", node_id, " with ", vnodes_per_node_, " vnodes, ring size=", ring_.size());
}

void ConsistentHashRing::remove_node(const NodeId& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = node_positions_.find(node_id);
    if (it == node_positions_.end()) return;

    for (uint64_t pos : it->second) {
        ring_.erase(pos);
    }
    node_positions_.erase(it);
    LOG_INFO("Removed node ", node_id, ", ring size=", ring_.size());
}

NodeId ConsistentHashRing::get_node(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (ring_.empty()) {
        throw std::runtime_error("ConsistentHashRing: no nodes in ring");
    }

    uint64_t hash = compute_hash(key);
    auto it = ring_.lower_bound(hash);
    if (it == ring_.end()) it = ring_.begin();
    return it->second;
}

std::vector<NodeId> ConsistentHashRing::get_nodes(const std::string& key, int count) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (ring_.empty()) {
        throw std::runtime_error("ConsistentHashRing: no nodes in ring");
    }

    int available = static_cast<int>(node_positions_.size());
    int n = std::min(count, available);

    std::vector<NodeId> result;
    result.reserve(n);

    uint64_t hash = compute_hash(key);
    auto it = ring_.lower_bound(hash);
    if (it == ring_.end()) it = ring_.begin();

    // Walk clockwise collecting distinct physical nodes.
    while (static_cast<int>(result.size()) < n) {
        const NodeId& node = it->second;
        if (std::find(result.begin(), result.end(), node) == result.end()) {
            result.push_back(node);
        }
        ++it;
        if (it == ring_.end()) it = ring_.begin();
    }

    return result;
}

size_t ConsistentHashRing::node_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return node_positions_.size();
}

std::vector<NodeId> ConsistentHashRing::all_nodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<NodeId> nodes;
    nodes.reserve(node_positions_.size());
    for (const auto& [id, _] : node_positions_) {
        nodes.push_back(id);
    }
    return nodes;
}

bool ConsistentHashRing::has_node(const NodeId& node_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return node_positions_.count(node_id) > 0;
}

uint64_t ConsistentHashRing::hash_key(const std::string& key) const {
    return compute_hash(key);
}

} // namespace scdfs
