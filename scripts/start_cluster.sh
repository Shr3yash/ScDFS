#!/usr/bin/env bash
set -euo pipefail

# Starts a local ScDFS cluster with multiple storage nodes and the coordinator.

NUM_NODES=${1:-5}
BASE_PORT=9200
DATA_DIR="/tmp/scdfs_cluster"

echo "=== Starting ScDFS Cluster ==="
echo "  Nodes: ${NUM_NODES}"
echo "  Data:  ${DATA_DIR}"
echo ""

# Build if needed
if [ ! -f build/scdfs_storage ]; then
    echo "Building ScDFS..."
    mkdir -p build && cd build
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu)
    cd ..
fi

# Clean up any previous cluster
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

PIDS=()

cleanup() {
    echo ""
    echo "Shutting down cluster..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null
    echo "Cluster stopped."
}
trap cleanup EXIT INT TERM

# Start storage nodes
for i in $(seq 0 $((NUM_NODES - 1))); do
    PORT=$((BASE_PORT + i))
    NODE_ID="node_${i}"
    NODE_DIR="${DATA_DIR}/${NODE_ID}"
    mkdir -p "${NODE_DIR}"

    echo "Starting storage node: ${NODE_ID} on port ${PORT}"
    ./build/scdfs_storage "${NODE_ID}" "127.0.0.1" "${PORT}" "${DATA_DIR}" &
    PIDS+=($!)
done

sleep 1
echo ""

# Start coordinator with auto-registration
echo "Starting coordinator..."
{
    for i in $(seq 0 $((NUM_NODES - 1))); do
        PORT=$((BASE_PORT + i))
        echo "register node_${i} 127.0.0.1 ${PORT}"
    done
    echo "status"
    # Keep stdin open
    cat
} | ./build/scdfs_coordinator &
PIDS+=($!)

echo ""
echo "=== Cluster Ready ==="
echo "  Storage nodes: ${NUM_NODES} (ports ${BASE_PORT}-$((BASE_PORT + NUM_NODES - 1)))"
echo ""
echo "Press Ctrl+C to stop the cluster."

wait
