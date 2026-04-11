#!/usr/bin/env bash
set -euo pipefail

# Build and run ScDFS benchmarks.

echo "=== ScDFS Benchmarks ==="

# Build
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DSCDFS_BUILD_BENCHMARKS=ON
make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu) scdfs_benchmark
cd ..

# Clean up previous benchmark data
rm -rf /tmp/scdfs_bench

# Run with different configurations
echo ""
echo "--- Configuration 1: 1MB chunks, 5 nodes, 8 threads ---"
./build/scdfs_benchmark 1048576 5 8

echo ""
echo "--- Configuration 2: 4MB chunks, 5 nodes, 8 threads ---"
./build/scdfs_benchmark 4194304 5 8

echo ""
echo "--- Configuration 3: 1MB chunks, 10 nodes, 16 threads ---"
./build/scdfs_benchmark 1048576 10 16

# Cleanup
rm -rf /tmp/scdfs_bench
echo ""
echo "=== Benchmarks Complete ==="
