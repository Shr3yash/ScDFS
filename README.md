# ScDFS — Scalable Distributed File System

A chunk-based distributed file system built in C++ featuring consistent hashing, Cassandra-backed metadata, 3x replication, and multithreaded operations.

## Architecture

```
┌──────────┐     ┌──────────────┐     ┌────────────────────┐
│  Client  │────▶│  Coordinator │────▶│  Cassandra / Memory │
│          │     │              │     │  (Metadata Store)   │
└──────────┘     └──────┬───────┘     └────────────────────┘
                        │
              ┌─────────┼─────────┐
              ▼         ▼         ▼
         ┌─────────┐ ┌─────────┐ ┌─────────┐
         │ Storage  │ │ Storage  │ │ Storage  │
         │ Node 0   │ │ Node 1   │ │ Node 2   │  ... N nodes
         └─────────┘ └─────────┘ └─────────┘
```

**Four components:**
- **Coordinator** — chunk placement, version management, quorum enforcement
- **Storage Nodes** — store chunk bytes on disk, serve over TCP, pipeline replication
- **Metadata Store** — Cassandra (production) or in-memory (development) — stores file-to-chunk maps, replica locations, version state
- **Recovery Worker** — heartbeat monitoring, failure detection, merge protocol, re-replication

## Key Design Decisions

### Consistent Hashing (Murmur3)
Nodes are placed on a hash ring with 128 virtual nodes each. Adding a node moves ~1/N of keys instead of the O(N) reshuffling that modulo hashing causes. Uses the same MurmurHash3 x64_128 algorithm as Cassandra's Murmur3Partitioner.

### Write Path
1. File splits into fixed-size chunks (default 64 MB)
2. Each chunk ID is hashed onto the ring → primary is the next clockwise node → next R-1 distinct physical nodes are replicas
3. Pipeline-style replication: client streams to primary, which forwards to the next replica
4. Coordinator waits for W acknowledgments, then commits `COMMITTED` status to metadata
5. Nothing visible to reads until the commit lands — monotonically increasing versions

### Read Path
1. Query metadata for chunk list and node locations
2. Fetch all chunks **in parallel** using the thread pool — this is where the latency improvement comes from
3. Without threading: N chunks × sequential round trips; with threading: all N fetch concurrently, total ≈ slowest single chunk

### Failure Recovery (Merge Protocol)
When a node fails:
1. Heartbeat monitor detects timeout → marks `FAILED` in metadata → removes from hash ring
2. Recovery worker scans for chunks that listed the failed node as a replica
3. **Merge protocol** handles the hard edge case — write-in-flight during failure:
   - If metadata shows `PENDING` (uncommitted): discard partial replicas, surface previous version
   - If metadata shows `COMMITTED`: re-replicate from survivors to new targets, restore replication factor

## Benchmark Results

```
ScDFS THROUGHPUT BENCHMARK
  Chunk size:    1024 KB | Storage nodes: 5 | Thread pool: 8 | Replication: 3x

[1] UPLOAD THROUGHPUT
  32 MB x 5 files:  149.57 MB/s  avg_lat=213.95ms  p95=230.77ms

[2] DOWNLOAD: PARALLEL vs SEQUENTIAL
  Parallel (thread pool):  1615.16 MB/s  avg_lat=19.43ms
  Sequential:               975.74 MB/s  avg_lat=32.38ms
  >> Parallel is 39.6% faster than sequential

[3] CONCURRENT CLIENT UPLOADS (16 MB each)
   1 client:   174.33 MB/s
   3 clients:  198.35 MB/s
   5 clients:  214.32 MB/s
  10 clients:  254.18 MB/s

[5] FAILURE RECOVERY
  Recovery time: 1510.79 ms | Chunks recovered: 627
  Data accessible after recovery: YES | Integrity check: PASS
```

## Building

**Requirements:** C++17 compiler (g++ or clang++)

```bash
# Build everything (library, executables, tests, benchmark)
make all

# Run tests
make test

# Run benchmark
make benchmark

# Clean
make clean
```

### With Cassandra support (optional)

```bash
# Start Cassandra via Docker
./scripts/setup_cassandra.sh

# Build with Cassandra driver
# (Requires DataStax C++ driver: https://github.com/datastax/cpp-driver)
cmake -DSCDFS_USE_CASSANDRA=ON ..
```

## Usage

### Interactive Shell

```bash
# Start coordinator
./build/scdfs_coordinator

# Available commands:
scdfs> register node_0 127.0.0.1 9200
scdfs> register node_1 127.0.0.1 9201
scdfs> register node_2 127.0.0.1 9202
scdfs> put /path/to/local/file.dat /remote/file.dat
scdfs> get /remote/file.dat /path/to/output.dat
scdfs> ls
scdfs> info /remote/file.dat
scdfs> recover node_0
scdfs> status
```

### Start a Cluster

```bash
# Start 5 storage nodes + coordinator
./scripts/start_cluster.sh 5
```

### Storage Node

```bash
./build/scdfs_storage node_0 127.0.0.1 9200 ./data
```

## Project Structure

```
ScDFS/
├── include/
│   ├── common/         # Types, config, logger, serialization
│   ├── hashing/        # Murmur3, consistent hash ring
│   ├── threading/      # Thread pool
│   ├── network/        # TCP server/client
│   ├── storage/        # Chunk store, storage node
│   ├── metadata/       # MetadataStore interface, Memory + Cassandra backends
│   ├── replication/    # Replication manager, recovery worker
│   ├── coordinator/    # Coordinator (orchestrator)
│   └── client/         # ScDFS client API
├── src/                # Implementations
├── tests/              # Unit + integration tests
├── benchmarks/         # Throughput benchmark
├── scripts/            # Cluster setup, Cassandra setup, benchmark runner
├── Makefile
└── CMakeLists.txt
```

## Tradeoffs

- **Consistency costs write throughput.** Waiting for W acks before committing adds latency. Deliberate tradeoff against serving stale data.
- **Cassandra is a critical path.** If the metadata cluster degrades, reads and writes degrade with it.
- **Fixed chunk size is a simplification.** Small files waste space. Variable-sized chunks or a fast path for small files is the production answer.
- **Consistent hashing reduces but doesn't eliminate rebalancing.** Virtual nodes distribute load more evenly but the ring still needs updating on membership changes.
