# ScDFS

Chunked file store in C++. The coordinator splits a file, places the chunks with a consistent hash ring, and keeps a few copies on storage nodes that speak TCP. Metadata is an in-memory map unless you build the Cassandra backend.

`ScDFSClient` calls the coordinator in the same process. The storage nodes are the processes that listen.

## Where chunks go

Each node is inserted as 128 virtual nodes (`Config::virtual_nodes`) on a ring hashed with MurmurHash3 x64_128. `get_nodes` starts at the key and walks clockwise, skipping vnode hits for a physical node it already picked, until it has `replication_factor` distinct nodes. Default is 3. Adding a node moves the keys that land in its new arcs, not the whole keyspace.

`Murmur3::hash_to_token` is the first 64 bits, cast to `int64_t`. That is the same shape as Cassandra's Murmur3Partitioner. I have not diffed it against their Java implementation.

Chunk ids are the hex digest of `path:index`, so the same path and index always name the same chunk.

## Writes

`upload_file` slices the buffer at `chunk_size` (64 MB unless you change it) and inserts each chunk row as `PENDING` before any bytes go out.

`replicate_chunk` tries a pipeline. One `STORE_CHUNK` goes to the primary, with the other replicas listed as `host:port` strings. The primary writes its file, then forwards the rest with `REPLICATE_CHUNK`. Its ACK means the local write worked. A failed forward still produces that ACK, and the manager returns the entire target list.

If the primary call fails, the manager writes each replica from the coordinator and returns the nodes that answered. The coordinator marks a chunk `COMMITTED` when that list is at least `write_quorum` (default 2). On the pipeline path the list is everyone, so the check passes even if a later hop dropped the chunk. `read_quorum` sits on `Config` and nothing reads it. A fetch walks the replica list and takes the first node that returns bytes.

The file row is written only after every chunk commits. Downloads skip anything that is not `COMMITTED`.

Overwrite bumps `FileMetadata::version`. A `PENDING` chunk that recovery touches is deleted off the other replicas and marked `DELETED`. The writer has to send the file again. There is no rollback to the previous bytes.

## Reads

`download_file` loads the chunk list, fetches `COMMITTED` chunks on the coordinator thread pool, sorts by index, and concatenates. `download_file_sequential` is the same loop on the calling thread. `make benchmark` times both on whatever machine you run it on.

## When a node dies

`RecoveryWorker` sleeps `heartbeat_interval_ms` (2s) and compares `now` to `last_heartbeat`. Past `heartbeat_timeout_ms` (6s) the node is marked `FAILED`, removed from the ring, and `recover_node` runs.

`register_storage_node` is the only writer of `last_heartbeat`. Nothing sends `HEARTBEAT` and nothing calls `update_heartbeat`. Leave the process up past the timeout and every node looks dead. The `recover` shell command and the tests call `recover_node` directly.

For each chunk that listed the failed node:

- `PENDING`: delete it on the other replicas, set `DELETED`, stop.
- `DELETED`: skip.
- `COMMITTED`: drop the failed id. If the remaining count is under the replication factor, copy from the first survivor onto active nodes that do not already have the chunk. Stop if there is no such node.

`resolve_chunk` does that for a single chunk. Nothing calls it.

## Build and run

C++17, pthreads.

```bash
make
make test
make benchmark
```

`scripts/start_cluster.sh 5` uses CMake when `build/scdfs_storage` is missing, starts that many storage nodes at port 9200 and up, and pipes `register` lines into the coordinator.

```bash
./build/scdfs_storage node_0 127.0.0.1 9200 ./data
./build/scdfs_coordinator
```

Coordinator flags: `--cassandra <host>`, `--chunk-size`, `--threads`, `--replication`, `--data-dir`, `--debug`.

Shell commands: `register`, `put`, `get`, `rm`, `ls`, `info`, `recover`, `status`, `quit`. `put` and `get` take local paths.

`scripts/setup_cassandra.sh` starts `cassandra:4.1` in Docker and creates keyspace `scdfs`. The driver build is `-DSCDFS_USE_CASSANDRA=ON` and needs the DataStax C++ driver (`pkg-config` name `cassandra`). If that binary is started with `--cassandra` but the driver was not compiled in, `initialize()` fails and the coordinator logs and uses `MemoryMetadataStore`.

## Rough edges

Chunks are a fixed size. A 5-byte file is still one chunk file. The checksum stored with the chunk is FNV-1a. Reads ignore it.

The memory store is three maps, each with a `shared_mutex`. The Cassandra store is the same rows in CQL. Finding every chunk on a node scans the `chunks` table; there is no secondary index.

Wire integers are host endian (`memcpy` of `uint32_t`). The tests all run on one machine.

`include/` and `src/` are split into `hashing`, `threading`, `network`, `storage`, `metadata`, `replication`, `coordinator`, and `client`. Tests are in `tests/`. The benchmark is `benchmarks/throughput_benchmark.cpp`.
