#!/usr/bin/env bash
set -euo pipefail

# Sets up a local Cassandra instance for ScDFS metadata storage.
# Requires Docker.

CONTAINER_NAME="scdfs-cassandra"
CASSANDRA_PORT=9042

echo "=== ScDFS Cassandra Setup ==="

if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Container '${CONTAINER_NAME}' already exists."
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Starting existing container..."
        docker start "${CONTAINER_NAME}"
    fi
else
    echo "Creating Cassandra container..."
    docker run -d \
        --name "${CONTAINER_NAME}" \
        -p ${CASSANDRA_PORT}:9042 \
        -e CASSANDRA_CLUSTER_NAME=ScDFS \
        cassandra:4.1
fi

echo "Waiting for Cassandra to be ready..."
for i in $(seq 1 30); do
    if docker exec "${CONTAINER_NAME}" cqlsh -e "SELECT now() FROM system.local" &>/dev/null; then
        echo "Cassandra is ready."
        break
    fi
    echo "  Attempt $i/30..."
    sleep 2
done

echo ""
echo "Creating ScDFS schema..."
docker exec "${CONTAINER_NAME}" cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS scdfs
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE scdfs;

CREATE TABLE IF NOT EXISTS files (
    file_path text PRIMARY KEY,
    file_size bigint,
    chunk_count int,
    version bigint,
    created_at timestamp,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS chunks (
    file_path text,
    chunk_index int,
    chunk_id text,
    chunk_size bigint,
    checksum text,
    version bigint,
    status text,
    replica_nodes list<text>,
    PRIMARY KEY (file_path, chunk_index)
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id text PRIMARY KEY,
    address text,
    port int,
    status text,
    last_heartbeat timestamp,
    capacity_bytes bigint,
    used_bytes bigint
);
"

echo ""
echo "=== Cassandra setup complete ==="
echo "  Host: 127.0.0.1"
echo "  Port: ${CASSANDRA_PORT}"
echo ""
echo "To use with ScDFS coordinator:"
echo "  ./scdfs_coordinator --cassandra 127.0.0.1"
