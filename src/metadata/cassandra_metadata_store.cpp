#include "metadata/cassandra_metadata_store.hpp"
#include "common/logger.hpp"

namespace scdfs {

CassandraMetadataStore::CassandraMetadataStore(const Config& config)
    : config_(config) {}

CassandraMetadataStore::~CassandraMetadataStore() {
#ifdef SCDFS_USE_CASSANDRA
    if (session_) {
        CassFuture* close_future = cass_session_close(session_);
        cass_future_wait(close_future);
        cass_future_free(close_future);
        cass_session_free(session_);
    }
    if (cluster_) {
        cass_cluster_free(cluster_);
    }
#endif
}

bool CassandraMetadataStore::initialize() {
#ifdef SCDFS_USE_CASSANDRA
    cluster_ = cass_cluster_new();
    session_ = cass_session_new();

    cass_cluster_set_contact_points(cluster_, config_.cassandra_contact_points.c_str());
    cass_cluster_set_port(cluster_, config_.cassandra_port);

    CassFuture* connect_future = cass_session_connect(session_, cluster_);
    CassError rc = cass_future_error_code(connect_future);
    cass_future_free(connect_future);

    if (rc != CASS_OK) {
        LOG_ERROR("Cassandra connection failed: ", cass_error_desc(rc));
        return false;
    }

    LOG_INFO("Connected to Cassandra at ", config_.cassandra_contact_points);
    return create_schema();
#else
    LOG_ERROR("CassandraMetadataStore: compiled without Cassandra support");
    return false;
#endif
}

#ifdef SCDFS_USE_CASSANDRA

bool CassandraMetadataStore::create_schema() {
    std::string create_keyspace =
        "CREATE KEYSPACE IF NOT EXISTS " + config_.cassandra_keyspace +
        " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";

    if (!execute_query(create_keyspace)) return false;

    std::string use_ks = "USE " + config_.cassandra_keyspace;
    if (!execute_query(use_ks)) return false;

    if (!execute_query(
        "CREATE TABLE IF NOT EXISTS files ("
        "  file_path text PRIMARY KEY,"
        "  file_size bigint,"
        "  chunk_count int,"
        "  version bigint,"
        "  created_at timestamp,"
        "  updated_at timestamp"
        ")")) return false;

    if (!execute_query(
        "CREATE TABLE IF NOT EXISTS chunks ("
        "  file_path text,"
        "  chunk_index int,"
        "  chunk_id text,"
        "  chunk_size bigint,"
        "  checksum text,"
        "  version bigint,"
        "  status text,"
        "  replica_nodes list<text>,"
        "  PRIMARY KEY (file_path, chunk_index)"
        ")")) return false;

    if (!execute_query(
        "CREATE TABLE IF NOT EXISTS nodes ("
        "  node_id text PRIMARY KEY,"
        "  address text,"
        "  port int,"
        "  status text,"
        "  last_heartbeat timestamp,"
        "  capacity_bytes bigint,"
        "  used_bytes bigint"
        ")")) return false;

    LOG_INFO("Cassandra schema initialized");
    return true;
}

bool CassandraMetadataStore::execute_query(const std::string& query) {
    CassStatement* stmt = cass_statement_new(query.c_str(), 0);
    CassFuture* future = cass_session_execute(session_, stmt);
    cass_statement_free(stmt);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        const char* msg;
        size_t msg_len;
        cass_future_error_message(future, &msg, &msg_len);
        LOG_ERROR("Cassandra query failed: ", std::string(msg, msg_len));
    }
    cass_future_free(future);
    return rc == CASS_OK;
}

CassFuture* CassandraMetadataStore::execute_statement(CassStatement* stmt) {
    return cass_session_execute(session_, stmt);
}

std::string CassandraMetadataStore::join_list(const std::vector<std::string>& items) const {
    std::string result = "[";
    for (size_t i = 0; i < items.size(); i++) {
        if (i > 0) result += ",";
        result += "'" + items[i] + "'";
    }
    result += "]";
    return result;
}

std::vector<std::string> CassandraMetadataStore::parse_list(const CassValue* value) const {
    std::vector<std::string> result;
    CassIterator* iter = cass_iterator_from_collection(value);
    while (cass_iterator_next(iter)) {
        const CassValue* item = cass_iterator_get_value(iter);
        const char* str;
        size_t str_len;
        cass_value_get_string(item, &str, &str_len);
        result.emplace_back(str, str_len);
    }
    cass_iterator_free(iter);
    return result;
}

#endif // SCDFS_USE_CASSANDRA

// ---- File operations ----

bool CassandraMetadataStore::put_file(const FileMetadata& file) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query =
        "INSERT INTO files (file_path, file_size, chunk_count, version, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, toTimestamp(now()), toTimestamp(now()))";
    CassStatement* stmt = cass_statement_new(query.c_str(), 4);
    cass_statement_bind_string(stmt, 0, file.file_path.c_str());
    cass_statement_bind_int64(stmt, 1, static_cast<cass_int64_t>(file.file_size));
    cass_statement_bind_int32(stmt, 2, file.chunk_count);
    cass_statement_bind_int64(stmt, 3, static_cast<cass_int64_t>(file.version));

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);
    return rc == CASS_OK;
#else
    (void)file;
    return false;
#endif
}

std::optional<FileMetadata> CassandraMetadataStore::get_file(const FilePath& path) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query = "SELECT * FROM files WHERE file_path = ?";
    CassStatement* stmt = cass_statement_new(query.c_str(), 1);
    cass_statement_bind_string(stmt, 0, path.c_str());

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        cass_future_free(future);
        cass_statement_free(stmt);
        return std::nullopt;
    }

    const CassResult* result = cass_future_get_result(future);
    const CassRow* row = cass_result_first_row(result);

    if (!row) {
        cass_result_free(result);
        cass_future_free(future);
        cass_statement_free(stmt);
        return std::nullopt;
    }

    FileMetadata meta;
    meta.file_path = path;

    cass_int64_t file_size;
    cass_value_get_int64(cass_row_get_column_by_name(row, "file_size"), &file_size);
    meta.file_size = static_cast<size_t>(file_size);

    cass_int32_t chunk_count;
    cass_value_get_int32(cass_row_get_column_by_name(row, "chunk_count"), &chunk_count);
    meta.chunk_count = chunk_count;

    cass_int64_t version;
    cass_value_get_int64(cass_row_get_column_by_name(row, "version"), &version);
    meta.version = static_cast<Version>(version);

    cass_result_free(result);
    cass_future_free(future);
    cass_statement_free(stmt);
    return meta;
#else
    (void)path;
    return std::nullopt;
#endif
}

bool CassandraMetadataStore::delete_file(const FilePath& path) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query = "DELETE FROM files WHERE file_path = ?";
    CassStatement* stmt = cass_statement_new(query.c_str(), 1);
    cass_statement_bind_string(stmt, 0, path.c_str());

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);
    return rc == CASS_OK;
#else
    (void)path;
    return false;
#endif
}

std::vector<FileMetadata> CassandraMetadataStore::list_files(const std::string& prefix) {
#ifdef SCDFS_USE_CASSANDRA
    // Full table scan with optional prefix filter
    std::string query = "SELECT * FROM files";
    CassStatement* stmt = cass_statement_new(query.c_str(), 0);

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    std::vector<FileMetadata> results;

    if (rc == CASS_OK) {
        const CassResult* result = cass_future_get_result(future);
        CassIterator* iter = cass_iterator_from_result(result);

        while (cass_iterator_next(iter)) {
            const CassRow* row = cass_iterator_get_row(iter);
            const char* fp;
            size_t fp_len;
            cass_value_get_string(cass_row_get_column_by_name(row, "file_path"), &fp, &fp_len);
            std::string file_path(fp, fp_len);

            if (!prefix.empty() && file_path.substr(0, prefix.size()) != prefix) continue;

            FileMetadata meta;
            meta.file_path = file_path;

            cass_int64_t file_size;
            cass_value_get_int64(cass_row_get_column_by_name(row, "file_size"), &file_size);
            meta.file_size = static_cast<size_t>(file_size);

            cass_int32_t chunk_count;
            cass_value_get_int32(cass_row_get_column_by_name(row, "chunk_count"), &chunk_count);
            meta.chunk_count = chunk_count;

            cass_int64_t version;
            cass_value_get_int64(cass_row_get_column_by_name(row, "version"), &version);
            meta.version = static_cast<Version>(version);

            results.push_back(meta);
        }

        cass_iterator_free(iter);
        cass_result_free(result);
    }

    cass_future_free(future);
    cass_statement_free(stmt);
    return results;
#else
    (void)prefix;
    return {};
#endif
}

// ---- Chunk operations ----

bool CassandraMetadataStore::put_chunk(const ChunkMetadata& chunk) {
#ifdef SCDFS_USE_CASSANDRA
    std::string replicas = join_list(chunk.replica_nodes);
    std::string query =
        "INSERT INTO chunks (file_path, chunk_index, chunk_id, chunk_size, checksum, version, status, replica_nodes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, " + replicas + ")";
    CassStatement* stmt = cass_statement_new(query.c_str(), 7);
    cass_statement_bind_string(stmt, 0, chunk.file_path.c_str());
    cass_statement_bind_int32(stmt, 1, chunk.chunk_index);
    cass_statement_bind_string(stmt, 2, chunk.chunk_id.c_str());
    cass_statement_bind_int64(stmt, 3, static_cast<cass_int64_t>(chunk.chunk_size));
    cass_statement_bind_string(stmt, 4, chunk.checksum.c_str());
    cass_statement_bind_int64(stmt, 5, static_cast<cass_int64_t>(chunk.version));
    cass_statement_bind_string(stmt, 6, to_string(chunk.status));

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);
    return rc == CASS_OK;
#else
    (void)chunk;
    return false;
#endif
}

std::optional<ChunkMetadata> CassandraMetadataStore::get_chunk(const FilePath& file_path, int chunk_index) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query = "SELECT * FROM chunks WHERE file_path = ? AND chunk_index = ?";
    CassStatement* stmt = cass_statement_new(query.c_str(), 2);
    cass_statement_bind_string(stmt, 0, file_path.c_str());
    cass_statement_bind_int32(stmt, 1, chunk_index);

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        cass_future_free(future);
        cass_statement_free(stmt);
        return std::nullopt;
    }

    const CassResult* result = cass_future_get_result(future);
    const CassRow* row = cass_result_first_row(result);

    if (!row) {
        cass_result_free(result);
        cass_future_free(future);
        cass_statement_free(stmt);
        return std::nullopt;
    }

    ChunkMetadata meta;
    meta.file_path = file_path;
    meta.chunk_index = chunk_index;

    const char* cid;
    size_t cid_len;
    cass_value_get_string(cass_row_get_column_by_name(row, "chunk_id"), &cid, &cid_len);
    meta.chunk_id = std::string(cid, cid_len);

    cass_int64_t chunk_size;
    cass_value_get_int64(cass_row_get_column_by_name(row, "chunk_size"), &chunk_size);
    meta.chunk_size = static_cast<size_t>(chunk_size);

    const char* cs;
    size_t cs_len;
    cass_value_get_string(cass_row_get_column_by_name(row, "checksum"), &cs, &cs_len);
    meta.checksum = std::string(cs, cs_len);

    cass_int64_t version;
    cass_value_get_int64(cass_row_get_column_by_name(row, "version"), &version);
    meta.version = static_cast<Version>(version);

    const char* status_str;
    size_t status_len;
    cass_value_get_string(cass_row_get_column_by_name(row, "status"), &status_str, &status_len);
    std::string status(status_str, status_len);
    meta.status = (status == "COMMITTED") ? ChunkStatus::COMMITTED :
                  (status == "DELETED")   ? ChunkStatus::DELETED :
                                            ChunkStatus::PENDING;

    meta.replica_nodes = parse_list(cass_row_get_column_by_name(row, "replica_nodes"));

    cass_result_free(result);
    cass_future_free(future);
    cass_statement_free(stmt);
    return meta;
#else
    (void)file_path; (void)chunk_index;
    return std::nullopt;
#endif
}

std::vector<ChunkMetadata> CassandraMetadataStore::get_chunks_for_file(const FilePath& file_path) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query = "SELECT * FROM chunks WHERE file_path = ? ORDER BY chunk_index ASC";
    CassStatement* stmt = cass_statement_new(query.c_str(), 1);
    cass_statement_bind_string(stmt, 0, file_path.c_str());

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    std::vector<ChunkMetadata> results;

    if (rc == CASS_OK) {
        const CassResult* result = cass_future_get_result(future);
        CassIterator* iter = cass_iterator_from_result(result);

        while (cass_iterator_next(iter)) {
            const CassRow* row = cass_iterator_get_row(iter);
            ChunkMetadata meta;
            meta.file_path = file_path;

            cass_int32_t idx;
            cass_value_get_int32(cass_row_get_column_by_name(row, "chunk_index"), &idx);
            meta.chunk_index = idx;

            const char* cid;
            size_t cid_len;
            cass_value_get_string(cass_row_get_column_by_name(row, "chunk_id"), &cid, &cid_len);
            meta.chunk_id = std::string(cid, cid_len);

            cass_int64_t csz;
            cass_value_get_int64(cass_row_get_column_by_name(row, "chunk_size"), &csz);
            meta.chunk_size = static_cast<size_t>(csz);

            const char* cs;
            size_t cs_len;
            cass_value_get_string(cass_row_get_column_by_name(row, "checksum"), &cs, &cs_len);
            meta.checksum = std::string(cs, cs_len);

            cass_int64_t ver;
            cass_value_get_int64(cass_row_get_column_by_name(row, "version"), &ver);
            meta.version = static_cast<Version>(ver);

            const char* st;
            size_t st_len;
            cass_value_get_string(cass_row_get_column_by_name(row, "status"), &st, &st_len);
            std::string status(st, st_len);
            meta.status = (status == "COMMITTED") ? ChunkStatus::COMMITTED :
                          (status == "DELETED")   ? ChunkStatus::DELETED :
                                                    ChunkStatus::PENDING;

            meta.replica_nodes = parse_list(cass_row_get_column_by_name(row, "replica_nodes"));
            results.push_back(meta);
        }

        cass_iterator_free(iter);
        cass_result_free(result);
    }

    cass_future_free(future);
    cass_statement_free(stmt);
    return results;
#else
    (void)file_path;
    return {};
#endif
}

bool CassandraMetadataStore::update_chunk_status(const FilePath& file_path, int chunk_index, ChunkStatus status) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query = "UPDATE chunks SET status = ? WHERE file_path = ? AND chunk_index = ?";
    CassStatement* stmt = cass_statement_new(query.c_str(), 3);
    cass_statement_bind_string(stmt, 0, to_string(status));
    cass_statement_bind_string(stmt, 1, file_path.c_str());
    cass_statement_bind_int32(stmt, 2, chunk_index);

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);
    return rc == CASS_OK;
#else
    (void)file_path; (void)chunk_index; (void)status;
    return false;
#endif
}

bool CassandraMetadataStore::update_chunk_replicas(const FilePath& file_path, int chunk_index,
                                                    const std::vector<NodeId>& replicas) {
#ifdef SCDFS_USE_CASSANDRA
    std::string list_str = join_list(replicas);
    std::string query = "UPDATE chunks SET replica_nodes = " + list_str +
                        " WHERE file_path = ? AND chunk_index = ?";
    CassStatement* stmt = cass_statement_new(query.c_str(), 2);
    cass_statement_bind_string(stmt, 0, file_path.c_str());
    cass_statement_bind_int32(stmt, 1, chunk_index);

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);
    return rc == CASS_OK;
#else
    (void)file_path; (void)chunk_index; (void)replicas;
    return false;
#endif
}

std::vector<ChunkMetadata> CassandraMetadataStore::get_chunks_on_node(const NodeId& node_id) {
#ifdef SCDFS_USE_CASSANDRA
    // Requires a full scan since Cassandra doesn't support CONTAINS queries efficiently.
    // In production, maintain a secondary index or a separate table.
    std::string query = "SELECT * FROM chunks";
    CassStatement* stmt = cass_statement_new(query.c_str(), 0);

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    std::vector<ChunkMetadata> results;

    if (rc == CASS_OK) {
        const CassResult* result = cass_future_get_result(future);
        CassIterator* iter = cass_iterator_from_result(result);

        while (cass_iterator_next(iter)) {
            const CassRow* row = cass_iterator_get_row(iter);
            auto replicas = parse_list(cass_row_get_column_by_name(row, "replica_nodes"));
            bool found = false;
            for (const auto& r : replicas) {
                if (r == node_id) { found = true; break; }
            }
            if (!found) continue;

            ChunkMetadata meta;
            const char* fp;
            size_t fp_len;
            cass_value_get_string(cass_row_get_column_by_name(row, "file_path"), &fp, &fp_len);
            meta.file_path = std::string(fp, fp_len);

            cass_int32_t idx;
            cass_value_get_int32(cass_row_get_column_by_name(row, "chunk_index"), &idx);
            meta.chunk_index = idx;

            const char* cid;
            size_t cid_len;
            cass_value_get_string(cass_row_get_column_by_name(row, "chunk_id"), &cid, &cid_len);
            meta.chunk_id = std::string(cid, cid_len);

            meta.replica_nodes = replicas;
            results.push_back(meta);
        }

        cass_iterator_free(iter);
        cass_result_free(result);
    }

    cass_future_free(future);
    cass_statement_free(stmt);
    return results;
#else
    (void)node_id;
    return {};
#endif
}

// ---- Node operations ----

bool CassandraMetadataStore::register_node(const NodeInfo& node) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query =
        "INSERT INTO nodes (node_id, address, port, status, last_heartbeat, capacity_bytes, used_bytes) "
        "VALUES (?, ?, ?, ?, toTimestamp(now()), ?, ?)";
    CassStatement* stmt = cass_statement_new(query.c_str(), 6);
    cass_statement_bind_string(stmt, 0, node.node_id.c_str());
    cass_statement_bind_string(stmt, 1, node.address.c_str());
    cass_statement_bind_int32(stmt, 2, node.port);
    cass_statement_bind_string(stmt, 3, to_string(node.status));
    cass_statement_bind_int64(stmt, 4, static_cast<cass_int64_t>(node.capacity_bytes));
    cass_statement_bind_int64(stmt, 5, static_cast<cass_int64_t>(node.used_bytes));

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);
    return rc == CASS_OK;
#else
    (void)node;
    return false;
#endif
}

bool CassandraMetadataStore::update_node_status(const NodeId& node_id, NodeStatus status) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query = "UPDATE nodes SET status = ? WHERE node_id = ?";
    CassStatement* stmt = cass_statement_new(query.c_str(), 2);
    cass_statement_bind_string(stmt, 0, to_string(status));
    cass_statement_bind_string(stmt, 1, node_id.c_str());

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);
    return rc == CASS_OK;
#else
    (void)node_id; (void)status;
    return false;
#endif
}

bool CassandraMetadataStore::update_heartbeat(const NodeId& node_id) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query = "UPDATE nodes SET last_heartbeat = toTimestamp(now()) WHERE node_id = ?";
    CassStatement* stmt = cass_statement_new(query.c_str(), 1);
    cass_statement_bind_string(stmt, 0, node_id.c_str());

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);
    return rc == CASS_OK;
#else
    (void)node_id;
    return false;
#endif
}

std::optional<NodeInfo> CassandraMetadataStore::get_node(const NodeId& node_id) {
#ifdef SCDFS_USE_CASSANDRA
    std::string query = "SELECT * FROM nodes WHERE node_id = ?";
    CassStatement* stmt = cass_statement_new(query.c_str(), 1);
    cass_statement_bind_string(stmt, 0, node_id.c_str());

    CassFuture* future = execute_statement(stmt);
    CassError rc = cass_future_error_code(future);

    if (rc != CASS_OK) {
        cass_future_free(future);
        cass_statement_free(stmt);
        return std::nullopt;
    }

    const CassResult* result = cass_future_get_result(future);
    const CassRow* row = cass_result_first_row(result);

    if (!row) {
        cass_result_free(result);
        cass_future_free(future);
        cass_statement_free(stmt);
        return std::nullopt;
    }

    NodeInfo node;
    node.node_id = node_id;

    const char* addr;
    size_t addr_len;
    cass_value_get_string(cass_row_get_column_by_name(row, "address"), &addr, &addr_len);
    node.address = std::string(addr, addr_len);

    cass_int32_t port;
    cass_value_get_int32(cass_row_get_column_by_name(row, "port"), &port);
    node.port = static_cast<uint16_t>(port);

    const char* st;
    size_t st_len;
    cass_value_get_string(cass_row_get_column_by_name(row, "status"), &st, &st_len);
    std::string status(st, st_len);
    node.status = (status == "ACTIVE") ? NodeStatus::ACTIVE :
                  (status == "FAILED") ? NodeStatus::FAILED :
                                         NodeStatus::DECOMMISSIONING;

    cass_result_free(result);
    cass_future_free(future);
    cass_statement_free(stmt);
    return node;
#else
    (void)node_id;
    return std::nullopt;
#endif
}

std::vector<NodeInfo> CassandraMetadataStore::get_all_nodes() {
#ifdef SCDFS_USE_CASSANDRA
    std::string query = "SELECT * FROM nodes";
    CassStatement* stmt = cass_statement_new(query.c_str(), 0);
    CassFuture* future = execute_statement(stmt);
    std::vector<NodeInfo> results;

    if (cass_future_error_code(future) == CASS_OK) {
        const CassResult* result = cass_future_get_result(future);
        CassIterator* iter = cass_iterator_from_result(result);

        while (cass_iterator_next(iter)) {
            const CassRow* row = cass_iterator_get_row(iter);
            NodeInfo node;

            const char* nid;
            size_t nid_len;
            cass_value_get_string(cass_row_get_column_by_name(row, "node_id"), &nid, &nid_len);
            node.node_id = std::string(nid, nid_len);

            const char* addr;
            size_t addr_len;
            cass_value_get_string(cass_row_get_column_by_name(row, "address"), &addr, &addr_len);
            node.address = std::string(addr, addr_len);

            cass_int32_t port;
            cass_value_get_int32(cass_row_get_column_by_name(row, "port"), &port);
            node.port = static_cast<uint16_t>(port);

            const char* st;
            size_t st_len;
            cass_value_get_string(cass_row_get_column_by_name(row, "status"), &st, &st_len);
            std::string status(st, st_len);
            node.status = (status == "ACTIVE") ? NodeStatus::ACTIVE :
                          (status == "FAILED") ? NodeStatus::FAILED :
                                                 NodeStatus::DECOMMISSIONING;

            results.push_back(node);
        }

        cass_iterator_free(iter);
        cass_result_free(result);
    }

    cass_future_free(future);
    cass_statement_free(stmt);
    return results;
#else
    return {};
#endif
}

std::vector<NodeInfo> CassandraMetadataStore::get_active_nodes() {
    auto all = get_all_nodes();
    std::vector<NodeInfo> active;
    for (auto& n : all) {
        if (n.status == NodeStatus::ACTIVE) active.push_back(std::move(n));
    }
    return active;
}

std::vector<NodeInfo> CassandraMetadataStore::get_failed_nodes() {
    auto all = get_all_nodes();
    std::vector<NodeInfo> failed;
    for (auto& n : all) {
        if (n.status == NodeStatus::FAILED) failed.push_back(std::move(n));
    }
    return failed;
}

} // namespace scdfs
