CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -I include
LDFLAGS := -lpthread

# macOS doesn't support MSG_NOSIGNAL
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    CXXFLAGS += -DMSG_NOSIGNAL=0
endif

BUILD_DIR := build
SRC_DIR := src

# Core library sources
LIB_SRCS := \
    $(SRC_DIR)/hashing/murmur3.cpp \
    $(SRC_DIR)/hashing/consistent_hash_ring.cpp \
    $(SRC_DIR)/threading/thread_pool.cpp \
    $(SRC_DIR)/network/tcp_server.cpp \
    $(SRC_DIR)/network/tcp_client.cpp \
    $(SRC_DIR)/storage/chunk_store.cpp \
    $(SRC_DIR)/storage/storage_node.cpp \
    $(SRC_DIR)/metadata/memory_metadata_store.cpp \
    $(SRC_DIR)/metadata/cassandra_metadata_store.cpp \
    $(SRC_DIR)/replication/replication_manager.cpp \
    $(SRC_DIR)/replication/recovery_worker.cpp \
    $(SRC_DIR)/coordinator/coordinator.cpp \
    $(SRC_DIR)/client/scdfs_client.cpp

LIB_OBJS := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(LIB_SRCS))

# Executables
TARGETS := \
    $(BUILD_DIR)/scdfs_storage \
    $(BUILD_DIR)/scdfs_coordinator \
    $(BUILD_DIR)/scdfs_benchmark

# Tests
TEST_TARGETS := \
    $(BUILD_DIR)/test_murmur3 \
    $(BUILD_DIR)/test_consistent_hash \
    $(BUILD_DIR)/test_thread_pool \
    $(BUILD_DIR)/test_chunk_store \
    $(BUILD_DIR)/test_integration

.PHONY: all clean test benchmark

all: $(TARGETS) $(TEST_TARGETS)

# Static library
$(BUILD_DIR)/libscdfs.a: $(LIB_OBJS)
	@mkdir -p $(dir $@)
	ar rcs $@ $^

# Object files
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Executables
$(BUILD_DIR)/scdfs_storage: $(SRC_DIR)/storage_main.cpp $(BUILD_DIR)/libscdfs.a
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $< -L$(BUILD_DIR) -lscdfs $(LDFLAGS) -o $@

$(BUILD_DIR)/scdfs_coordinator: $(SRC_DIR)/coordinator_main.cpp $(BUILD_DIR)/libscdfs.a
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $< -L$(BUILD_DIR) -lscdfs $(LDFLAGS) -o $@

$(BUILD_DIR)/scdfs_benchmark: benchmarks/throughput_benchmark.cpp $(BUILD_DIR)/libscdfs.a
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $< -L$(BUILD_DIR) -lscdfs $(LDFLAGS) -o $@

# Tests
$(BUILD_DIR)/test_murmur3: tests/test_murmur3.cpp $(BUILD_DIR)/libscdfs.a
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $< -L$(BUILD_DIR) -lscdfs $(LDFLAGS) -o $@

$(BUILD_DIR)/test_consistent_hash: tests/test_consistent_hash.cpp $(BUILD_DIR)/libscdfs.a
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $< -L$(BUILD_DIR) -lscdfs $(LDFLAGS) -o $@

$(BUILD_DIR)/test_thread_pool: tests/test_thread_pool.cpp $(BUILD_DIR)/libscdfs.a
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $< -L$(BUILD_DIR) -lscdfs $(LDFLAGS) -o $@

$(BUILD_DIR)/test_chunk_store: tests/test_chunk_store.cpp $(BUILD_DIR)/libscdfs.a
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $< -L$(BUILD_DIR) -lscdfs $(LDFLAGS) -o $@

$(BUILD_DIR)/test_integration: tests/test_integration.cpp $(BUILD_DIR)/libscdfs.a
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $< -L$(BUILD_DIR) -lscdfs $(LDFLAGS) -o $@

# Run all tests
test: $(TEST_TARGETS)
	@echo ""
	@echo "=== Running All Tests ==="
	@echo ""
	@for t in $(TEST_TARGETS); do \
		echo "--- Running $$t ---"; \
		$$t || exit 1; \
	done
	@echo "=== All Tests Passed ==="

# Run benchmark
benchmark: $(BUILD_DIR)/scdfs_benchmark
	@rm -rf /tmp/scdfs_bench
	$(BUILD_DIR)/scdfs_benchmark

clean:
	rm -rf $(BUILD_DIR)
