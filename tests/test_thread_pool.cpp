#include "threading/thread_pool.hpp"
#include <cassert>
#include <iostream>
#include <atomic>
#include <chrono>
#include <numeric>
#include <vector>

using namespace scdfs;

void test_basic_submit() {
    ThreadPool pool(4);
    auto f = pool.submit([]() { return 42; });
    assert(f.get() == 42);
    std::cout << "  [PASS] Basic submit\n";
}

void test_concurrent_tasks() {
    ThreadPool pool(4);
    std::atomic<int> counter{0};
    int num_tasks = 1000;

    std::vector<std::future<void>> futures;
    for (int i = 0; i < num_tasks; i++) {
        futures.push_back(pool.submit([&counter]() {
            counter.fetch_add(1, std::memory_order_relaxed);
        }));
    }

    for (auto& f : futures) f.get();
    assert(counter == num_tasks);
    std::cout << "  [PASS] Concurrent tasks: " << counter << "/" << num_tasks << "\n";
}

// Prevent compiler from optimizing away computation
static volatile int sink;

static int heavy_work() {
    int sum = 0;
    for (int j = 0; j < 5000000; j++) {
        sum ^= j * 2654435761;
    }
    sink = sum;
    return sum;
}

void test_parallel_speedup() {
    int n = 16;

    auto start_seq = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < n; i++) {
        heavy_work();
    }
    auto end_seq = std::chrono::high_resolution_clock::now();
    double seq_ms = std::chrono::duration<double, std::milli>(end_seq - start_seq).count();

    ThreadPool pool(4);
    auto start_par = std::chrono::high_resolution_clock::now();
    std::vector<std::future<int>> futures;
    for (int i = 0; i < n; i++) {
        futures.push_back(pool.submit(heavy_work));
    }
    for (auto& f : futures) f.get();
    auto end_par = std::chrono::high_resolution_clock::now();
    double par_ms = std::chrono::duration<double, std::milli>(end_par - start_par).count();

    double speedup = seq_ms / par_ms;

    std::cout << "  [PASS] Parallel speedup: " << speedup << "x"
              << " (seq=" << seq_ms << "ms, par=" << par_ms << "ms)\n";

    assert(speedup > 1.5);
}

void test_exception_handling() {
    ThreadPool pool(2);
    auto f = pool.submit([]() -> int {
        throw std::runtime_error("test error");
    });

    bool caught = false;
    try {
        f.get();
    } catch (const std::runtime_error& e) {
        caught = true;
        assert(std::string(e.what()) == "test error");
    }
    assert(caught);
    std::cout << "  [PASS] Exception propagation\n";
}

void test_pending_tasks() {
    ThreadPool pool(1);

    std::atomic<bool> block{true};
    pool.enqueue([&block]() {
        while (block) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    for (int i = 0; i < 5; i++) {
        pool.enqueue([]() {});
    }

    assert(pool.pending_tasks() >= 4);
    block = false;
    std::cout << "  [PASS] Pending tasks tracking\n";
}

int main() {
    std::cout << "=== Thread Pool Tests ===\n";
    test_basic_submit();
    test_concurrent_tasks();
    test_parallel_speedup();
    test_exception_handling();
    test_pending_tasks();
    std::cout << "All thread pool tests passed.\n\n";
    return 0;
}
