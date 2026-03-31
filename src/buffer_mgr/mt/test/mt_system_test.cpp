#include "bm.hpp"
#include "config.hpp"
#include "kuring.hpp"
#include "test_backend.hpp"
#include "utils/my_logger.hpp"
#include "utils/rdtsc_clock.hpp"

#include <atomic>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fcntl.h>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

u64 write_cycles = 0;
RDTSCClock write_clock(2.4_GHz);
u64 io_cycles = 0;
BufferManager bm;

namespace {

constexpr PID kMaxTestPid = 192;
std::array<std::atomic<int>, 2> g_concurrent_fault_state {};

std::string make_test_file() {
    const auto path = (std::filesystem::temp_directory_path() / "buffer_mgr_mt_system_test.bin").string();
    int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    check_ret(fd);
    check_ret(ftruncate(fd, (kMaxTestPid + 1) * pageSize));
    check_ret(close(fd));
    return path;
}

void seed_test_pages(const std::string& path) {
    int fd = open(path.c_str(), O_RDWR);
    check_ret(fd);
    Page page {};
    for (PID pid = 1; pid <= kMaxTestPid; ++pid) {
        auto* words = reinterpret_cast<u64*>(&page);
        words[0] = pid;
        words[1] = pid ^ 0x9e3779b97f4a7c15ULL;
        ensure(pwrite(fd, &page, pageSize, pid * pageSize) == pageSize);
    }
    check_ret(fsync(fd));
    check_ret(close(fd));
}

void configure(const std::string& path, mt_test::Backend backend) {
    auto& cfg = Config::get();
    cfg.setup_mode = SetupMode::DEFER_TASKRUN;
    cfg.reg_ring = false;
    cfg.reg_fds = false;
    cfg.reg_bufs = false;
    cfg.iopoll = false;
    cfg.nvme_cmds = false;
    cfg.core_id = -1;
    cfg.ssds = {path};
    cfg.virt_size = 16 * pageSize;
    cfg.evict_batch = 4;
    cfg.num_workers = 2;
    cfg.concurrency = 1;
    cfg.free_target = 0.25;
    cfg.page_table_factor = 4.0;
    cfg.submit_always = false;
    mt_test::apply_backend(backend);
}

template <class FiberFn>
void run_workers(const char* phase_name, int num_workers, int fiber_count, FiberFn&& fiber_fn) {
    std::atomic<int> ready {0};
    std::atomic<bool> go {false};
    std::atomic<int> finished {0};
    std::vector<std::jthread> workers;
    workers.reserve(num_workers);

    for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
        workers.emplace_back([&, worker_id](std::stop_token) {
            bm.init_worker(worker_id, fiber_count);
            bool local_stop = false;

            mini::Fiber worker_fiber([&, worker_id] {
                bm.worker().my_id.reset(new uint64_t {(static_cast<uint64_t>(worker_id) << 32) | 1});
                ready.fetch_add(1, std::memory_order_release);
                while (!go.load(std::memory_order_acquire)) {
                    mini::yield();
                }
                fiber_fn(worker_id, local_stop);
            });

            bm.worker().reactor->run(local_stop);
            bm.shutdown_worker();
            finished.fetch_add(1, std::memory_order_release);
        });
    }

    const auto ready_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (ready.load(std::memory_order_acquire) != num_workers) {
        ensure(std::chrono::steady_clock::now() < ready_deadline, phase_name);
        std::this_thread::yield();
    }
    go.store(true, std::memory_order_release);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (finished.load(std::memory_order_acquire) != num_workers) {
        if (!(std::chrono::steady_clock::now() < deadline)) {
            std::ostringstream oss;
            oss << phase_name
                << " finished=" << finished.load(std::memory_order_acquire)
                << " ready=" << ready.load(std::memory_order_acquire)
                << " w0=" << g_concurrent_fault_state[0].load(std::memory_order_acquire)
                << " w1=" << g_concurrent_fault_state[1].load(std::memory_order_acquire);
            ensure(false, oss.str().c_str());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void test_concurrent_fault_single_loader() {
    Logger::info("mt_system_test phase=concurrent_fault start");
    constexpr PID fault_pid = 7;
    std::atomic<int> completed {0};
    g_concurrent_fault_state[0].store(0, std::memory_order_release);
    g_concurrent_fault_state[1].store(0, std::memory_order_release);

    run_workers("concurrent fault worker timeout", 2, 1, [&](int worker_id, bool& local_stop) {
        for (;;) {
            g_concurrent_fault_state[worker_id].store(1, std::memory_order_release);
            Page* page = bm.fixS(fault_pid);
            if (!page) {
                g_concurrent_fault_state[worker_id].store(2, std::memory_order_release);
                bm.handleRestart();
                continue;
            }
            g_concurrent_fault_state[worker_id].store(3, std::memory_order_release);
            ensure(bm.isValidPtr(page));
            bm.unfixS(page);
            completed.fetch_add(1, std::memory_order_relaxed);
            local_stop = true;
            return;
        }
    });

    ensure(completed.load(std::memory_order_relaxed) == 2);
    ensure(bm.physUsedCount.load() >= 2);
    Logger::info("mt_system_test phase=concurrent_fault done");
}

} // namespace

int main(int argc, char** argv) {
    if (!jmp::init()) {
        return errno;
    }

    const auto backend = mt_test::parse_backend(argc, argv);
    const auto path = make_test_file();
    seed_test_pages(path);
    configure(path, backend);
    Logger::info("mt_system_test backend=", mt_test::backend_name(backend));

    bm.init();
    bm.allocCount.store(kMaxTestPid + 1);
    test_concurrent_fault_single_loader();

    unlink(path.c_str());
    return 0;
}
