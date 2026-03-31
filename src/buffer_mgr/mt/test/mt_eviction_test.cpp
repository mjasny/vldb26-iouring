#include "bm.hpp"
#include "config.hpp"
#include "kuring.hpp"
#include "test_backend.hpp"
#include "utils/my_logger.hpp"
#include "utils/rdtsc_clock.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fcntl.h>
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

std::string make_test_file() {
    const auto path = (std::filesystem::temp_directory_path() / "buffer_mgr_mt_eviction_test.bin").string();
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
    cfg.num_workers = 1;
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
        ensure(std::chrono::steady_clock::now() < deadline, phase_name);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void expect_page_value(PID pid, Page* page, u64 expected0, u64 expected1) {
    ensure(page);
    auto* words = reinterpret_cast<u64*>(page);
    ensure(words[0] == expected0);
    ensure(words[1] == expected1);
}

void test_dirty_eviction_writeback() {
    Logger::info("mt_eviction_test phase=dirty_writeback start");
    constexpr PID dirty_begin = 32;
    constexpr PID dirty_end = 39;
    constexpr u64 kMagic = 0x123456789abcdef0ULL;

    std::atomic<int> dirtied {0};
    std::atomic<int> checked {0};
    std::atomic<int> dirty_barrier {0};
    const int worker_count = Config::get().num_workers;

    auto wait_for_all = [&](std::atomic<int>& counter) {
        counter.fetch_add(1, std::memory_order_acq_rel);
        while (counter.load(std::memory_order_acquire) != worker_count) {
            mini::yield();
        }
    };

    run_workers("eviction worker timeout", worker_count, 1, [&](int worker_id, bool& local_stop) {
        for (PID pid = dirty_begin + worker_id; pid <= dirty_end; pid += worker_count) {
            for (;;) {
                Page* page = bm.fixX(pid);
                if (!page) {
                    bm.handleRestart();
                    continue;
                }
                auto* words = reinterpret_cast<u64*>(page);
                words[0] = pid ^ kMagic;
                words[1] = pid;
                bm.unfixX(page);
                dirtied.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            mini::yield();
        }
        wait_for_all(dirty_barrier);

        const u64 writes_before = bm.total_write_count();
        for (int i = 0; i < 4; ++i) {
            bm.evict();
        }
        ensure(bm.total_write_count() > writes_before);

        for (PID pid = dirty_begin + worker_id; pid <= dirty_end; pid += worker_count) {
            for (;;) {
                Page* page = bm.fixS(pid);
                if (!page) {
                    bm.handleRestart();
                    continue;
                }
                expect_page_value(pid, page, pid ^ kMagic, pid);
                bm.unfixS(page);
                checked.fetch_add(1, std::memory_order_relaxed);
                break;
            }
        }
        local_stop = true;
    });

    ensure(dirtied.load(std::memory_order_relaxed) == (dirty_end - dirty_begin + 1));
    ensure(checked.load(std::memory_order_relaxed) == (dirty_end - dirty_begin + 1));
    Logger::info("mt_eviction_test phase=dirty_writeback done");
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
    Logger::info("mt_eviction_test backend=", mt_test::backend_name(backend));

    bm.init();
    bm.allocCount.store(kMaxTestPid + 1);
    test_dirty_eviction_writeback();

    unlink(path.c_str());
    return 0;
}
