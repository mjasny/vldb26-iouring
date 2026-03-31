#include "bm.hpp"
#include "btree.hpp"
#include "config.hpp"
#include "kuring.hpp"
#include "test_backend.hpp"
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

u64 write_cycles = 0;
RDTSCClock write_clock(2.4_GHz);
u64 io_cycles = 0;
BufferManager bm;

namespace {

std::string make_test_file() {
    const auto path = (std::filesystem::temp_directory_path() / "buffer_mgr_mt_runtime_test.bin").string();
    int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    check_ret(fd);
    check_ret(ftruncate(fd, 64 * 1024 * 1024));
    check_ret(close(fd));
    return path;
}

void configure(mt_test::Backend backend) {
    auto& cfg = Config::get();
    cfg.setup_mode = SetupMode::DEFER_TASKRUN;
    cfg.reg_ring = false;
    cfg.reg_fds = false;
    cfg.reg_bufs = false;
    cfg.iopoll = false;
    cfg.nvme_cmds = false;
    cfg.core_id = -1;
    cfg.ssds = {make_test_file()};
    cfg.virt_size = 64_MiB;
    cfg.evict_batch = 8;
    cfg.num_workers = 2;
    cfg.concurrency = 2;
    cfg.free_target = 0.1;
    cfg.page_table_factor = 2.0;
    cfg.submit_always = false;
    mt_test::apply_backend(backend);
}

} // namespace

int main(int argc, char** argv) {
    if (!jmp::init()) {
        return errno;
    }

    const auto backend = mt_test::parse_backend(argc, argv);
    configure(backend);

    Logger::info("mt_runtime_test backend=", mt_test::backend_name(backend));
    bm.init();
    std::unique_ptr<BTree> tree;

    std::atomic<uint64_t> ops {0};
    std::atomic<bool> stop {false};

    bm.init_worker(0, 3);
    tree = std::make_unique<BTree>();
    bool loaded = false;
    mini::Fiber loader([&] {
        bm.worker().my_id.reset(new uint64_t {0xff});
        for (uint64_t i = 1; i <= 256; ++i) {
            uint64_t payload = i * 10;
            tree->insert({reinterpret_cast<u8*>(&i), sizeof(i)},
                         {reinterpret_cast<u8*>(&payload), sizeof(payload)});
        }
        loaded = true;
    });
    bm.worker().reactor->run(loaded);

    auto worker_main = [&](int worker_id) {
        if (worker_id != 0) {
            bm.init_worker(worker_id, 3);
        }
        bool local_stop = false;

        mini::Fiber stop_fiber([&] {
            bm.worker().my_id.reset(new uint64_t {(static_cast<uint64_t>(worker_id) << 32) | 0xffff});
            while (!stop.load(std::memory_order_acquire)) {
                mini::yield();
            }
            local_stop = true;
        });

        mini::Fiber lookup1([&] {
            bm.worker().my_id.reset(new uint64_t {(static_cast<uint64_t>(worker_id) << 32) | 1});
            while (!stop.load(std::memory_order_acquire)) {
                uint64_t key = 1 + (ops.load(std::memory_order_relaxed) % 256);
                uint8_t out[sizeof(uint64_t)];
                int rc = tree->lookup({reinterpret_cast<u8*>(&key), sizeof(key)}, out, sizeof(out));
                ensure(rc == sizeof(uint64_t));
                ops.fetch_add(1, std::memory_order_relaxed);
                mini::yield();
            }
        });

        mini::Fiber lookup2([&] {
            bm.worker().my_id.reset(new uint64_t {(static_cast<uint64_t>(worker_id) << 32) | 2});
            while (!stop.load(std::memory_order_acquire)) {
                uint64_t key = 1 + ((ops.load(std::memory_order_relaxed) + 17) % 256);
                uint8_t out[sizeof(uint64_t)];
                int rc = tree->lookup({reinterpret_cast<u8*>(&key), sizeof(key)}, out, sizeof(out));
                ensure(rc == sizeof(uint64_t));
                ops.fetch_add(1, std::memory_order_relaxed);
                mini::yield();
            }
        });

        bm.worker().reactor->run(local_stop);
        bm.shutdown_worker();
    };

    std::jthread w1([&](std::stop_token) {
        worker_main(1);
    });
    std::jthread stopper([&](std::stop_token) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        stop.store(true, std::memory_order_release);
    });

    worker_main(0);
    ensure(ops.load(std::memory_order_relaxed) > 100);
    unlink(Config::get().ssds.front().c_str());
    return 0;
}
