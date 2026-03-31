#include "bm.hpp"
#include "btree.hpp"
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
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

u64 write_cycles = 0;
RDTSCClock write_clock(2.4_GHz);
u64 io_cycles = 0;
BufferManager bm;

namespace {

struct Args {
    mt_test::Backend backend = mt_test::Backend::Posix;
    int workers = 2;
};

Args parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--backend=posix") {
            args.backend = mt_test::Backend::Posix;
        } else if (arg == "--backend=uring") {
            args.backend = mt_test::Backend::Uring;
        } else if (arg.rfind("--workers=", 0) == 0) {
            args.workers = std::stoi(std::string(arg.substr(sizeof("--workers=") - 1)));
        } else {
            ensure(false, "unknown arg");
        }
    }
    ensure(args.workers == 2 || args.workers == 4, "workers must be 2 or 4");
    return args;
}

std::string make_test_file(int workers) {
    auto pattern = (std::filesystem::temp_directory_path() /
                    ("buffer_mgr_mt_multiworker_progress_test_" + std::to_string(workers) + "_XXXXXX"))
                       .string();
    std::vector<char> path(pattern.begin(), pattern.end());
    path.push_back('\0');
    int fd = mkstemp(path.data());
    check_ret(fd);
    const auto file_path = std::string(path.data());
    constexpr uint64_t file_size = 128_MiB;
    check_ret(ftruncate(fd, file_size));
    check_ret(close(fd));
    return file_path;
}

void configure(const std::string& path, mt_test::Backend backend, int workers) {
    auto& cfg = Config::get();
    cfg.setup_mode = SetupMode::DEFER_TASKRUN;
    cfg.reg_ring = false;
    cfg.reg_fds = false;
    cfg.reg_bufs = false;
    cfg.iopoll = false;
    cfg.nvme_cmds = false;
    cfg.core_id = -1;
    cfg.ssds = {path};
    cfg.virt_size = 128_MiB;
    cfg.evict_batch = 8;
    cfg.num_workers = workers;
    cfg.concurrency = 1;
    cfg.free_target = 0.25;
    cfg.page_table_factor = 4.0;
    cfg.submit_always = false;
    mt_test::apply_backend(backend);
}

template <class FiberFn>
void run_workers_with_progress(const char* phase_name,
                               int num_workers,
                               int fiber_count,
                               std::atomic<uint64_t>& progress,
                               std::atomic<int>* worker_state,
                               FiberFn&& fiber_fn) {
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
                worker_state[worker_id].store(1, std::memory_order_release);
                ready.fetch_add(1, std::memory_order_release);
                while (!go.load(std::memory_order_acquire)) {
                    mini::yield();
                }
                worker_state[worker_id].store(2, std::memory_order_release);
                fiber_fn(worker_id, local_stop);
                worker_state[worker_id].store(3, std::memory_order_release);
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

    uint64_t last_progress = progress.load(std::memory_order_acquire);
    auto progress_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
    const auto overall_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);

    while (finished.load(std::memory_order_acquire) != num_workers) {
        const auto now = std::chrono::steady_clock::now();
        const auto current_progress = progress.load(std::memory_order_acquire);
        if (current_progress != last_progress) {
            last_progress = current_progress;
            progress_deadline = now + std::chrono::seconds(4);
        }
        if (!(now < overall_deadline) || !(now < progress_deadline)) {
            std::ostringstream oss;
            oss << phase_name
                << " finished=" << finished.load(std::memory_order_acquire)
                << " ready=" << ready.load(std::memory_order_acquire)
                << " progress=" << current_progress;
            for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
                oss << " w" << worker_id << "=" << worker_state[worker_id].load(std::memory_order_acquire);
            }
            ensure(false, oss.str().c_str());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void initialize_tree(std::unique_ptr<BTree>& tree) {
    bm.init_worker(0, 1);
    bool done = false;
    mini::Fiber setup([&] {
        bm.worker().my_id.reset(new uint64_t {0xff});
        tree = std::make_unique<BTree>();
        done = true;
    });
    bm.worker().reactor->run(done);
    bm.shutdown_worker();
}

void test_multiworker_insert_lookup_progress(int workers) {
    Logger::info("mt_multiworker_progress_test workers=", workers, " phase=insert_lookup start");

    std::unique_ptr<BTree> tree;
    initialize_tree(tree);

    constexpr uint64_t keys_per_worker = 4096;
    std::atomic<uint64_t> progress {0};
    auto worker_state = std::make_unique<std::atomic<int>[]>(workers);
    for (int i = 0; i < workers; ++i) {
        worker_state[i].store(0, std::memory_order_release);
    }

    run_workers_with_progress("multiworker insert timeout", workers, 1, progress, worker_state.get(),
                              [&](int worker_id, bool& local_stop) {
                                  const uint64_t base = static_cast<uint64_t>(worker_id) * keys_per_worker;
                                  for (uint64_t i = 0; i < keys_per_worker; ++i) {
                                      uint64_t key = base + i + 1;
                                      uint64_t payload = key ^ 0x9e3779b97f4a7c15ULL;
                                      tree->insert({reinterpret_cast<u8*>(&key), sizeof(key)},
                                                   {reinterpret_cast<u8*>(&payload), sizeof(payload)});
                                      progress.fetch_add(1, std::memory_order_release);
                                  }
                                  local_stop = true;
                              });

    ensure(progress.load(std::memory_order_acquire) == static_cast<uint64_t>(workers) * keys_per_worker);

    progress.store(0, std::memory_order_release);
    for (int i = 0; i < workers; ++i) {
        worker_state[i].store(0, std::memory_order_release);
    }

    run_workers_with_progress("multiworker lookup timeout", workers, 1, progress, worker_state.get(),
                              [&](int worker_id, bool& local_stop) {
                                  const uint64_t base = static_cast<uint64_t>(worker_id) * keys_per_worker;
                                  for (uint64_t i = 0; i < 1024; ++i) {
                                      uint64_t key = base + ((i * 17) % keys_per_worker) + 1;
                                      uint8_t out[sizeof(uint64_t)];
                                      const int rc =
                                          tree->lookup({reinterpret_cast<u8*>(&key), sizeof(key)}, out, sizeof(out));
                                      ensure(rc == sizeof(uint64_t));
                                      uint64_t payload = 0;
                                      std::memcpy(&payload, out, sizeof(payload));
                                      ensure(payload == (key ^ 0x9e3779b97f4a7c15ULL));
                                      progress.fetch_add(1, std::memory_order_release);
                                  }
                                  local_stop = true;
                              });

    ensure(progress.load(std::memory_order_acquire) == static_cast<uint64_t>(workers) * 1024);
    Logger::info("mt_multiworker_progress_test workers=", workers, " phase=insert_lookup done");
}

} // namespace

int main(int argc, char** argv) {
    if (!jmp::init()) {
        return errno;
    }

    const auto args = parse_args(argc, argv);
    const auto path = make_test_file(args.workers);
    configure(path, args.backend, args.workers);

    Logger::info("mt_multiworker_progress_test backend=", mt_test::backend_name(args.backend),
                 " workers=", args.workers);

    bm.init();
    test_multiworker_insert_lookup_progress(args.workers);
    unlink(path.c_str());
    return 0;
}
