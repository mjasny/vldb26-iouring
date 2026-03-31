#include "bm.hpp"
#include "kuring.hpp"
#include "test_backend.hpp"
#include "utils/my_logger.hpp"
#include "utils/rdtsc_clock.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

u64 write_cycles = 0;
RDTSCClock write_clock(2.4_GHz);
u64 io_cycles = 0;
BufferManager bm;

namespace {

struct Args {
    mt_test::Backend backend = mt_test::Backend::Posix;
    enum class Mode : uint8_t {
        SameWorker,
        Ring,
        Fanout,
    } mode = Mode::Ring;
    int workers = 2;
    int duration_ms = 2000;
};

std::string_view mode_name(Args::Mode mode) {
    switch (mode) {
        case Args::Mode::SameWorker:
            return "same";
        case Args::Mode::Ring:
            return "ring";
        case Args::Mode::Fanout:
            return "fanout";
    }
    return "unknown";
}

Args parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--backend=posix") {
            args.backend = mt_test::Backend::Posix;
        } else if (arg == "--backend=uring") {
            args.backend = mt_test::Backend::Uring;
        } else if (arg == "--mode=same") {
            args.mode = Args::Mode::SameWorker;
        } else if (arg == "--mode=ring") {
            args.mode = Args::Mode::Ring;
        } else if (arg == "--mode=fanout") {
            args.mode = Args::Mode::Fanout;
        } else if (arg.rfind("--workers=", 0) == 0) {
            args.workers = std::stoi(std::string(arg.substr(sizeof("--workers=") - 1)));
        } else if (arg.rfind("--duration_ms=", 0) == 0) {
            args.duration_ms = std::stoi(std::string(arg.substr(sizeof("--duration_ms=") - 1)));
        } else {
            ensure(false, "unknown arg");
        }
    }
    ensure(args.workers > 0);
    ensure(args.duration_ms > 0);
    return args;
}

struct BenchWakeCtx {
    WakeMatrix* wakes = nullptr;
    Reactor* reactor = nullptr;
    uint32_t worker_id = 0;
    std::atomic<uint64_t>* wake_calls = nullptr;
};

void bench_wake_drain_hook(void* raw_ctx) {
    auto* ctx = static_cast<BenchWakeCtx*>(raw_ctx);
    const size_t my_worker = ctx->worker_id;
    auto& pending = ctx->wakes->pending_by_dst[my_worker];
    if (pending.load(std::memory_order_acquire) == 0) {
        return;
    }

    uint64_t drained = 0;
    for (size_t word_idx = 0; word_idx < ctx->wakes->source_word_count; ++word_idx) {
        uint64_t active = ctx->wakes->active_sources_word(my_worker, word_idx).exchange(0, std::memory_order_acq_rel);
        while (active != 0) {
            const unsigned bit_idx = static_cast<unsigned>(__builtin_ctzll(active));
            const size_t src = word_idx * 64 + bit_idx;
            active &= active - 1;
            if (src >= ctx->wakes->worker_count) {
                continue;
            }
            auto& ring = ctx->wakes->channel(src, my_worker).ring;
            Reactor::Fiber* fiber = nullptr;
            while (ring.pop(fiber)) {
                ctx->wake_calls->fetch_add(1, std::memory_order_relaxed);
                ctx->reactor->wake(fiber);
                ++drained;
            }
        }
    }
    if (drained > 0) {
        pending.fetch_sub(drained, std::memory_order_acq_rel);
    }
}

void bench_remote_wake(WakeMatrix& wakes, uint32_t src, Reactor::Fiber* fiber, std::atomic<uint64_t>& wake_calls) {
    ensure(fiber != nullptr);
    const uint32_t dst = fiber->owner_worker;
    if (dst == src) {
        wake_calls.fetch_add(1, std::memory_order_relaxed);
        mini::wake(fiber);
        return;
    }

    auto& channel = wakes.channel(src, dst).ring;
    while (!channel.push(fiber)) {
        __builtin_ia32_pause();
    }
    const size_t word_idx = src / 64;
    const uint64_t bit = 1ull << (src % 64);
    wakes.active_sources_word(dst, word_idx).fetch_or(bit, std::memory_order_release);
    wakes.pending_by_dst[dst].fetch_add(1, std::memory_order_release);
}

} // namespace

int main(int argc, char** argv) {
    if (!jmp::init()) {
        return errno;
    }

    const Args args = parse_args(argc, argv);
    mt_test::apply_backend(args.backend);

    Logger::info("mt_wake_bench backend=", mt_test::backend_name(args.backend),
                 " mode=", mode_name(args.mode),
                 " workers=", args.workers,
                 " duration_ms=", args.duration_ms);

    WakeMatrix wakes(static_cast<size_t>(args.workers));
    std::atomic<bool> start {false};
    std::atomic<bool> stop {false};
    std::atomic<uint64_t> sends {0};
    std::atomic<uint64_t> receives {0};
    std::atomic<uint64_t> wake_calls {0};
    std::atomic<uint64_t> park_calls {0};
    std::atomic<uint64_t> park_returns {0};
    std::vector<std::atomic<Reactor::Fiber*>> receiver_fibers(static_cast<size_t>(args.workers));
    for (auto& slot : receiver_fibers) {
        slot.store(nullptr, std::memory_order_relaxed);
    }

    std::vector<std::jthread> workers;
    workers.reserve(static_cast<size_t>(args.workers));

    for (int worker_id = 0; worker_id < args.workers; ++worker_id) {
        workers.emplace_back([&, worker_id](std::stop_token) {
            io_uring ring {};
            Reactor reactor(ring);
            reactor.set_owner_worker(static_cast<uint32_t>(worker_id));
            BenchWakeCtx wake_ctx {&wakes, &reactor, static_cast<uint32_t>(worker_id), &wake_calls};
            reactor.set_wake_drain_hook(&wake_ctx, &bench_wake_drain_hook);
            mini::set_reactor(reactor);

            bool local_stop = false;
            const int next_worker = (worker_id + 1) % args.workers;

            auto receiver = std::make_unique<Reactor::Fiber>();
            receiver_fibers[worker_id].store(receiver.get(), std::memory_order_release);
            reactor.spawn(receiver.get(), [&] {
                while (!start.load(std::memory_order_acquire)) {
                    mini::yield();
                }
                while (!stop.load(std::memory_order_acquire)) {
                    park_calls.fetch_add(1, std::memory_order_relaxed);
                    mini::park();
                    park_returns.fetch_add(1, std::memory_order_relaxed);
                    if (!stop.load(std::memory_order_acquire)) {
                        receives.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });

            auto sender = std::make_unique<Reactor::Fiber>();
            reactor.spawn(sender.get(), [&] {
                while (!start.load(std::memory_order_acquire)) {
                    mini::yield();
                }
                int target_worker = (worker_id + 1) % args.workers;
                while (!stop.load(std::memory_order_acquire)) {
                    if (args.mode == Args::Mode::Fanout) {
                        Reactor::Fiber* target = receiver_fibers[target_worker].load(std::memory_order_acquire);
                        if (target && target_worker != worker_id) {
                            bench_remote_wake(wakes, static_cast<uint32_t>(worker_id), target, wake_calls);
                            sends.fetch_add(1, std::memory_order_relaxed);
                        }
                        target_worker = (target_worker + 1) % args.workers;
                        if (target_worker == worker_id) {
                            target_worker = (target_worker + 1) % args.workers;
                        }
                    } else {
                        const int dst_worker =
                            args.mode == Args::Mode::SameWorker ? worker_id : next_worker;
                        Reactor::Fiber* target = receiver_fibers[dst_worker].load(std::memory_order_acquire);
                        if (target) {
                            bench_remote_wake(wakes, static_cast<uint32_t>(worker_id), target, wake_calls);
                            sends.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    mini::yield();
                }
            });

            auto stopper = std::make_unique<Reactor::Fiber>();
            reactor.spawn(stopper.get(), [&] {
                while (!stop.load(std::memory_order_acquire)) {
                    mini::yield();
                }
                mini::wake(receiver.get());
                local_stop = true;
            });

            reactor.run(local_stop);
        });
    }

    for (;;) {
        bool all_ready = true;
        for (int worker_id = 0; worker_id < args.workers; ++worker_id) {
            if (!receiver_fibers[worker_id].load(std::memory_order_acquire)) {
                all_ready = false;
                break;
            }
        }
        if (all_ready) {
            break;
        }
        std::this_thread::yield();
    }

    const auto start_tp = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(args.duration_ms));
    stop.store(true, std::memory_order_release);
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_tp).count();

    workers.clear();

    const uint64_t sent = sends.load(std::memory_order_relaxed);
    const uint64_t recv = receives.load(std::memory_order_relaxed);
    const uint64_t wakes_called = wake_calls.load(std::memory_order_relaxed);
    const uint64_t parks = park_calls.load(std::memory_order_relaxed);
    const uint64_t park_resumes = park_returns.load(std::memory_order_relaxed);
    const double seconds = std::max(1.0, static_cast<double>(elapsed_ms) / 1000.0);

    Logger::info("mt_wake_bench sent=", sent,
                 " recv=", recv,
                 " wake_calls=", wakes_called,
                 " park_calls=", parks,
                 " park_returns=", park_resumes,
                 " sent_per_s=", static_cast<uint64_t>(sent / seconds),
                 " recv_per_s=", static_cast<uint64_t>(recv / seconds),
                 " wake_calls_per_s=", static_cast<uint64_t>(wakes_called / seconds),
                 " park_calls_per_s=", static_cast<uint64_t>(parks / seconds));
    return 0;
}
