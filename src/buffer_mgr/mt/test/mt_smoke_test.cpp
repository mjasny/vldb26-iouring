#include "bm.hpp"
#include "test_backend.hpp"
#include "utils/my_logger.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <thread>
#include <vector>

namespace {

struct TestWakeDrainCtx {
    WakeMatrix* wakes = nullptr;
    Reactor* reactor = nullptr;
};

void test_wake_drain_hook(void* raw_ctx) {
    auto* ctx = static_cast<TestWakeDrainCtx*>(raw_ctx);
    Reactor::Fiber* fiber = nullptr;
    while (ctx->wakes->channel(0, 1).ring.pop(fiber)) {
        ctx->reactor->wake(fiber);
    }
}

void test_page_state() {
    PageState entry;
    entry.init_unlocked();

    auto state = entry.load();
    ensure(PageState::get_state(state) == PageState::Unlocked);
    ensure(entry.try_lock_x(state));
    ensure(PageState::get_state(entry.load()) == PageState::Locked);

    entry.unlock_x_marked();
    state = entry.load();
    ensure(PageState::get_state(state) == PageState::Marked);

    ensure(entry.try_lock_s(state));
    state = entry.load();
    ensure(PageState::get_state(state) == 1);
    ensure(entry.try_upgrade_s_to_x(state));
    ensure(PageState::get_state(entry.load()) == PageState::Locked);
    entry.unlock_x_marked();

    state = entry.load();
    ensure(entry.try_lock_s(state));
    entry.unlock_s_marked();
    ensure(PageState::get_state(entry.load()) == PageState::Marked);
}

void test_atomic_page_table() {
    AtomicPageTable table(64, 4);
    constexpr BID a_bid = 11;
    constexpr BID b_bid = 12;

    PageState a;
    a.init_unlocked(true);
    PageState b;
    b.init_locked();

    ensure(table.insert(101, a_bid));
    ensure(table.insert(202, b_bid));

    bool seen_a = false;
    bool seen_b = false;
    table.with_entry(101, [&](BID* entry) {
        ensure(entry);
        ensure(*entry == 11);
        seen_a = true;
    });
    table.with_entry(202, [&](BID* entry) {
        ensure(entry);
        ensure(*entry == 12);
        seen_b = true;
    });
    ensure(seen_a && seen_b);

    size_t swept = 0;
    bool got_one = table.clock_sweep_next_owned(0, 1, 64, 2, [&](PID pid, BID&) {
        ensure(pid == 101 || pid == 202);
        ++swept;
        return true;
    });
    ensure(got_one);
    ensure(swept == 1);

    ensure(table.erase(101));
    bool missing = false;
    table.with_entry(101, [&](BID* entry) {
        missing = (entry == nullptr);
    });
    ensure(missing);
}

void test_wake_matrix() {
    WakeMatrix wakes(4);
    Reactor::Fiber fiber;
    fiber.owner_worker = 3;

    ensure(wakes.channel(1, 3).ring.push(&fiber));
    Reactor::Fiber* out = nullptr;
    ensure(wakes.channel(1, 3).ring.pop(out));
    ensure(out == &fiber);
}

void test_duplicate_insert_race() {
    AtomicPageTable table(256, 8);
    constexpr PID pid = 424242;
    constexpr int num_threads = 8;

    std::atomic<int> inserted {0};
    std::vector<std::jthread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i](std::stop_token) {
            PageState entry;
            entry.init_locked();
            if (table.insert(pid, static_cast<BID>(100 + i))) {
                inserted.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    ensure(inserted.load(std::memory_order_relaxed) == 1);

    bool found = false;
    table.with_entry(pid, [&](BID* entry) {
        ensure(entry);
        ensure(*entry >= 100 && *entry < 100 + num_threads);
        found = true;
    });
    ensure(found);
}

void test_owned_sweep_partitioning() {
    AtomicPageTable table(256, 8);

    PID even_pid = 0;
    PID odd_pid = 0;
    for (PID pid = 1; pid < 10000 && (!even_pid || !odd_pid); ++pid) {
        const auto owner_partition = table.owner_partition(pid);
        if ((owner_partition % 2) == 0 && !even_pid) {
            even_pid = pid;
        }
        if ((owner_partition % 2) == 1 && !odd_pid) {
            odd_pid = pid;
        }
    }
    ensure(even_pid != 0);
    ensure(odd_pid != 0);

    constexpr BID even_bid = 21;
    constexpr BID odd_bid = 22;
    PageState even_entry;
    even_entry.init_unlocked();
    PageState odd_entry;
    odd_entry.init_unlocked();
    ensure(table.insert(even_pid, even_bid));
    ensure(table.insert(odd_pid, odd_bid));

    PID owner0_pid = 0;
    PID owner1_pid = 0;
    ensure(table.clock_sweep_next_owned(0, 2, 64, 2, [&](PID pid, BID&) {
        owner0_pid = pid;
        return true;
    }));
    ensure(table.clock_sweep_next_owned(1, 2, 64, 2, [&](PID pid, BID&) {
        owner1_pid = pid;
        return true;
    }));

    ensure((table.owner_partition(owner0_pid) % 2) == 0);
    ensure((table.owner_partition(owner1_pid) % 2) == 1);
}

void test_cross_worker_reactor_wakeup() {
    WakeMatrix wakes(2);
    std::atomic<bool> parked {false};
    std::atomic<bool> resumed {false};
    std::atomic<Reactor::Fiber*> remote_fiber {nullptr};

    std::jthread worker1([&](std::stop_token) {
        io_uring ring {};
        Reactor reactor(ring);
        reactor.set_owner_worker(1);
        TestWakeDrainCtx wake_ctx {&wakes, &reactor};
        reactor.set_wake_drain_hook(&wake_ctx, &test_wake_drain_hook);
        mini::set_reactor(reactor);

        bool stop = false;
        auto fiber = std::make_unique<Reactor::Fiber>();
        remote_fiber.store(fiber.get(), std::memory_order_release);
        reactor.spawn(fiber.get(), [&] {
            parked.store(true, std::memory_order_release);
            mini::park();
            resumed.store(true, std::memory_order_release);
            stop = true;
        });

        reactor.run(stop);
    });

    while (!parked.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    Reactor::Fiber* fiber = nullptr;
    while ((fiber = remote_fiber.load(std::memory_order_acquire)) == nullptr) {
        std::this_thread::yield();
    }
    ensure(wakes.channel(0, 1).ring.push(fiber));

    while (!resumed.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

} // namespace

int main(int argc, char** argv) {
    const auto backend = mt_test::parse_backend(argc, argv);
    Logger::info("mt_smoke_test backend=", mt_test::backend_name(backend));
    test_page_state();
    test_atomic_page_table();
    test_wake_matrix();
    test_duplicate_insert_race();
    test_owned_sweep_partitioning();
    test_cross_worker_reactor_wakeup();
    return 0;
}
