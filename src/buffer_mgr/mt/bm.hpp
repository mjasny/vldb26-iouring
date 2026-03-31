#pragma once

#include "boost/fiber/fss.hpp"
#include "concurrency.hpp"
#include "config.hpp"
#include "kuring.hpp"
#include "rh_backshift_u64_map.hpp"
#include "types.hpp"
#include "utils/jmp.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/stats_printer.hpp"
#include "utils/utils.hpp"

#include <cassert>
#include <atomic>
#include <fstream>
#include <liburing.h>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <variant>
#include <vector>


struct SleepingFiber {
    bool stop = false;
    bool running = false;
    std::atomic<bool> pending_wakeup {false};
    std::unique_ptr<mini::Fiber> fiber;
    Reactor::Fiber* ctx;

    template <typename SetupFn, typename LoopFn>
    void spawn(SetupFn&& setup_fn, LoopFn&& loop_fn) {
        fiber = std::make_unique<mini::Fiber>(
            [this,
             setup = std::forward<SetupFn>(setup_fn),
             loop = std::forward<LoopFn>(loop_fn)]() mutable {
            register_self();
            setup();

            while (!stop) {
                if (loop()) {
                    park();
                }
            }
        });
    }

    ~SleepingFiber() {
        stop = true;
        wakeup();
        // if (fiber.joinable()) {
        //     fiber.join();
        // }
    }

    void register_self() {
        ensure(!ctx);
        ctx = mini::current();
        running = true;
    }

    void park() {
        ensure(ctx == mini::current());
        ensure(running);
        running = false;
        if (pending_wakeup.exchange(false, std::memory_order_acq_rel)) {
            running = true;
            return;
        }
        mini::park();
    }

    void wakeup() {
        if (running) {
            pending_wakeup.store(true, std::memory_order_release);
            return;
        }
        running = true;
        mini::wake(ctx);
    }
};

struct BufferManager;

struct BufferFrame {
    PID pid; // page-id
    // Waiters for an in-flight page read. The loader opens the list with
    // begin_waiters(), waiters CAS-push stack-allocated mini::Op nodes via
    // enqueue_waiter(), and completion detaches/closes the list with
    // close_waiters() before waking the detached stack.
    std::atomic<mini::Op*> waiting;

    static mini::Op* closed_waiters() {
        return reinterpret_cast<mini::Op*>(1);
    }

    explicit BufferFrame(PID pid) : pid(pid), waiting(closed_waiters()) {
    }

    // Open waiter registration for a newly started read.
    void begin_waiters() {
        waiting.store(nullptr, std::memory_order_release);
    }

    // Close waiter registration and detach the full waiter stack for wakeup.
    mini::Op* close_waiters() {
        mini::Op* head = waiting.exchange(closed_waiters(), std::memory_order_acq_rel);
        return head == closed_waiters() ? nullptr : head;
    }

    // Try to publish a waiter node while registration is still open.
    bool enqueue_waiter(mini::Op& op) {
        mini::Op* head = waiting.load(std::memory_order_acquire);
        while (true) {
            if (head == closed_waiters()) {
                return false;
            }
            op.next = head;
            if (waiting.compare_exchange_weak(head, &op, std::memory_order_release, std::memory_order_acquire)) {
                return true;
            }
        }
    }
};

struct PageFaultException {
    PID pid;
};

struct RestartException {
    PID pid;
    BID bid;
};

struct AllocException {};
struct ValidationRestart {};

using RestartReason = std::variant<PageFaultException, RestartException, AllocException, ValidationRestart>;

enum class LockConflictPolicy {
    Wait,
    Restart,
};

struct SpinLock {
    std::atomic_flag flag = ATOMIC_FLAG_INIT;

    inline bool try_lock() noexcept {
        return !flag.test_and_set(std::memory_order_acquire);
    }

    inline void lock() noexcept {
        while (flag.test_and_set(std::memory_order_acquire)) {
            __builtin_ia32_pause();
        }
    }

    inline void unlock() noexcept {
        flag.clear(std::memory_order_release);
    }
};

struct SpinGuard {
    SpinLock& lock_ref;
    explicit SpinGuard(SpinLock& lock_ref) : lock_ref(lock_ref) {
        lock_ref.lock();
    }
    ~SpinGuard() {
        lock_ref.unlock();
    }
};

// Each lock on its own cache line so concurrent per-worker acquisitions don't
// produce false sharing on the lock array.
struct alignas(64) CacheLineLock {
    SpinLock lock;
};

struct alignas(64) FreePool {
    SpinLock lock;
    std::vector<BID> bids;
};

struct AtomicPageTable {
    struct Slot {
        std::atomic<uint64_t> raw;

        Slot() : raw(EMPTY_SLOT) {
        }

        static bool is_empty(uint64_t raw) {
            return raw == EMPTY_SLOT;
        }

        static bool is_tombstone(uint64_t raw) {
            return raw == TOMBSTONE_SLOT;
        }

        static PID pid(uint64_t raw) {
            return (raw >> BID_BITS) - 1;
        }

        static BID bid(uint64_t raw) {
            return raw & BID_MASK;
        }

        static uint64_t pack(PID pid, BID bid) {
            ensure(pid < PID_LIMIT);
            ensure(static_cast<uint64_t>(bid) <= BID_MASK);
            return ((pid + 1) << BID_BITS) | static_cast<uint64_t>(bid);
        }
    };

    static constexpr unsigned BID_BITS = 24;
    static constexpr uint64_t BID_MASK = (1ull << BID_BITS) - 1;
    static constexpr uint64_t PID_LIMIT = (1ull << (64 - BID_BITS)) - 1;
    static constexpr uint64_t EMPTY_SLOT = 0;
    static constexpr uint64_t TOMBSTONE_SLOT = 1;

    // Each worker writes its own sweep position on every eviction batch.
    // Pack each position into its own cache line to prevent false sharing.
    struct alignas(64) SweepEntry {
        std::atomic<size_t> pos {0};
    };

    // Read-only after init — keep together so a single cache line fetch covers
    // all the constants needed by find_copy / insert / erase / sweep.
    Slot* slots = nullptr;
    size_t slot_count = 0;
    size_t slot_mask = 0;
    size_t owner_partition_mask = 0;
    size_t sweep_slot = 0;
    std::unique_ptr<SweepEntry[]> owner_sweep;
    size_t owner_sweep_count = 0;

    AtomicPageTable() = default;

    AtomicPageTable(size_t total_capacity_pow2, size_t owner_partition_count_pow2) {
        ensure(total_capacity_pow2 > 0 && ((total_capacity_pow2 & (total_capacity_pow2 - 1)) == 0));
        ensure(owner_partition_count_pow2 > 0 && ((owner_partition_count_pow2 & (owner_partition_count_pow2 - 1)) == 0));
        slot_count = total_capacity_pow2;
        slot_mask = total_capacity_pow2 - 1;
        owner_partition_mask = owner_partition_count_pow2 - 1;
        owner_sweep_count = owner_partition_count_pow2;
        owner_sweep = std::make_unique<SweepEntry[]>(owner_partition_count_pow2);
        slots = reinterpret_cast<Slot*>(HugePages::malloc_interleaved(sizeof(Slot) * slot_count));
        for (size_t i = 0; i < slot_count; ++i) {
            new (slots + i) Slot();
        }
    }

    ~AtomicPageTable() {
        if (slots) {
            HugePages::free_array<Slot>(slots, slot_count);
        }
    }

    AtomicPageTable(const AtomicPageTable&) = delete;
    AtomicPageTable& operator=(const AtomicPageTable&) = delete;

    static uint64_t hash(uint64_t x) {
        x += 0x9e3779b97f4a7c15ULL;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    }

    size_t owner_partition(PID pid) const {
        return std::hash<uint64_t> {}(pid) & owner_partition_mask;
    }

    size_t slot_idx(PID pid) const {
        return hash(pid) & slot_mask;
    }

    template <class Fn>
    decltype(auto) with_entry(PID pid, Fn&& fn) {
        BID bid = 0;
        if (find_copy(pid, bid)) {
            return fn(&bid);
        }
        return fn(static_cast<BID*>(nullptr));
    }

    bool find_copy(PID pid, BID& out_bid) {
        const size_t start = slot_idx(pid);
        for (size_t probed = 0; probed < slot_count; ++probed) {
            const size_t idx = (start + probed) & slot_mask;
            const uint64_t raw = slots[idx].raw.load(std::memory_order_acquire);
            if (Slot::is_empty(raw)) {
                return false;
            }
            if (Slot::is_tombstone(raw)) {
                continue;
            }
            if (Slot::pid(raw) == pid) {
                out_bid = Slot::bid(raw);
                return true;
            }
        }
        return false;
    }

    bool insert(PID pid, BID bid) {
        const uint64_t packed = Slot::pack(pid, bid);

        while (true) {
            std::optional<size_t> first_tombstone;
            const size_t start = slot_idx(pid);

            for (size_t probed = 0; probed < slot_count; ++probed) {
                const size_t idx = (start + probed) & slot_mask;
                const uint64_t raw = slots[idx].raw.load(std::memory_order_acquire);

                if (Slot::is_tombstone(raw)) {
                    if (!first_tombstone) {
                        first_tombstone = idx;
                    }
                    continue;
                }

                if (Slot::is_empty(raw)) {
                    const size_t target = first_tombstone.value_or(idx);
                    const uint64_t expected = first_tombstone ? TOMBSTONE_SLOT : EMPTY_SLOT;
                    uint64_t desired = expected;
                    if (slots[target].raw.compare_exchange_strong(desired, packed, std::memory_order_acq_rel, std::memory_order_acquire)) {
                        return true;
                    }
                    break;
                }

                if (Slot::pid(raw) == pid) {
                    return false;
                }
            }

            if (!first_tombstone) {
                // CAS failed and no tombstone found; another thread is inserting
                // concurrently into the same probe window. Retry. (The table is
                // always over-provisioned so a full-table spin is not possible.)
                __builtin_ia32_pause();
                continue;
            }

            const size_t target = *first_tombstone;
            uint64_t expected = TOMBSTONE_SLOT;
            if (slots[target].raw.compare_exchange_strong(expected, packed, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return true;
            }
        }
    }

    bool erase(PID pid) {
        while (true) {
            const size_t start = slot_idx(pid);
            for (size_t probed = 0; probed < slot_count; ++probed) {
                const size_t idx = (start + probed) & slot_mask;
                const uint64_t raw = slots[idx].raw.load(std::memory_order_acquire);
                if (Slot::is_empty(raw)) {
                    return false;
                }
                if (Slot::is_tombstone(raw) || Slot::pid(raw) != pid) {
                    continue;
                }
                uint64_t expected = raw;
                if (slots[idx].raw.compare_exchange_strong(expected, TOMBSTONE_SLOT, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    collapse_tombstone_run(idx);
                    return true;
                }
                break;
            }
        }
    }

    // Validate the entry for pid via precheck(bid). If it passes, atomically
    // erase the entry (CAS to tombstone) and then invoke postcommit(bid).
    // The callbacks are each invoked at most once; if the CAS fails we return
    // false without retrying.
    template <class PrecheckFn, class PostCommitFn>
    bool erase_if(PID pid, PrecheckFn&& precheck, PostCommitFn&& postcommit) {
        const size_t start = slot_idx(pid);
        for (size_t probed = 0; probed < slot_count; ++probed) {
            const size_t idx = (start + probed) & slot_mask;
            const uint64_t raw = slots[idx].raw.load(std::memory_order_acquire);
            if (Slot::is_empty(raw)) {
                return false;
            }
            if (Slot::is_tombstone(raw) || Slot::pid(raw) != pid) {
                continue;
            }
            const BID bid = Slot::bid(raw);
            if (!precheck(bid)) {
                return false;
            }
            uint64_t expected = raw;
            if (slots[idx].raw.compare_exchange_strong(expected, TOMBSTONE_SLOT,
                                                       std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
                collapse_tombstone_run(idx);
                postcommit(bid);
                return true;
            }
            return false;
        }
        return false;
    }

    template <class Callback>
    bool clock_sweep_next(Callback&& cb) {
        // Caller must serialize sweep progression for a given owner_idx.
        if (slot_count == 0) {
            return false;
        }
        for (size_t scanned = 0; scanned < slot_count; ++scanned) {
            const size_t idx = (sweep_slot + scanned) & slot_mask;
            const uint64_t raw = slots[idx].raw.load(std::memory_order_acquire);
            if (Slot::is_empty(raw) || Slot::is_tombstone(raw)) {
                continue;
            }
            PID pid = Slot::pid(raw);
            BID bid = Slot::bid(raw);
            if (cb(pid, bid)) {
                sweep_slot = (idx + 1) & slot_mask;
                return true;
            }
        }
        return false;
    }

    template <class Callback>
    bool clock_sweep_next_owned(size_t owner_idx, size_t owner_count, size_t batch_hint, size_t size_hint, Callback&& cb) {
        if (slot_count == 0) {
            return false;
        }
        ensure(owner_count > 0);
        ensure(size_hint > 0);
        owner_idx %= owner_count;
        ensure(owner_sweep_count >= owner_count);
        const size_t start = owner_sweep[owner_idx].pos.load(std::memory_order_relaxed) % slot_count;
        const size_t filled = size_hint;
        // Average number of slots between two consecutive entries owned by this worker.
        const size_t slots_per_owned = (filled > 0)
            ? (slot_count * owner_count / filled)
            : slot_count;
        // Scan enough slots to expect ~8x the batch size of owned entries (accounts
        // for second-chance passes and temporarily locked/dirty pages).
        const size_t window = std::min(slot_count, std::max<size_t>(batch_hint * slots_per_owned * 8, 1024));
        for (size_t scanned = 0; scanned < window; ++scanned) {
            const size_t idx = (start + scanned) & slot_mask;
            const uint64_t raw = slots[idx].raw.load(std::memory_order_acquire);
            if (Slot::is_empty(raw) || Slot::is_tombstone(raw)) {
                continue;
            }
            PID pid = Slot::pid(raw);
            if ((owner_partition(pid) % owner_count) != owner_idx) {
                continue;
            }
            BID bid = Slot::bid(raw);
            if (cb(pid, bid)) {
                owner_sweep[owner_idx].pos.store((idx + 1) & slot_mask, std::memory_order_relaxed);
                return true;
            }
        }
        owner_sweep[owner_idx].pos.store((start + window) & slot_mask, std::memory_order_relaxed);
        return false;
    }

    template <class Callback>
    void dump(Callback&& cb) {
        for (size_t idx = 0; idx < slot_count; ++idx) {
            const uint64_t raw = slots[idx].raw.load(std::memory_order_acquire);
            if (Slot::is_empty(raw) || Slot::is_tombstone(raw)) {
                continue;
            }
            PID pid = Slot::pid(raw);
            BID bid = Slot::bid(raw);
            cb(pid, bid, slot_idx(pid), idx);
        }
    }

    size_t capacity() const {
        return slot_count;
    }

private:
    void collapse_tombstone_run(size_t start_idx) {
        size_t run_len = 1;
        size_t idx = start_idx;

        while (run_len < slot_count) {
            const size_t next = (idx + 1) & slot_mask;
            const uint64_t next_raw = slots[next].raw.load(std::memory_order_acquire);
            if (Slot::is_tombstone(next_raw)) {
                idx = next;
                ++run_len;
                continue;
            }
            if (!Slot::is_empty(next_raw)) {
                return;
            }
            break;
        }

        size_t clear_idx = idx;
        for (size_t cleared = 0; cleared < run_len; ++cleared) {
            uint64_t expected = TOMBSTONE_SLOT;
            if (!slots[clear_idx].raw.compare_exchange_strong(expected, EMPTY_SLOT,
                                                              std::memory_order_acq_rel,
                                                              std::memory_order_acquire)) {
                return;
            }
            clear_idx = (clear_idx == 0) ? slot_mask : (clear_idx - 1);
        }
    }
};


struct WorkerStats {
    uint64_t reads = 0;
    uint64_t writes = 0;
    uint64_t fixes = 0;
    uint64_t restarts = 0;
    uint64_t pagefaults = 0;
    uint64_t waits = 0;
    uint64_t wait_io = 0;
    uint64_t wait_evict = 0;
    uint64_t wait_lock = 0;
    uint64_t restart_validation = 0;
    uint64_t restart_alloc = 0;
    uint64_t fiber_run = 0;
    uint64_t get_events = 0;
    uint64_t outstanding_io = 0;
    uint64_t num_submits = 0;
};

struct WorkerLocal {
    BufferManager* owner = nullptr;
    struct io_uring ring;
    std::unique_ptr<Reactor> reactor;
    SleepingFiber eviction_fiber;
    std::atomic<bool> stop_requested {false};
    boost::fibers::fiber_specific_ptr<uint64_t> my_id;
    StatsPrinter::Scope stats_scope;
    int worker_id = -1;
    std::vector<int> ssd_fds;
    uint64_t txn_count = 0;
    WorkerStats stats;
    std::vector<BID> local_free_list;
    std::vector<BID> evict_candidates;
    std::vector<BID> evict_retired;
    std::vector<BID> write_candidates;
    std::vector<std::vector<BID>> evict_pooled_freed;
    std::vector<Reactor::Fiber*> evict_to_wake;
    RestartReason restart_ctx = ValidationRestart {};
    // Per-worker physUsedCount accumulator: buffered increments/decrements that
    // have not yet been flushed to the global atomic. Reduces cache-line bouncing
    // when 64 workers each do physUsedCount++ on every page fault.
    int64_t phys_used_local_delta = 0;
};

struct WakeChannel {
    static constexpr size_t kCapacity = 4096;
    SpscRing<Reactor::Fiber*, kCapacity> ring;
};

struct WakeMatrix {
    std::vector<std::vector<std::unique_ptr<WakeChannel>>> queues;
    std::unique_ptr<std::atomic<uint64_t>[]> pending_by_dst;
    std::unique_ptr<std::atomic<uint64_t>[]> active_sources_by_dst;
    size_t worker_count = 0;
    size_t source_word_count = 0;

    WakeMatrix() = default;

    explicit WakeMatrix(size_t workers) {
        worker_count = workers;
        source_word_count = (workers + 63) / 64;
        queues.resize(workers);
        pending_by_dst = std::make_unique<std::atomic<uint64_t>[]>(workers);
        active_sources_by_dst = std::make_unique<std::atomic<uint64_t>[]>(workers * source_word_count);
        for (size_t dst = 0; dst < workers; ++dst) {
            pending_by_dst[dst].store(0, std::memory_order_relaxed);
            for (size_t word = 0; word < source_word_count; ++word) {
                active_sources_by_dst[dst * source_word_count + word].store(0, std::memory_order_relaxed);
            }
            queues[dst].resize(workers);
            for (size_t src = 0; src < workers; ++src) {
                queues[dst][src] = std::make_unique<WakeChannel>();
            }
        }
    }

    WakeChannel& channel(size_t src, size_t dst) {
        return *queues.at(dst).at(src);
    }

    std::atomic<uint64_t>& active_sources_word(size_t dst, size_t word_idx) {
        return active_sources_by_dst[dst * source_word_count + word_idx];
    }
};

struct BufferManager {
    static constexpr auto REG_BUF_SIZE = 1_GiB;

    bool do_log = false;
    Config cfg;

    u64 page_count;

    std::vector<int> blockfds;
    std::vector<uint32_t> device_nsids;
    std::vector<uint32_t> device_lba_shifts;
    SpinLock global_lock;
    std::unique_ptr<CacheLineLock[]> free_waiters_locks;

    AtomicCounter<u64> allocCount {1};              // pid 0 reserved for meta data
    alignas(64) AtomicCounter<u64> physUsedCount {1}; // metadata loaded — own cache line (written per-fault by all workers)

    // maps page_id (pid) to buffer_id (bid)
    std::unique_ptr<AtomicPageTable> page_table;
    std::unique_ptr<WakeMatrix> wake_matrix;
    BufferFrame* buffer_frames;
    PageState* resident_meta;
    Page* pages;

    // shared free physical slots (sharded pools), buffer-ids
    std::unique_ptr<FreePool[]> free_pools;
    size_t free_pool_count = 0;
    size_t free_pool_mask = 0;
    alignas(64) AtomicCounter<u64> shared_free_count {0};
    alignas(64) std::atomic<u64> evict_pages_inflight {0};
    std::atomic<u64> free_bid_wake_budget {0};
    std::vector<std::vector<Reactor::Fiber*>> free_frame_waiters_by_worker;

    std::vector<WorkerLocal*> active_workers;
    AtomicCounter<uint32_t> active_worker_count {0};

    void handleRestart();

    template <class Reason>
    inline void request_restart(Reason&& reason) {
        ++worker().stats.restarts;
        using ReasonT = std::decay_t<Reason>;
        if constexpr (std::is_same_v<ReasonT, ValidationRestart>) {
            ++worker().stats.restart_validation;
        } else if constexpr (std::is_same_v<ReasonT, AllocException>) {
            ++worker().stats.restart_alloc;
        }
        worker().restart_ctx = std::forward<Reason>(reason);
    }

    static constexpr jmp::static_branch<bool> sync_variant = false;
    static constexpr jmp::static_branch<bool> posix_variant = false;


    BufferManager();
    ~BufferManager() {}

    void init();
    void init_worker(int worker_id, int fiber_count);
    void shutdown_worker();
    void abandon_worker();
    void request_stop_active_workers();
    WorkerLocal& worker();
    const WorkerLocal& worker() const;
    uint64_t current_fiber_id() const;
    WorkerStats snapshot_worker_stats() const;
    uint64_t total_write_count() const;
    double buffer_load() const {
        return page_count > 0 ? (physUsedCount.load() / static_cast<double>(page_count)) : 0.0;
    }
    void reset_active_runtime_counters();

    struct SSDTarget {
        size_t ssd_idx;
        int fd;
        uint64_t offset;
        uint32_t nsid;
        uint32_t lba_shift;
    };
    SSDTarget ssd_target(PID pid) const;
    BID to_bid(const Page* page) const {
        ensure(page >= pages);
        ensure(page < pages + page_count);
        return static_cast<BID>(page - pages);
    }

    Page* fixX(PID pid);
    Page* fixX(PID pid, LockConflictPolicy wait_policy);
    Page* fixX(PID pid, BID bid, u64 expected_version, LockConflictPolicy wait_policy);
    void unfixX(Page* page);
    void unfixX_bid(BID bid);
    Page* fixS(PID pid);
    Page* fixS(PID pid, BID bid, u64 expected_version);
    void unfixS(Page* page);
    void unfixS_bid(BID bid);
    Page* upgradeToX(PID pid);
    Page* upgradeToX_bid(PID pid, BID bid);
    Page* fixO(PID pid, u64& version_out);
    bool validateO(PID pid, u64 version) const;
    bool validateO(PID pid, BID bid, u64 version) const;


    bool isValidPtr(void* page) {
        return (page >= pages) && (page < (pages + page_count));
    }

    size_t local_free_refill_target(size_t min_count) const {
        const size_t base = std::max<size_t>(static_cast<size_t>(cfg.evict_batch), 8);
        return std::max<size_t>(min_count, base);
    }

    size_t local_free_cap(bool prefer_local_waiters = false) const {
        const size_t base = std::max<size_t>(local_free_refill_target(1) * 4, 256);
        return prefer_local_waiters ? (base * 2) : base;
    }

    size_t local_free_low_watermark() const {
        return std::max<size_t>(local_free_refill_target(1) / 4, 8);
    }

    size_t free_pool_index_for_worker(uint32_t worker_id) const {
        return static_cast<size_t>(worker_id) & free_pool_mask;
    }

    size_t free_pool_index_for_bid(BID bid) const {
        return static_cast<size_t>(bid) & free_pool_mask;
    }

    bool refill_local_free_list(size_t min_count);
    BID acquire_free_bid();
    void release_free_bid(BID bid);

    // Accumulate delta into per-worker physUsedCount buffer; flush to global
    // atomic when |delta| reaches threshold, avoiding per-fault cache-line bouncing.
    static constexpr int64_t kPhysUsedFlushThreshold = 64;
    void accum_phys_used(int64_t d) noexcept {
        auto& delta = worker().phys_used_local_delta;
        delta += d;
        if (delta >= kPhysUsedFlushThreshold || delta <= -kPhysUsedFlushThreshold) {
            physUsedCount.fetch_add(static_cast<u64>(delta));
            delta = 0;
        }
    }
    void flush_phys_used_delta() noexcept {
        auto& delta = worker().phys_used_local_delta;
        if (delta != 0) {
            physUsedCount.fetch_add(static_cast<u64>(delta));
            delta = 0;
        }
    }

    bool wait_for_free_bid();
    void wake_free_bid_waiters_locked(uint32_t preferred_worker, std::vector<Reactor::Fiber*>& out);
    void wake_all_local_free_waiters();
    void wake_fiber(Reactor::Fiber* fiber);
    void drain_inbound_wakeups();
    void drain_inbound_wakeups(uint32_t worker_id);
    Page* allocPage();

    void handleFault(PID pid);
    void handleWait(PID pid, BID bid);
    enum class EvictResult {
        Idle,
        Progress,
    };
    EvictResult evict();


    // debug


    void dump_pt() {
        Logger::info("Dumping pt");
        std::ofstream f("page_table.csv");
        f << "key,bid,ideal_index,index\n";
        page_table->dump([&](PID pid, BID& bid, uint64_t ideal_index, uint64_t index) {
            // Logger::info("pid=", pid, " bid=", bid, " slot=", slot);
            f << pid << "," << bid << "," << ideal_index << "," << index << "\n";
        });
        Logger::info("Done");
    }
};


extern BufferManager bm;
