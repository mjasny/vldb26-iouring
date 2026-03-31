#include "bm.hpp"

#include "kuring.hpp"
#include "config.hpp"
#include "utils.hpp"
#include "utils/hugepages.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/nvme.hpp"
#include "utils/rdtsc_clock.hpp"

#include <boost/fiber/operations.hpp>
#include <chrono>
#include <execinfo.h>
#include <fcntl.h>
#include <libaio.h>
#include <liburing.h>
#include <cstring>
#include <sstream>
#include <sys/mman.h>
#include <unistd.h>

// Legacy probe outputs from utils/nvme.hpp. MT copies them into per-device vectors.
uint32_t nsid;
uint32_t lba_shift;

thread_local WorkerLocal* g_worker_local = nullptr;

namespace {
void drain_inbound_wakeups_hook(void* ctx) {
    auto* local = static_cast<WorkerLocal*>(ctx);
    ensure(local != nullptr);
    ensure(local->owner != nullptr);
    local->owner->drain_inbound_wakeups(static_cast<uint32_t>(local->worker_id));
}
}

namespace {
inline u64 resident_mode(const PageState& entry) {
    return PageState::get_state(entry.load());
}

inline bool resident_is_free_for_optimistic(const PageState& entry) {
    const u64 state = resident_mode(entry);
    return state != PageState::Locked && state != PageState::Evicted;
}

inline bool resident_prepare_optimistic(PageState& entry, u64& state_out) {
    while (true) {
        const u64 state = entry.load();
        const u64 mode = PageState::get_state(state);
        if (mode == PageState::Locked || mode == PageState::Evicted) {
            return false;
        }
        if (mode == PageState::Unlocked) {
            if (entry.try_mark(state)) {
                state_out = PageState::same_version(state, PageState::Marked);
                return true;
            }
            continue;
        }
        state_out = state;
        return true;
    }
}

inline u64 resident_version(const PageState& entry) {
    return PageState::get_version(entry.load());
}

inline bool resident_same_version(u64 lhs, u64 rhs) {
    return PageState::get_version(lhs) == PageState::get_version(rhs);
}

} // namespace


BufferManager::BufferManager() {
}

BufferManager::SSDTarget BufferManager::ssd_target(PID pid) const {
    ensure(!cfg.ssds.empty());
    const size_t ssd_idx = static_cast<size_t>(pid % cfg.ssds.size());
    const uint64_t offset = (pid / cfg.ssds.size()) * pageSize;
    const auto& local = worker();
    ensure(ssd_idx < local.ssd_fds.size());
    return {
        ssd_idx,
        local.ssd_fds[ssd_idx],
        offset,
        device_nsids.empty() ? 0u : device_nsids[ssd_idx],
        device_lba_shifts.empty() ? 0u : device_lba_shifts[ssd_idx],
    };
}

void BufferManager::init() {
    cfg = Config::get();
    const size_t worker_slots = static_cast<size_t>(std::max(cfg.num_workers, cfg.num_loaders));

    page_count = cfg.virt_size / pageSize; // physical slots we can keep in memory

    auto next_pow2 = [](u64 x) -> u64 {
        return 1ull << (64 - __builtin_clzl(x - 1));
    };

    uint64_t page_table_sz = next_pow2(page_count * cfg.page_table_factor);
    uint64_t owner_partition_count = 1;
    while (owner_partition_count < static_cast<uint64_t>(std::max<size_t>(1, worker_slots))) {
        owner_partition_count <<= 1;
    }
    owner_partition_count = std::min(owner_partition_count, page_table_sz);
    Logger::info("page_count=", page_count, " page_table_sz=", page_table_sz,
                 " ratio=", page_table_sz / static_cast<double>(page_count),
                 " page_table_owner_partitions=", owner_partition_count);
    page_table = std::make_unique<AtomicPageTable>(page_table_sz, owner_partition_count);
    wake_matrix = std::make_unique<WakeMatrix>(worker_slots);
    free_pool_count = next_pow2(std::max<u64>(1, static_cast<u64>(worker_slots)));
    free_pool_mask = free_pool_count - 1;
    free_pools = std::make_unique<FreePool[]>(free_pool_count);
    free_frame_waiters_by_worker.clear();
    free_frame_waiters_by_worker.resize(worker_slots);
    free_waiters_locks = std::make_unique<CacheLineLock[]>(worker_slots);
    buffer_frames = HugePages::malloc_array<BufferFrame>(page_count);
    resident_meta = HugePages::malloc_array<PageState>(page_count);
    pages = HugePages::malloc_array<Page>(page_count);

    int open_flags = O_DIRECT | O_RDWR;
    if (cfg.nvme_cmds) {
        open_flags &= ~O_DIRECT;
    }
    blockfds.clear();
    device_nsids.clear();
    device_lba_shifts.clear();
    blockfds.reserve(cfg.ssds.size());
    device_nsids.reserve(cfg.ssds.size());
    device_lba_shifts.reserve(cfg.ssds.size());
    for (const auto& path : cfg.ssds) {
        int fd = open(path.c_str(), open_flags, 0);
        check_ret(fd);
        blockfds.push_back(fd);
        if (cfg.nvme_cmds) {
            nvme_get_info(fd);
            device_nsids.push_back(nsid);
            device_lba_shifts.push_back(lba_shift);
        } else {
            device_nsids.push_back(0);
            device_lba_shifts.push_back(0);
        }
    }
    for (size_t i = 0; i < free_pool_count; ++i) {
        free_pools[i].bids.reserve((page_count / free_pool_count) + 64);
    }
    // distribute free physical slots across shared pools
    for (u64 i = 0; i < page_count; ++i) {
        BID bid = page_count - i - 1;
        if (bid == 0) {
            break; // metadata page
        }
        ensure(isValidPtr(pages + bid));
        free_pools[free_pool_index_for_bid(bid)].bids.push_back(bid);
        shared_free_count++;
    }
    // allocate and map metadata page 0 to physical 0
    PageState& buf = resident_meta[0];
    buf.init_unlocked(true);
    buf.set_dirty(true);
    bool inserted = page_table->insert(0, 0);
    ensure(inserted);

    for (u64 bid = 0; bid < page_count; ++bid) {
        new (&buffer_frames[bid]) BufferFrame(~0ull);
    }

    auto& frame = buffer_frames[0];
    new (&frame) BufferFrame(0);
}

void BufferManager::init_worker(int worker_id, int fiber_count) {
    ensure(!g_worker_local);
    g_worker_local = new WorkerLocal();
    auto* local = g_worker_local;
    local->owner = this;
    local->worker_id = worker_id;
    local->local_free_list.reserve(local_free_cap());
    local->evict_candidates.reserve(cfg.evict_batch);
    local->evict_retired.reserve(cfg.evict_batch);
    local->write_candidates.reserve(cfg.evict_batch);
    local->evict_pooled_freed.resize(free_pool_count);
    local->evict_to_wake.reserve(cfg.num_workers);

    auto next_pow2 = [](uint32_t x) -> uint32_t {
        uint32_t v = 1;
        while (v < x) {
            v <<= 1;
        }
        return v;
    };
    const bool scheduler_only_ring = posix_variant;
    const uint32_t sq_entries = scheduler_only_ring
        ? 8
        : next_pow2(std::max<uint32_t>(128, static_cast<uint32_t>(fiber_count * 8 + cfg.evict_batch * 2)));
    const uint32_t cq_entries = scheduler_only_ring ? 0 : (sq_entries * 4);

    // setup uring
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CLAMP;
    if (!scheduler_only_ring) {
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = cq_entries;
    }
    if (cfg.setup_mode == SetupMode::DEFER_TASKRUN) {
        params.flags |= IORING_SETUP_DEFER_TASKRUN;
    }
    if (cfg.setup_mode == SetupMode::SQPOLL) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 1000;
        if (cfg.core_id != -1) {
            auto& cpu_map = CPUMap::get();
            const size_t core_index = static_cast<size_t>(cfg.core_id) + static_cast<size_t>(worker_id) + 1;
            params.sq_thread_cpu = static_cast<uint32_t>(core_index % cpu_map.total_cores);
            params.flags |= IORING_SETUP_SQ_AFF;
        }
    }
    if (cfg.setup_mode == SetupMode::COOP_TASKRUN) {
        params.flags |= IORING_SETUP_COOP_TASKRUN;
    }
    if (cfg.iopoll) {
        params.flags |= IORING_SETUP_IOPOLL;
    }
    if (cfg.nvme_cmds) {
        params.flags |= IORING_SETUP_CQE32 | IORING_SETUP_SQE128;
    }

    auto res = io_uring_queue_init_params(sq_entries, &local->ring, &params);
    if (res < 0) {
        throw std::system_error(-res, std::system_category());
    }

    if (cfg.reg_ring) {
        ensure(io_uring_register_ring_fd(&local->ring) == 1);
    }


    std::vector<int> local_ssd_fds = blockfds;
    if (cfg.reg_fds) {
        check_iou(io_uring_register_files_sparse(&local->ring, 1024));
        check_iou(io_uring_register_files_update(&local->ring, /*off*/ 0, local_ssd_fds.data(), local_ssd_fds.size()));
        for (size_t i = 0; i < local_ssd_fds.size(); ++i) {
            local_ssd_fds[i] = static_cast<int>(i);
        }
    }

    if (cfg.reg_bufs) {
        uint64_t mem_size = page_count * pageSize;
        uint32_t num_bufs = (mem_size + REG_BUF_SIZE - 1) / REG_BUF_SIZE;
        std::vector<struct iovec> iov(num_bufs);
        size_t offset = 0;
        for (uint32_t i = 0; i < num_bufs; ++i) {
            size_t len = std::min(REG_BUF_SIZE, mem_size - offset);
            iov[i].iov_base = reinterpret_cast<uint8_t*>(pages) + offset;
            iov[i].iov_len = len;
            offset += len;
        }
        check_iou(io_uring_register_buffers(&local->ring, iov.data(), iov.size()));
    }

    local->reactor = std::make_unique<Reactor>(local->ring);
    local->reactor->set_owner_worker(worker_id);
    local->reactor->set_wake_drain_hook(local, &drain_inbound_wakeups_hook);
    {
        SpinGuard guard(global_lock);
        const size_t worker_slot = static_cast<size_t>(worker_id) + 1;
        if (active_workers.size() < worker_slot) {
            active_workers.resize(worker_slot, nullptr);
        }
        active_workers.at(static_cast<size_t>(worker_id)) = local;
        active_worker_count.fetch_add(1);
    }
    auto& stats = StatsPrinter::get();
    stats.register_aggr(local->stats_scope, local->txn_count, "tps");
    stats.register_aggr(local->stats_scope, local->stats.reads, "reads");
    stats.register_aggr(local->stats_scope, local->stats.writes, "writes");
    stats.register_aggr(local->stats_scope, local->stats.fixes, "fixes");
    stats.register_aggr(local->stats_scope, local->stats.restarts, "restarts");
    stats.register_aggr(local->stats_scope, local->stats.pagefaults, "pagefaults");
    stats.register_aggr(local->stats_scope, local->stats.waits, "waits");
    stats.register_aggr(local->stats_scope, local->stats.wait_io, "wait_io");
    stats.register_aggr(local->stats_scope, local->stats.wait_evict, "wait_evict");
    stats.register_aggr(local->stats_scope, local->stats.wait_lock, "wait_lock");
    stats.register_aggr(local->stats_scope, local->stats.restart_validation, "restart_val");
    stats.register_aggr(local->stats_scope, local->stats.restart_alloc, "restart_alloc");
    stats.register_aggr(local->stats_scope, local->reactor->fiber_run, "fiber_run");
    stats.register_aggr(local->stats_scope, local->reactor->get_events, "get_events");
    stats.register_aggr(local->stats_scope, local->reactor->num_submits, "num_submits");
    stats.register_aggr(local->stats_scope, local->reactor->outstanding_io, "io_out", false);
    mini::set_reactor(*local->reactor);
    local->reactor->total_io_fibers = fiber_count;
    local->ssd_fds = std::move(local_ssd_fds);

    local->eviction_fiber.spawn(
        [local] {
            local->my_id.reset(new uint64_t{0xfe}); // special id for evictor
        },
        [this, local] {
            auto below_free_target = [this, local]() {
                // Include this worker's unbuffered phys_used delta so the eviction
                // trigger is accurate even when physUsedCount hasn't been flushed yet.
                const u64 effective_used = physUsedCount.load()
                    + static_cast<u64>(local->phys_used_local_delta);
                const u64 global_free = page_count - effective_used;
                return (global_free + evict_pages_inflight.load(std::memory_order_relaxed)) <= page_count * cfg.free_target;
            };
            if (!below_free_target()) {
                return true; // park
            }

            switch (evict()) {
                case EvictResult::Progress:
                    return false;
                case EvictResult::Idle:
                    break;
            }
            return true;
        });
}

void BufferManager::shutdown_worker() {
    if (!g_worker_local) {
        return;
    }
    flush_phys_used_delta();
    {
        SpinGuard guard(global_lock);
        const auto idx = static_cast<size_t>(g_worker_local->worker_id);
        if (idx < active_workers.size() && active_workers[idx] == g_worker_local) {
            active_workers[idx] = nullptr;
            active_worker_count.fetch_sub(1);
        }
    }
    delete g_worker_local;
    g_worker_local = nullptr;
}

void BufferManager::abandon_worker() {
    if (!g_worker_local) {
        return;
    }
    {
        SpinGuard guard(global_lock);
        const auto idx = static_cast<size_t>(g_worker_local->worker_id);
        if (idx < active_workers.size() && active_workers[idx] == g_worker_local) {
            active_workers[idx] = nullptr;
            active_worker_count.fetch_sub(1);
        }
    }
    static std::vector<WorkerLocal*> leaked_workers;
    leaked_workers.push_back(g_worker_local);
    g_worker_local = nullptr;
}

void BufferManager::request_stop_active_workers() {
    SpinGuard guard(global_lock);
    for (auto* local : active_workers) {
        if (local != nullptr) {
            local->stop_requested.store(true, std::memory_order_release);
        }
    }
}

WorkerLocal& BufferManager::worker() {
    ensure(g_worker_local != nullptr, "g_worker_local is null");
    return *g_worker_local;
}

const WorkerLocal& BufferManager::worker() const {
    ensure(g_worker_local != nullptr, "g_worker_local is null");
    return *g_worker_local;
}

uint64_t BufferManager::current_fiber_id() const {
    if (!g_worker_local || !g_worker_local->my_id.get()) {
        return ~0ull;
    }
    return *g_worker_local->my_id;
}

WorkerStats BufferManager::snapshot_worker_stats() const {
    WorkerStats total;
    SpinGuard guard(const_cast<SpinLock&>(global_lock));
    for (auto* worker_local : active_workers) {
        if (!worker_local) {
            continue;
        }
        total.reads += worker_local->stats.reads;
        total.writes += worker_local->stats.writes;
        total.fixes += worker_local->stats.fixes;
        total.restarts += worker_local->stats.restarts;
        total.pagefaults += worker_local->stats.pagefaults;
        total.waits += worker_local->stats.waits;
        total.wait_io += worker_local->stats.wait_io;
        total.wait_evict += worker_local->stats.wait_evict;
        total.wait_lock += worker_local->stats.wait_lock;
        total.restart_validation += worker_local->stats.restart_validation;
        total.restart_alloc += worker_local->stats.restart_alloc;
        total.fiber_run += worker_local->reactor ? worker_local->reactor->fiber_run : 0;
        total.get_events += worker_local->reactor ? worker_local->reactor->get_events : 0;
        total.outstanding_io += worker_local->reactor ? worker_local->reactor->outstanding_io : 0;
        total.num_submits += worker_local->reactor ? worker_local->reactor->num_submits : 0;
    }
    return total;
}

uint64_t BufferManager::total_write_count() const {
    return snapshot_worker_stats().writes;
}

void BufferManager::reset_active_runtime_counters() {
    auto& stats = StatsPrinter::get();
    SpinGuard guard(global_lock);
    for (auto* worker_local : active_workers) {
        if (!worker_local) {
            continue;
        }
        worker_local->txn_count = 0;
        worker_local->stats = {};
        if (worker_local->reactor) {
            worker_local->reactor->fiber_run = 0;
            worker_local->reactor->get_events = 0;
            worker_local->reactor->num_submits = 0;
        }
        stats.reset_scope(worker_local->stats_scope);
    }
}

void BufferManager::wake_fiber(Reactor::Fiber* fiber) {
    if (!fiber) {
        return;
    }

    const uint32_t dst = fiber->owner_worker;
    const uint32_t src = static_cast<uint32_t>(worker().worker_id);
    if (dst == src) {
        mini::wake(fiber);
        return;
    }

    auto& channel = wake_matrix->channel(src, dst).ring;
    while (!channel.push(fiber)) {
        __builtin_ia32_pause();
    }
    const size_t word_idx = src / 64;
    const uint64_t bit = 1ull << (src % 64);
    wake_matrix->active_sources_word(dst, word_idx).fetch_or(bit, std::memory_order_release);
    wake_matrix->pending_by_dst[dst].fetch_add(1, std::memory_order_release);
}

void BufferManager::drain_inbound_wakeups() {
    drain_inbound_wakeups(static_cast<uint32_t>(worker().worker_id));
}

void BufferManager::drain_inbound_wakeups(uint32_t worker_id) {
    WorkerLocal* local = g_worker_local;
    ensure(local != nullptr);
    auto my_worker = static_cast<size_t>(worker_id);
    auto& pending = wake_matrix->pending_by_dst[my_worker];
    if (pending.load(std::memory_order_acquire) == 0) {
        return;
    }

    uint64_t drained = 0;
    for (size_t word_idx = 0; word_idx < wake_matrix->source_word_count; ++word_idx) {
        uint64_t active = wake_matrix->active_sources_word(my_worker, word_idx).exchange(0, std::memory_order_acq_rel);
        while (active != 0) {
            const unsigned bit_idx = static_cast<unsigned>(__builtin_ctzll(active));
            const size_t src = word_idx * 64 + bit_idx;
            active &= active - 1;
            if (src >= wake_matrix->worker_count) {
                continue;
            }
            auto& ring = wake_matrix->channel(src, my_worker).ring;
            Reactor::Fiber* fiber = nullptr;
            while (ring.pop(fiber)) {
                mini::wake(fiber);
                ++drained;
            }
        }
    }
    if (drained > 0) {
        pending.fetch_sub(drained, std::memory_order_acq_rel);
    }
}

bool BufferManager::refill_local_free_list(size_t min_count) {
    auto& local = worker().local_free_list;
    if (local.size() >= min_count) {
        return true;
    }

    const size_t refill_target = local_free_refill_target(min_count);
    const size_t own_idx = free_pool_index_for_worker(worker().worker_id);
    const auto refill_from_pool = [&](size_t pool_idx) {
        auto& pool = free_pools[pool_idx];
        SpinGuard guard(pool.lock);
        const size_t take = std::min(refill_target - local.size(), pool.bids.size());
        if (take == 0) {
            return;
        }
        const size_t old_size = local.size();
        local.resize(old_size + take);
        std::copy(pool.bids.end() - take, pool.bids.end(), local.begin() + old_size);
        pool.bids.resize(pool.bids.size() - take);
        shared_free_count -= take;
    };

    refill_from_pool(own_idx);
    if (local.size() < refill_target) {
        for (size_t step = 1; step < free_pool_count && local.size() < refill_target; ++step) {
            refill_from_pool((own_idx + step) & free_pool_mask);
        }
    }

    if (shared_free_count.load() + local.size() <= page_count * cfg.free_target) {
        worker().eviction_fiber.wakeup();
    }
    return local.size() >= min_count;
}

void BufferManager::wake_free_bid_waiters_locked(uint32_t preferred_worker, std::vector<Reactor::Fiber*>& out) {
    const size_t worker_count = free_frame_waiters_by_worker.size();
    if (worker_count == 0) {
        return;
    }

    // Atomically claim the full wake budget so concurrent callers don't each
    // spend the same budget and collectively over-wake more fibers than frames.
    u64 budget = free_bid_wake_budget.load(std::memory_order_relaxed);
    size_t remaining = 0;
    while (budget > 0) {
        if (free_bid_wake_budget.compare_exchange_weak(budget, 0,
                                                       std::memory_order_acq_rel,
                                                       std::memory_order_relaxed)) {
            remaining = static_cast<size_t>(budget);
            break;
        }
    }
    if (remaining == 0) {
        return;
    }

    auto drain_queue = [&](size_t worker_idx) {
        SpinGuard guard(free_waiters_locks[worker_idx].lock);
        auto& queue = free_frame_waiters_by_worker[worker_idx];
        const size_t wake_count = std::min(remaining, queue.size());
        for (size_t i = 0; i < wake_count; ++i) {
            out.push_back(queue.back());
            queue.pop_back();
        }
        remaining -= wake_count;
    };

    drain_queue(static_cast<size_t>(preferred_worker) % worker_count);
    for (size_t step = 1; step < worker_count && remaining > 0; ++step) {
        drain_queue((static_cast<size_t>(preferred_worker) + step) % worker_count);
    }

    // Refund budget that wasn't consumed (fewer waiters than claimed budget).
    if (remaining > 0) {
        free_bid_wake_budget.fetch_add(remaining, std::memory_order_relaxed);
    }
}

bool BufferManager::wait_for_free_bid() {
    Reactor::Fiber* self = mini::R->current();
    ensure(self);
    {
        SpinGuard guard(free_waiters_locks[static_cast<size_t>(worker().worker_id)].lock);
        if (shared_free_count.load() > 0) {
            return false;
        }
        free_frame_waiters_by_worker[static_cast<size_t>(worker().worker_id)].push_back(self);
    }
    mini::park();
    return true;
}

void BufferManager::wake_all_local_free_waiters() {
    std::vector<Reactor::Fiber*> to_wake;
    {
        SpinGuard guard(free_waiters_locks[static_cast<size_t>(worker().worker_id)].lock);
        auto& local_waiters = free_frame_waiters_by_worker[static_cast<size_t>(worker().worker_id)];
        to_wake.swap(local_waiters);
    }
    for (auto* fiber : to_wake) {
        mini::wake(fiber);
    }
}

void BufferManager::release_free_bid(BID bid) {
    auto& local = worker().local_free_list;
    std::vector<Reactor::Fiber*> to_wake;
    bool prefer_local_waiters = false;
    SpinLock& my_waiters_lock = free_waiters_locks[static_cast<size_t>(worker().worker_id)].lock;
    {
        SpinGuard waiters_guard(my_waiters_lock);
        auto& local_waiters = free_frame_waiters_by_worker[static_cast<size_t>(worker().worker_id)];
        prefer_local_waiters = !local_waiters.empty();
    }

    if (local.size() < local_free_cap(prefer_local_waiters)) {
        local.push_back(bid);
        if (prefer_local_waiters) {
            SpinGuard waiters_guard(my_waiters_lock);
            auto& local_waiters = free_frame_waiters_by_worker[static_cast<size_t>(worker().worker_id)];
            const size_t wake_count = std::min(local.size(), local_waiters.size());
            for (size_t i = 0; i < wake_count; ++i) {
                to_wake.push_back(local_waiters.back());
                local_waiters.pop_back();
            }
        }
        for (auto* fiber : to_wake) {
            wake_fiber(fiber);
        }
        return;
    }

    auto& pool = free_pools[free_pool_index_for_bid(bid)];
    {
        SpinGuard pool_guard(pool.lock);
        pool.bids.push_back(bid);
        shared_free_count++;
    }

    to_wake.clear();
    free_bid_wake_budget.fetch_add(1, std::memory_order_relaxed);
    wake_free_bid_waiters_locked(static_cast<uint32_t>(worker().worker_id), to_wake);
    for (auto* fiber : to_wake) {
        wake_fiber(fiber);
    }

    if (shared_free_count.load() <= page_count * cfg.free_target) {
        worker().eviction_fiber.wakeup();
    }
}

BID BufferManager::acquire_free_bid() {
    auto& local = worker().local_free_list;
    if (local.empty() && !refill_local_free_list(1)) {
        return 0;
    }
    if (local.size() == local_free_low_watermark()) {
        refill_local_free_list(local_free_refill_target(1));
    }
    ensure(!local.empty());
    BID bid = local.back();
    local.pop_back();
    return bid;
}

// allocated new page and fix it
Page* BufferManager::allocPage() {
    BID bid = acquire_free_bid();
    if (bid == 0) {
        request_restart(AllocException {});
        return nullptr;
    }
    accum_phys_used(1);

    // assign a new logical PID
    PID pid = allocCount++;
    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " alloc pid=", pid, " bid=", bid);

    PageState& buf = resident_meta[bid];
    buf.rebind_locked();
    buf.set_dirty(true);

    auto& frame = buffer_frames[bid];
    new (&frame) BufferFrame(pid);

    try {
        bool inserted = page_table->insert(pid, bid);
        ensure(inserted);
    } catch (...) {
        dump_pt();
        throw;
    }

    auto* page = pages + bid;
    std::memset(page, 0, pageSize);

    ensure(isValidPtr(page));
    return page;
}

void BufferManager::handleFault(PID pid) {
    BID existing_bid = 0;
    if (page_table->find_copy(pid, existing_bid)) {
        auto& existing = resident_meta[existing_bid];
        if (existing.io_in_progress()) {
            handleWait(pid, existing_bid);
        } else if (existing.evicting()) {
            mini::yield();
        }
        return;
    }

    while (!refill_local_free_list(1)) {
        wait_for_free_bid();
        if (page_table->find_copy(pid, existing_bid)) {
            auto& existing = resident_meta[existing_bid];
            if (existing.io_in_progress()) {
                handleWait(pid, existing_bid);
            } else if (existing.evicting()) {
                mini::yield();
            }
            return;
        }
    }


    BID bid = acquire_free_bid();
    ensure(bid != 0);
    accum_phys_used(1);

    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " read pid=", pid, " bid=", bid);


    PageState& buf = resident_meta[bid];
    buf.rebind_locked();
    buf.set_io_in_progress(true);

    auto& frame = buffer_frames[bid];
    new (&frame) BufferFrame(pid);

    bool inserted = page_table->insert(pid, bid);
    if (!inserted) {
        buf.rebind_unlocked();
        accum_phys_used(-1);
        release_free_bid(bid);
        BID winner_bid = 0;
        if (page_table->find_copy(pid, winner_bid)) {
            auto& winner_meta = resident_meta[winner_bid];
            if (winner_meta.io_in_progress()) {
                ++worker().stats.waits;
                ++worker().stats.wait_io;
                handleWait(pid, winner_bid);
                return;
            }
            if (winner_meta.evicting()) {
                ++worker().stats.waits;
                ++worker().stats.wait_evict;
                mini::yield();
                return;
            }
            return;
        }
        mini::yield();
        return;
    }
    // Logger::info("fiber=", *my_id, " inserted pid=", pid);


    auto* page = pages + bid;
    auto prep_sqe = [&](struct io_uring_sqe* sqe) {
        const auto target = ssd_target(pid);

        if (cfg.nvme_cmds) {
            prep_nvme_read(sqe, target.fd, page, pageSize, target.offset, target.nsid, target.lba_shift);
            if (cfg.reg_bufs) {
                int buf_idx = (bid * pageSize) / REG_BUF_SIZE;
                sqe->uring_cmd_flags |= IORING_URING_CMD_FIXED;
                sqe->buf_index = buf_idx;
            }
        } else if (!cfg.reg_bufs) {
            io_uring_prep_read(sqe, target.fd, page, pageSize, target.offset);
        } else {
            int buf_idx = (bid * pageSize) / REG_BUF_SIZE;
            io_uring_prep_read_fixed(sqe, target.fd, page, pageSize, target.offset, buf_idx);
        }

        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
    };


    auto prep_libaio = [&](struct iocb* cb) {
        const auto target = ssd_target(pid);
        io_prep_pread(cb, target.fd, page, pageSize, target.offset);
    };

    mini::Op* waiters = nullptr;
    if (sync_variant) {
        frame.begin_waiters();

        RDTSCClock clock(2.4_GHz);
        clock.start();

        if (posix_variant) {
            const auto target = ssd_target(pid);
            ensure(pread(target.fd, page, pageSize, target.offset) == pageSize);
        } else {
            struct io_uring_sqe* sqe = io_uring_get_sqe(&worker().ring);
            check_ptr(sqe);
            prep_sqe(sqe);

            int left = 1;
            while (true) {
                io_uring_submit_and_wait(&worker().ring, left);

                int i = 0;
                uint32_t head;
                struct io_uring_cqe* cqe;
                io_uring_for_each_cqe(&worker().ring, head, cqe) {
                    ++i;
                    check_iou(cqe->res);
                    if (!cfg.nvme_cmds) {
                        ensure(cqe->res == pageSize);
                    }
                }
                io_uring_cq_advance(&worker().ring, i);
                left -= i;
                if (left == 0) {
                    break;
                }
            }
        }


        clock.stop();
        io_cycles += clock.cycles();

        waiters = frame.close_waiters();

    } else {

        mini::Op op;
        frame.begin_waiters();
        int rc;
        if constexpr (mini::LIBAIO) {
            rc = mini::io(op, prep_libaio);
        } else {
            rc = mini::io(op, prep_sqe);
        }
        if (!cfg.nvme_cmds) {
            ensure(rc == pageSize);
        }

        waiters = frame.close_waiters();
    }


    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " read done pid=", pid, " bid=", bid);

    auto& meta = resident_meta[bid];
    meta.set_io_in_progress(false);
    meta.unlock_x_marked();

    while (waiters) {
        auto* next = waiters->next;
        wake_fiber(waiters->ctx);
        waiters = next;
    }

    ++worker().stats.reads;
}


// helper type for the visitor
template <class... Ts>
struct overloads : Ts... {
    using Ts::operator()...;
};


void BufferManager::handleRestart() {
    auto& restart_ctx = worker().restart_ctx;
    std::visit(overloads{
                   [&](PageFaultException& e) {
                       bm.handleFault(e.pid);
                   },
                   [&](RestartException& e) {
                       bm.handleWait(e.pid, e.bid);
                   },
                   [&](AllocException&) {
                       mini::yield();
                   },
                   [&](ValidationRestart&) {
                       mini::yield();
                   },
               },
               restart_ctx);
}


void BufferManager::handleWait(PID pid, BID bid) {
    auto& frame = buffer_frames[bid];
    if (frame.pid != pid || !resident_meta[bid].io_in_progress()) {
        mini::yield();
        return;
    }

    mini::Op op;
    op.ctx = mini::current();
    if (!frame.enqueue_waiter(op)) {
        mini::yield();
        return;
    }

    // After successful publication, the stack-allocated waiter must stay alive
    // until the completer detaches and walks the waiter list.
    mini::park();
}

Page* BufferManager::fixS(PID pid) {
    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " fixS pid=", pid);

    ++worker().stats.fixes;

    if (pid >= allocCount.load()) {
        request_restart(ValidationRestart {});
        return nullptr;
    }

    for (;;) {
        BID bid = 0;
        if (!page_table->find_copy(pid, bid)) {
            ++worker().stats.pagefaults;
            handleFault(pid);
            continue;
        }

        auto& meta = resident_meta[bid];
        if (buffer_frames[bid].pid != pid) {
            mini::yield();
            continue;
        }
        u64 state = meta.load();
        if (buffer_frames[bid].pid != pid) {
            mini::yield();
            continue;
        }
        if (meta.io_in_progress()) {
            ++worker().stats.waits;
            ++worker().stats.wait_io;
            handleWait(pid, bid);
            continue;
        }
        if (meta.evicting()) {
            ++worker().stats.waits;
            ++worker().stats.wait_evict;
            mini::yield();
            continue;
        }
        if (!meta.try_lock_s(state)) {
            ++worker().stats.waits;
            ++worker().stats.wait_lock;
            mini::yield();
            continue;
        }
        if (buffer_frames[bid].pid != pid) {
            meta.unlock_s();
            mini::yield();
            continue;
        }
        Page* page = pages + bid;
        ensure(isValidPtr(page));
        return page;
    }
}

Page* BufferManager::fixS(PID pid, BID bid, u64 expected_version) {
    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " fixS pid=", pid, " bid=", bid);

    ++worker().stats.fixes;

    if (pid >= allocCount.load()) {
        request_restart(ValidationRestart {});
        return nullptr;
    }

    auto& meta = resident_meta[bid];
    if (buffer_frames[bid].pid != pid) {
        request_restart(ValidationRestart {});
        return nullptr;
    }
    u64 state = meta.load();
    if (!resident_same_version(state, expected_version)) {
        request_restart(ValidationRestart {});
        return nullptr;
    }
    if (buffer_frames[bid].pid != pid) {
        request_restart(ValidationRestart {});
        return nullptr;
    }
    if (meta.io_in_progress() || meta.evicting()) {
        request_restart(ValidationRestart {});
        return nullptr;
    }
    if (!meta.try_lock_s(state)) {
        request_restart(ValidationRestart {});
        return nullptr;
    }
    if (buffer_frames[bid].pid != pid) {
        meta.unlock_s();
        request_restart(ValidationRestart {});
        return nullptr;
    }
    Page* page = pages + bid;
    ensure(isValidPtr(page));
    return page;
}


Page* BufferManager::fixX(PID pid) {
    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " fixX pid=", pid);

    ++worker().stats.fixes;

    if (pid >= allocCount.load()) {
        request_restart(ValidationRestart {});
        return nullptr;
    }

    for (;;) {
        BID bid = 0;
        if (!page_table->find_copy(pid, bid)) {
            ++worker().stats.pagefaults;
            handleFault(pid);
            continue;
        }

        auto& meta = resident_meta[bid];
        if (buffer_frames[bid].pid != pid) {
            mini::yield();
            continue;
        }

        u64 state = meta.load();
        if (buffer_frames[bid].pid != pid) {
            mini::yield();
            continue;
        }
        if (meta.io_in_progress()) {
            ++worker().stats.waits;
            ++worker().stats.wait_io;
            handleWait(pid, bid);
            continue;
        }
        if (meta.evicting()) {
            ++worker().stats.waits;
            ++worker().stats.wait_evict;
            mini::yield();
            continue;
        }
        if (!meta.try_lock_x_dirty(state)) {
            ++worker().stats.waits;
            ++worker().stats.wait_lock;
            mini::yield();
            continue;
        }
        if (buffer_frames[bid].pid != pid) {
            meta.unlock_x();
            mini::yield();
            continue;
        }
        Page* page = pages + bid;
        ensure(isValidPtr(page));
        return page;
    }
}

Page* BufferManager::fixX(PID pid, LockConflictPolicy wait_policy) {
    if (wait_policy == LockConflictPolicy::Wait) {
        return fixX(pid);
    }

    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " fixX pid=", pid);

    ++worker().stats.fixes;

    if (pid >= allocCount.load()) {
        request_restart(ValidationRestart {});
        return nullptr;
    }

    for (;;) {
        BID bid = 0;
        if (!page_table->find_copy(pid, bid)) {
            request_restart(PageFaultException {pid});
            return nullptr;
        }

        auto& meta = resident_meta[bid];
        if (buffer_frames[bid].pid != pid) {
            request_restart(ValidationRestart {});
            return nullptr;
        }
        u64 state = meta.load();
        if (buffer_frames[bid].pid != pid) {
            request_restart(ValidationRestart {});
            return nullptr;
        }
        if (meta.io_in_progress() || meta.evicting()) {
            ++worker().stats.waits;
            ++worker().stats.wait_lock;
            request_restart(ValidationRestart {});
            return nullptr;
        }
        if (!meta.try_lock_x_dirty(state)) {
            ++worker().stats.waits;
            ++worker().stats.wait_lock;
            request_restart(ValidationRestart {});
            return nullptr;
        }
        if (buffer_frames[bid].pid != pid) {
            meta.unlock_x();
            request_restart(ValidationRestart {});
            return nullptr;
        }
        Page* page = pages + bid;
        ensure(isValidPtr(page));
        return page;
    }
}

Page* BufferManager::fixX(PID pid, BID bid, u64 expected_version, LockConflictPolicy wait_policy) {
    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " fixX pid=", pid, " bid=", bid);

    ++worker().stats.fixes;

    if (pid >= allocCount.load()) {
        request_restart(ValidationRestart {});
        return nullptr;
    }

    for (;;) {
        auto& meta = resident_meta[bid];
        if (buffer_frames[bid].pid != pid) {
            request_restart(ValidationRestart {});
            return nullptr;
        }
        u64 state = meta.load();
        if (expected_version != std::numeric_limits<u64>::max() && !resident_same_version(state, expected_version)) {
            request_restart(ValidationRestart {});
            return nullptr;
        }
        if (buffer_frames[bid].pid != pid) {
            request_restart(ValidationRestart {});
            return nullptr;
        }
        if (meta.io_in_progress() || meta.evicting()) {
            ++worker().stats.waits;
            ++worker().stats.wait_lock;
            if (wait_policy == LockConflictPolicy::Restart) {
                request_restart(ValidationRestart {});
                return nullptr;
            }
            mini::yield();
            continue;
        }
        if (!meta.try_lock_x_dirty(state)) {
            ++worker().stats.waits;
            ++worker().stats.wait_lock;
            if (wait_policy == LockConflictPolicy::Restart) {
                request_restart(ValidationRestart {});
                return nullptr;
            }
            mini::yield();
            continue;
        }
        if (buffer_frames[bid].pid != pid) {
            meta.unlock_x();
            request_restart(ValidationRestart {});
            return nullptr;
        }
        Page* page = pages + bid;
        ensure(isValidPtr(page));
        return page;
    }
}

Page* BufferManager::fixO(PID pid, u64& version_out) {
    if (pid >= allocCount.load()) {
        request_restart(ValidationRestart {});
        return nullptr;
    }

    for (;;) {
        BID bid = 0;
        if (!page_table->find_copy(pid, bid)) {
            ++worker().stats.pagefaults;
            handleFault(pid);
            continue;
        }

        auto& meta = resident_meta[bid];
        if (buffer_frames[bid].pid != pid) {
            mini::yield();
            continue;
        }
        if (meta.io_in_progress()) {
            ++worker().stats.waits;
            ++worker().stats.wait_io;
            handleWait(pid, bid);
            continue;
        }
        if (meta.evicting()) {
            ++worker().stats.waits;
            ++worker().stats.wait_evict;
            mini::yield();
            continue;
        }
        u64 state = 0;
        if (!resident_prepare_optimistic(meta, state)) {
            ++worker().stats.waits;
            ++worker().stats.wait_lock;
            mini::yield();
            continue;
        }
        if (buffer_frames[bid].pid != pid) {
            mini::yield();
            continue;
        }
        version_out = state;
        Page* page = pages + bid;
        ensure(isValidPtr(page));
        return page;
    }
}

bool BufferManager::validateO(PID pid, u64 version) const {
    BID bid = 0;
    if (!const_cast<AtomicPageTable&>(*page_table).find_copy(pid, bid)) {
        return false;
    }
    return validateO(pid, bid, version);
}

bool BufferManager::validateO(PID pid, BID bid, u64 version) const {
    if (buffer_frames[bid].pid != pid) {
        return false;
    }
    auto& meta = resident_meta[bid];
    if (meta.io_in_progress() || meta.evicting()) {
        return false;
    }
    const u64 current = meta.load();
    if (current == version) {
        return true;
    }
    if (!resident_same_version(current, version)) {
        return false;
    }
    const u64 state = PageState::get_state(current);
    return state <= PageState::MaxShared || state == PageState::Marked;
}

Page* BufferManager::upgradeToX(PID pid) {
    request_restart(ValidationRestart {});
    return nullptr;
}

Page* BufferManager::upgradeToX_bid(PID pid, BID bid) {
    (void)pid;
    (void)bid;
    request_restart(ValidationRestart {});
    return nullptr;
}


void BufferManager::unfixS(Page* page) {
    const BID bid = to_bid(page);
    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " unfixS bid=", bid);
    unfixS_bid(bid);
}

void BufferManager::unfixS_bid(BID bid) {
    auto& meta = resident_meta[bid];
    const u64 state = resident_mode(meta);
    ensure(state >= 1 && state <= PageState::MaxShared);
    meta.unlock_s_marked();
}

void BufferManager::unfixX(Page* page) {
    const BID bid = to_bid(page);
    if (do_log)
        Logger::info("fiber=", current_fiber_id(), " unfixX bid=", bid);
    unfixX_bid(bid);
}

void BufferManager::unfixX_bid(BID bid) {
    auto& meta = resident_meta[bid];
    ensure(resident_mode(meta) == PageState::Locked);
    meta.set_dirty(true);
    meta.unlock_x_marked();
}


BufferManager::EvictResult BufferManager::evict() {
    auto& toEvict = worker().evict_candidates;
    auto& toWrite = worker().write_candidates;
    auto& freed = worker().evict_retired;
    toEvict.clear();
    toWrite.clear();
    freed.clear();

    const u64 free_target = static_cast<u64>(page_count * cfg.free_target);
    u64 reserved_budget = 0;
    while (true) {
        const u64 current_free = page_count - physUsedCount.load();
        u64 inflight = evict_pages_inflight.load(std::memory_order_relaxed);
        if (current_free + inflight >= free_target) {
            return EvictResult::Idle;
        }
        const u64 deficit = free_target - (current_free + inflight);
        reserved_budget = std::max<u64>(1, std::min<u64>(cfg.evict_batch, deficit));
        if (evict_pages_inflight.compare_exchange_weak(inflight, inflight + reserved_budget,
                                                       std::memory_order_acq_rel,
                                                       std::memory_order_relaxed)) {
            break;
        }
    }

    uint64_t visited = 0;

    write_clock.start();

    auto sweep_cb = [&](PID pid, BID& bid) {
        auto& buf = resident_meta[bid];
        auto& frame = buffer_frames[bid];
        if (frame.pid != pid) {
            return false;
        }

        visited++;
        const u64 state = resident_mode(buf);
        if (state != PageState::Unlocked && state != PageState::Marked) {
            return false;
        }
        if (buf.io_in_progress()) {
            return false;
        }
        if (buf.evicting()) {
            return false;
        }

        // second chance eviction
        if (state == PageState::Marked) {
            const u64 expected = buf.load();
            buf.try_clear_mark(expected);
            return false;
        }
        if (pid == 0) {
            return false;
        }

        buf.set_evicting(true);

        if (buf.dirty()) {
            buf.set_dirty(false);
            toWrite.push_back(bid);
        } else {
            toEvict.push_back(bid);
        }
        bool done = (toEvict.size() + toWrite.size()) == reserved_budget;
        return done;
    };
    const size_t owner_count = std::max<size_t>(1, active_worker_count.load());
    const size_t owner_idx = static_cast<size_t>(worker().worker_id) % owner_count;
    bool exhausted = page_table->clock_sweep_next_owned(owner_idx, owner_count, static_cast<size_t>(cfg.evict_batch), static_cast<size_t>(physUsedCount.load()), sweep_cb);
    // if (toWrite.size() != cfg.evict_batch) {
    //     Logger::info("write=", toWrite.size());
    // }


    const u64 selected_count = static_cast<u64>(toEvict.size() + toWrite.size());
    if (selected_count < reserved_budget) {
        evict_pages_inflight.fetch_sub(reserved_budget - selected_count, std::memory_order_relaxed);
        reserved_budget = selected_count;
    }

    if (toWrite.size() > 0) {
        if (do_log)
        Logger::info("fiber=", current_fiber_id(), " evicting: ", toWrite.size(), " pages");

        auto prep_sqe = [&](int b, struct io_uring_sqe* sqe) {
            BID bid = toWrite[b];
            PID pid = buffer_frames[bid].pid; // we can avoid this lookup by saving pid+bid in toWrite

            Page* page = pages + bid;
            const auto target = ssd_target(pid);

            if (cfg.nvme_cmds) {
                prep_nvme_write(sqe, target.fd, page, pageSize, target.offset, target.nsid, target.lba_shift);
                if (cfg.reg_bufs) {
                    int buf_idx = (bid * pageSize) / REG_BUF_SIZE;
                    sqe->uring_cmd_flags |= IORING_URING_CMD_FIXED;
                    sqe->buf_index = buf_idx;
                }
            } else if (!cfg.reg_bufs) {
                io_uring_prep_write(sqe, target.fd, page, pageSize, target.offset);
            } else {
                int buf_idx = (bid * pageSize) / REG_BUF_SIZE;
                io_uring_prep_write_fixed(sqe, target.fd, page, pageSize, target.offset, buf_idx);
            }

            if (cfg.reg_fds) {
                sqe->flags |= IOSQE_FIXED_FILE;
            }
        };

        auto prep_libaio = [&](int b, struct iocb* cb) {
            BID bid = toWrite[b];
            PID pid = buffer_frames[bid].pid; // we can avoid this lookup by saving pid+bid in toWrite

            Page* page = pages + bid;
            const auto target = ssd_target(pid);

            io_prep_pwrite(cb, target.fd, page, pageSize, target.offset);
        };


        if (sync_variant) {
            RDTSCClock clock(2.4_GHz);
            clock.start();

            if (posix_variant) {
                for (size_t i = 0; i < toWrite.size(); ++i) {
                    BID bid = toWrite[i];
                    PID pid = buffer_frames[bid].pid; // we can avoid this lookup by saving pid+bid in toWrite

                    Page* page = pages + bid;
                    const auto target = ssd_target(pid);

                    ensure(pwrite(target.fd, page, pageSize, target.offset) == pageSize);
                }
            } else {
                for (size_t i = 0; i < toWrite.size(); ++i) {
                    struct io_uring_sqe* sqe = io_uring_get_sqe(&worker().ring);
                    check_ptr(sqe);
                    prep_sqe(i, sqe);
                }

                int left = toWrite.size();
                while (true) {
                    io_uring_submit_and_wait(&worker().ring, left);
                    int i = 0;
                    uint32_t head;
                    struct io_uring_cqe* cqe;
                    io_uring_for_each_cqe(&worker().ring, head, cqe) {
                        ++i;
                        check_iou(cqe->res);
                        if (!cfg.nvme_cmds) {
                            ensure(cqe->res == pageSize);
                        }
                    }
                    io_uring_cq_advance(&worker().ring, i);
                    left -= i;
                    if (left == 0) {
                        break;
                    }
                }
            }


            clock.stop();
            io_cycles += clock.cycles();


        } else {
            mini::Op op;
            int rc;
            if constexpr (mini::LIBAIO) {
                rc = mini::io_batch(toWrite.size(), op, prep_libaio);
            } else {
                rc = mini::io_batch(toWrite.size(), op, prep_sqe);
            }
            if (!cfg.nvme_cmds) {
                ensure(rc == pageSize);
            }
        }

        worker().stats.writes += toWrite.size();


        if (do_log)
            Logger::info("fiber=", current_fiber_id(), " eviction done: ", toWrite.size(), " pages");
    }


    // 2. Evict chosen pages
    u64 evicted_count = 0;
    auto evictNow = [&](BID bid) {
        PID pid = buffer_frames[bid].pid;
        bool erased = page_table->erase_if(
            pid,
            [&](BID slot_bid) -> bool {
                if (slot_bid != bid) {
                    return false;
                }
                auto& meta = resident_meta[bid];
                if (!meta.evicting()) {
                    return false;
                }
                const u64 state = resident_mode(meta);
                if (state != PageState::Unlocked && state != PageState::Marked) {
                    Logger::info("evict and in_use bid=", bid);
                    return false;
                }
                if (meta.dirty()) {
                    return false;
                }
                ensure(!meta.io_in_progress());
                return true;
            },
            [&](BID committed_bid) {
                auto& meta = resident_meta[committed_bid];
                meta.set_evicting(false);
                freed.push_back(committed_bid);
                evicted_count++;
            });
        if (!erased) {
            return;
        }
    };

    // Release freed frames to shared pools and wake waiting fibers.
    const size_t refill_target = local_free_refill_target(1);
    auto releaseFreed = [&]() {
        if (freed.empty()) {
            return;
        }
        auto& pooled_freed = worker().evict_pooled_freed;
        auto& to_wake = worker().evict_to_wake;
        for (auto& v : pooled_freed) v.clear();
        to_wake.clear();
        for (auto bid : freed) {
            pooled_freed[free_pool_index_for_bid(bid)].push_back(bid);
        }
        for (size_t pool_idx = 0; pool_idx < free_pool_count; ++pool_idx) {
            auto& batch = pooled_freed[pool_idx];
            if (batch.empty()) {
                continue;
            }
            auto& pool = free_pools[pool_idx];
            SpinGuard pool_guard(pool.lock);
            pool.bids.insert(pool.bids.end(), batch.begin(), batch.end());
            shared_free_count += batch.size();
        }
        const size_t wake_slots = std::max<size_t>(1, freed.size() / refill_target);
        const size_t own_worker = static_cast<size_t>(worker().worker_id);
        {
            // Fast path: wake fibers parked on this worker's own queue directly,
            // avoiding the shared free_bid_wake_budget atomic and the full N-queue scan.
            SpinGuard guard(free_waiters_locks[own_worker].lock);
            auto& queue = free_frame_waiters_by_worker[own_worker];
            const size_t n = std::min(wake_slots, queue.size());
            for (size_t i = 0; i < n; ++i) {
                to_wake.push_back(queue.back());
                queue.pop_back();
            }
        }
        if (to_wake.empty()) {
            // Slow path: own queue empty, fall back to cross-worker global scan.
            free_bid_wake_budget.fetch_add(wake_slots, std::memory_order_relaxed);
            wake_free_bid_waiters_locked(static_cast<uint32_t>(own_worker), to_wake);
        }
        for (auto* fiber : to_wake) {
            wake_fiber(fiber);
        }
        // Combine the freed-page decrement with any buffered local delta into
        // a single global atomic op (avoids a separate fetch_add for the delta).
        auto& phys_delta = worker().phys_used_local_delta;
        const int64_t net = phys_delta - static_cast<int64_t>(freed.size());
        phys_delta = 0;
        physUsedCount.fetch_add(static_cast<u64>(net));
        freed.clear();
    };

    // Phase 1: evict clean pages immediately — no I/O needed.
    for (auto bid : toEvict)
        evictNow(bid);
    releaseFreed();

    // Phase 2: dirty pages — I/O already completed above; now evict and release.
    for (auto bid : toWrite)
        evictNow(bid);
    releaseFreed();

    if (reserved_budget > 0) {
        evict_pages_inflight.fetch_sub(reserved_budget, std::memory_order_relaxed);
    }
    if (!toWrite.empty() || evicted_count > 0) {
        return EvictResult::Progress;
    }
    return EvictResult::Idle;
}
