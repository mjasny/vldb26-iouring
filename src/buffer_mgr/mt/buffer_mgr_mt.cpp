#include "bm.hpp"
#include "btree.hpp"
#include "tpcc/types.hpp"
#include "kuring.hpp"
#include "tpcc/tpcc_workload.hpp"
#include "utils.hpp"
#include "utils/cpu_map.hpp"
#include "utils/my_logger.hpp"
#include "utils/stats_printer.hpp"
#include "utils/stopper.hpp"
#include "utils/utils.hpp"
#include "ycsb_workload.hpp"

#include <atomic>
#include <mutex>
#include <thread>

namespace {
struct WorkloadWakeDrainCtx {
    BufferManager* bm = nullptr;
    uint32_t worker_id = 0;
};

struct LoadWorkerDrainCtx {
    WorkerLocal* local = nullptr;
    bool* load_complete = nullptr;
    bool* stop = nullptr;
};

void workload_wake_drain_hook(void* raw_ctx) {
    auto* ctx = static_cast<WorkloadWakeDrainCtx*>(raw_ctx);
    ctx->bm->drain_inbound_wakeups(ctx->worker_id);
}

void load_worker_idle_stop_hook(void* raw_ctx) {
    auto* ctx = static_cast<LoadWorkerDrainCtx*>(raw_ctx);
    if (!*ctx->load_complete) {
        return;
    }
    auto* reactor = ctx->local->reactor.get();
    if (reactor->outstanding_io != 0 || reactor->submitted_io != 0 || reactor->pending_io != 0) {
        return;
    }
    *ctx->stop = true;
}

std::mutex leaked_worker_shutdown_mutex;
std::vector<std::unique_ptr<std::vector<mini::Fiber>>> leaked_worker_fibers;
}

template <class Record>
struct Adapter {
    BTree tree;

public:
    void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb) {
        if (bm.do_log)
            Logger::info("fiber=", bm.current_fiber_id(), " scan");

        // bm.ensureFreePages(); // previously in bm.alloc function

        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        u8 kk[Record::maxFoldLength()];
        tree.scanAsc({k, l}, [&](BTreeNode& node, unsigned slot) {
            memcpy(kk, node.getPrefix(), node.prefixLen);
            memcpy(kk + node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
            typename Record::Key typedKey;
            Record::unfoldKey(kk, typedKey);
            return found_record_cb(typedKey, *reinterpret_cast<const Record*>(node.getPayload(slot).data()));
        });


        if (bm.do_log)
            Logger::info("fiber=", bm.current_fiber_id(), " scan done");
    }

    void scanDesc(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb) {
        if (bm.do_log)
            Logger::info("fiber=", bm.current_fiber_id(), " scanDesc");

        // bm.ensureFreePages(); // previously in bm.alloc function

        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        u8 kk[Record::maxFoldLength()];
        bool first = true;
        tree.scanDesc({k, l}, [&](BTreeNode& node, unsigned slot, bool exactMatch) {
            if (first) { // XXX: hack
                first = false;
                if (!exactMatch)
                    return true;
            }
            memcpy(kk, node.getPrefix(), node.prefixLen);
            memcpy(kk + node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
            typename Record::Key typedKey;
            Record::unfoldKey(kk, typedKey);
            return found_record_cb(typedKey, *reinterpret_cast<const Record*>(node.getPayload(slot).data()));
        });
    }

    void insert(const typename Record::Key& key, const Record& record) {
        if (bm.do_log)
            Logger::info("fiber=", bm.current_fiber_id(), " insert");

        // bm.ensureFreePages(); // previously in bm.alloc function

        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        tree.insert({k, l}, {(u8*)(&record), sizeof(Record)});
    }

    template <class Fn>
    void lookup1(const typename Record::Key& key, Fn fn) {
        if (bm.do_log)
            Logger::info("fiber=", bm.current_fiber_id(), " lookup1");

        // bm.ensureFreePages(); // previously in bm.alloc function

        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        bool succ = tree.lookup({k, l}, [&](std::span<u8> payload) {
            fn(*reinterpret_cast<const Record*>(payload.data()));
        });
        assert(succ);
    }

    template <class Fn>
    void update1(const typename Record::Key& key, Fn fn) {
        if (bm.do_log)
            Logger::info("fiber=", bm.current_fiber_id(), " update1");

        // bm.ensureFreePages(); // previously in bm.alloc function

        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        tree.updateInPlace({k, l}, [&](std::span<u8> payload) {
            fn(*reinterpret_cast<Record*>(payload.data()));
        });
    }


    // Returns false if the record was not found
    bool erase(const typename Record::Key& key) {
        if (bm.do_log)
            Logger::info("fiber=", bm.current_fiber_id(), " erase");

        // bm.ensureFreePages(); // previously in bm.alloc function

        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        return tree.remove({k, l});
    }

    template <class Field>
    Field lookupField(const typename Record::Key& key, Field Record::* f) {
        Field value;
        lookup1(key, [&](const Record& r) { value = r.*f; });
        return value;
    }

    u64 count() {
        u64 cnt = 0;
        tree.scanAsc({(u8*)nullptr, 0}, [&](BTreeNode& node, unsigned slot) { cnt++; return true; });
        return cnt;
    }

    u64 countw(Integer w_id) {
        u8 k[sizeof(Integer)];
        fold(k, w_id);
        u64 cnt = 0;
        u8 kk[Record::maxFoldLength()];
        tree.scanAsc({k, sizeof(Integer)}, [&](BTreeNode& node, unsigned slot) {
            memcpy(kk, node.getPrefix(), node.prefixLen);
            memcpy(kk + node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
            if (memcmp(k, kk, sizeof(Integer)) != 0)
                return false;
            cnt++;
            return true;
        });
        return cnt;
    }
};


template <class T>
std::span<u8> fold(T& v) {
    static_assert(std::is_trivially_copyable_v<T>,
                  "fold requires trivially copyable T");
    return {reinterpret_cast<u8*>(&v), sizeof(T)};
}

template <class T>
T unfold(std::span<u8> payload) {
    static_assert(std::is_trivially_copyable_v<T>,
                  "unfold requires trivially copyable T");
    assert(payload.size() == sizeof(T) && "size mismatch in unfold");
    T out;
    std::memcpy(&out, payload.data(), sizeof(T)); // safe for alignment/aliasing
    return out;
}


u64 write_cycles = 0;
RDTSCClock write_clock(2.4_GHz);
u64 io_cycles = 0;

template <class InitFn, class LoadFn, class TxFn>
int run_workload(InitFn&& init_fn, LoadFn&& load_fn, TxFn&& tx_fn) {
    auto& cfg = Config::get();
    std::atomic<uint64_t> txn_total {0};
    uint64_t prev_io_cycles = 0;
    uint64_t prev_write_cycles = 0;
    uint64_t prev_txn_total = 0;
    uint64_t prev_writes = 0;
    uint64_t prev_fixes = 0;
    uint64_t prev_restarts = 0;

    auto diff_counter = [](uint64_t& prev, uint64_t current) {
        const uint64_t delta = current - prev;
        prev = current;
        return delta;
    };

    auto& stats = StatsPrinter::get();
    StatsPrinter::Scope stats_scope;
    stats.register_func(stats_scope, [&](auto& ss) { ss << " allocs=" << bm.allocCount.load(); });
    stats.register_func(stats_scope, [&](auto& ss) {
        const auto totals = bm.snapshot_worker_stats();
        ss << " io_cycles=" << diff_counter(prev_io_cycles, io_cycles);
        const double phys_used = static_cast<double>(bm.physUsedCount.load());
        ss << " pt_%=" << phys_used / static_cast<double>(bm.page_table->capacity());
        ss << " bm_%=" << phys_used / static_cast<double>(bm.page_count);

        const auto writes = diff_counter(prev_writes, totals.writes);
        const auto write_cycle_delta = diff_counter(prev_write_cycles, write_cycles);
        ss << " cycles/write=" << (writes > 0 ? write_cycle_delta / static_cast<double>(writes) : 0.0);
    });
    stats.register_func(stats_scope, [&](auto& ss) {
        auto _tps = diff_counter(prev_txn_total, txn_total.load(std::memory_order_relaxed));
        const auto totals = bm.snapshot_worker_stats();
        auto fixes_ps = _tps > 0 ? (diff_counter(prev_fixes, totals.fixes) / static_cast<double>(_tps)) : 0;
        auto restarts_ps = _tps > 0 ? (diff_counter(prev_restarts, totals.restarts) / static_cast<double>(_tps)) : 0;
        ss << " fixes/txn=" << fixes_ps;
        ss << " restarts/txn=" << restarts_ps;

        static RDTSCClock clock(2.4_GHz);
        clock.stop();
        ss << " total_cycles=" << clock.cycles();
        clock.start();
    });
    bm.init_worker(0, cfg.concurrency + 1);
    auto state = std::forward<InitFn>(init_fn)();

    bool loaded = false;
    mini::Fiber loader([&] {
        bm.worker().my_id.reset(new uint64_t{0xff});
        load_fn(*state);
        loaded = true;
    });
    bm.worker().reactor->run(loaded);

    Logger::info("space: ", (bm.allocCount.load() * pageSize) / (float)1_GiB, " GB");
    Logger::info("buffer_load=", bm.buffer_load());

    bm.reset_active_runtime_counters();
    prev_io_cycles = 0;
    prev_write_cycles = 0;
    prev_txn_total = 0;
    prev_writes = 0;
    prev_fixes = 0;
    prev_restarts = 0;
    Logger::info("load complete");

    std::atomic<bool> stop {false};
    std::jthread stop_thread([&](std::stop_token) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.duration));
        stop.store(true, std::memory_order_release);
        bm.request_stop_active_workers();
    });

    auto worker_main = [&](int worker_id, bool already_initialized) {
        if (!already_initialized) {
            if (cfg.core_id != -1) {
                auto& cpu_map = CPUMap::get();
                const size_t core_index = static_cast<size_t>(cfg.core_id) + static_cast<size_t>(worker_id);
                const int core = static_cast<int>(core_index % cpu_map.total_cores);
                cpu_map.pin(core);
            }
            bm.init_worker(worker_id, cfg.concurrency + 1);
        }
        auto* local = &bm.worker();
        local->stop_requested.store(false, std::memory_order_relaxed);
        local->txn_count = 0;

        WorkloadWakeDrainCtx wake_ctx {&bm, static_cast<uint32_t>(worker_id)};
        local->reactor->set_wake_drain_hook(&wake_ctx, &workload_wake_drain_hook);

        std::vector<mini::Fiber> fibers;
        for (int i = 0; i < cfg.concurrency; ++i) {
            fibers.emplace_back([&, i] {
                local->my_id.reset(new uint64_t{(static_cast<uint64_t>(worker_id) << 32) | static_cast<uint64_t>(i)});
                while (true) {
                    if (stop.load(std::memory_order_acquire)) {
                        return;
                    }
                    int tx_type = tx_fn(*state);
                    if (bm.do_log) {
                        Logger::info("worker=", worker_id, " fiber=", i, " ran tx_type=", tx_type);
                    }
                    ++local->txn_count;
                    txn_total.fetch_add(1, std::memory_order_relaxed);
                    mini::R->check_submit();
                    mini::yield();
                }
            });
        }

        bm.worker().reactor->run(local->stop_requested);
        if (local->stop_requested.load(std::memory_order_relaxed)) {
            std::lock_guard<std::mutex> guard(leaked_worker_shutdown_mutex);
            leaked_worker_fibers.push_back(std::make_unique<std::vector<mini::Fiber>>(std::move(fibers)));
            bm.abandon_worker();
            return;
        }
        fibers.clear();
        bm.shutdown_worker();
    };

    std::vector<std::jthread> workers;
    workers.reserve(cfg.num_workers > 0 ? cfg.num_workers - 1 : 0);
    for (int worker_id = 1; worker_id < cfg.num_workers; ++worker_id) {
        workers.emplace_back([&, worker_id](std::stop_token) {
            worker_main(worker_id, false);
        });
    }
    worker_main(0, true);
    return 0;
}

int tpcc() {
    auto& cfg = Config::get();
    return run_workload(
        [&] {
            struct TpccState {
                Adapter<warehouse_t> warehouse;
                Adapter<district_t> district;
                Adapter<customer_t> customer;
                Adapter<customer_wdl_t> customerwdl;
                Adapter<history_t> history;
                Adapter<neworder_t> neworder;
                Adapter<order_t> order;
                Adapter<order_wdc_t> order_wdc;
                Adapter<orderline_t> orderline;
                Adapter<item_t> item;
                Adapter<stock_t> stock;
                TPCCWorkload<Adapter> tpcc;

                explicit TpccState(int warehouses)
                    : tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock, true, warehouses, true) {
                }
            };
            return std::make_shared<TpccState>(cfg.tpcc_warehouses);
        },
        [&](auto& state) {
            state.tpcc.loadItem();
            state.tpcc.loadWarehouse();
            for (Integer w_id = 1; w_id < cfg.tpcc_warehouses + 1; w_id++) {
                state.tpcc.loadStock(w_id);
                state.tpcc.loadDistrinct(w_id);
                for (Integer d_id = 1; d_id <= 10; d_id++) {
                    state.tpcc.loadCustomer(w_id, d_id);
                    state.tpcc.loadOrders(w_id, d_id);
                }
            }
        },
        [&](auto& state) {
            int w_id = state.tpcc.urand(1, cfg.tpcc_warehouses);
            return state.tpcc.tx(w_id);
        });
}

int ycsb() {
    auto& cfg = Config::get();
    struct YcsbState {
        Adapter<ycsb_t> table;
        YCSBWorkload<Adapter> ycsb;

        YcsbState(uint64_t tuple_count, int read_ratio, double zipf_theta)
            : ycsb(table, tuple_count, read_ratio, zipf_theta) {
        }
    };

    auto parallel_load = [&](auto& state) {
        const uint64_t total = cfg.ycsb_tuple_count;
        uint64_t load_workers = static_cast<uint64_t>(cfg.num_loaders);
        if (load_workers == 0) {
            load_workers = 1;
        }
        if (total < load_workers) {
            load_workers = total;
        }
        if (load_workers == 0) {
            Logger::info("loaded 0 tuples");
            return;
        }

        auto load_worker_range = [&](int worker_id, uint64_t begin, uint64_t end, bool already_initialized) {
            if (!already_initialized) {
                if (cfg.core_id != -1) {
                    auto& cpu_map = CPUMap::get();
                    const size_t core_index = static_cast<size_t>(cfg.core_id) + static_cast<size_t>(worker_id);
                    const int core = static_cast<int>(core_index % cpu_map.total_cores);
                    cpu_map.pin(core);
                }
                bm.init_worker(worker_id, 2);
            }
            bool local_done = false;
            bool load_complete = false;
            LoadWorkerDrainCtx drain_ctx {};
            if (!already_initialized) {
                auto* local = &bm.worker();
                drain_ctx = LoadWorkerDrainCtx {local, &load_complete, &local_done};
                local->reactor->set_idle_stop_hook(&drain_ctx, &load_worker_idle_stop_hook);
            }
            mini::Fiber loader([&] {
                bm.worker().my_id.reset(new uint64_t {(static_cast<uint64_t>(worker_id) << 32) | 0xff});
                state.ycsb.loadRange(begin, end);
                if (already_initialized) {
                    local_done = true;
                    return;
                }
                load_complete = true;
                bm.worker().eviction_fiber.stop = true;
                bm.worker().eviction_fiber.wakeup();
            });
            bm.worker().reactor->run(local_done);
            if (!already_initialized) {
                bm.shutdown_worker();
            }
        };

        const uint64_t per_worker = total / load_workers;
        std::vector<std::jthread> load_threads;
        load_threads.reserve(load_workers > 0 ? static_cast<size_t>(load_workers - 1) : 0);
        for (uint64_t worker = 1; worker < load_workers; ++worker) {
            const uint64_t begin = per_worker * worker;
            const uint64_t end = (worker == (load_workers - 1)) ? total : (begin + per_worker);
            load_threads.emplace_back([&, worker, begin, end](std::stop_token) {
                load_worker_range(static_cast<int>(worker), begin, end, false);
            });
        }
        const uint64_t end0 = (load_workers == 1) ? total : per_worker;
        load_worker_range(0, 0, end0, true);
        load_threads.clear();
        Logger::info("loaded ", total, " tuples");
    };

    auto load_fn = [&](auto& state) {
        if (cfg.ycsb_load_mt) {
            parallel_load(state);
        } else {
            state.ycsb.loadTable();
        }
    };

    if (cfg.ycsb_zipf_theta > 0.0) {
        Logger::info("ycsb variant=zipf theta=", cfg.ycsb_zipf_theta);
        return run_workload(
            [&] { return std::make_shared<YcsbState>(cfg.ycsb_tuple_count, cfg.ycsb_read_ratio, cfg.ycsb_zipf_theta); },
            load_fn,
            [&](auto& state) { return state.ycsb.tx_skew(); });
    }

    Logger::info("ycsb variant=uniform");
    return run_workload(
        [&] { return std::make_shared<YcsbState>(cfg.ycsb_tuple_count, cfg.ycsb_read_ratio, cfg.ycsb_zipf_theta); },
        load_fn,
        [&](auto& state) { return state.ycsb.tx(); });
}

int rndread() {
    auto& cfg = Config::get();
    return run_workload(
        [&] {
            struct RndReadState {
                BTree bt;

                RndReadState() {
                    bt.splitOrdered = true;
                }
            };
            return std::make_shared<RndReadState>();
        },
        [&](auto& state) {
            for (uint64_t i = 0; i < cfg.ycsb_tuple_count; ++i) {
                union {
                    uint64_t v;
                    uint8_t key[sizeof(uint64_t)];
                } k {};
                k.v = __builtin_bswap64(i);

                std::array<uint8_t, 120> payload {};
                std::memcpy(payload.data(), k.key, sizeof(uint64_t));
                state.bt.insert({k.key, sizeof(uint64_t)}, {payload.data(), payload.size()});
            }
            Logger::info("loaded ", cfg.ycsb_tuple_count, " rndread tuples");
        },
        [&](auto& state) {
            union {
                uint64_t v;
                uint8_t key[sizeof(uint64_t)];
            } k {};
            k.v = __builtin_bswap64(RandomGenerator::getRand<uint64_t>(0, cfg.ycsb_tuple_count));

            std::array<uint8_t, 120> payload {};
            bool succ = state.bt.lookup({k.key, sizeof(uint64_t)}, [&](std::span<uint8_t> p) {
                std::memcpy(payload.data(), p.data(), p.size());
            });
            ensure(succ);
            ensure(std::memcmp(k.key, payload.data(), sizeof(uint64_t)) == 0);
            return 0;
        });
}

BufferManager bm;

int main(int argc, char** argv) {
    if (!jmp::init()) {
        return errno;
    }
    auto& cfg = Config::get();
    cfg.parse(argc, argv);

    Reactor::submit_always = cfg.submit_always;
    BufferManager::sync_variant = cfg.sync_variant;
    BufferManager::posix_variant = cfg.posix_variant;

    ensure(cfg.libaio == mini::LIBAIO);

    auto& stats = StatsPrinter::get();
    stats.interval = cfg.stats_interval;
    stats.start();

    if (cfg.core_id != -1) {
        auto& cpu_map = CPUMap::get();
        const int core = static_cast<int>(static_cast<size_t>(cfg.core_id) % cpu_map.total_cores);
        cpu_map.pin(core);
    }

    bm.init();

    if (cfg.workload == "tpcc") {
        return tpcc();
    }
    if (cfg.workload == "ycsb") {
        return ycsb();
    }
    if (cfg.workload == "rndread") {
        return rndread();
    }

    ensure(false, "unknown workload");
}
