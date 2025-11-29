#include "bm.hpp"
#include "btree.hpp"
#include "buffer_mgr/tpcc/types.hpp"
// #include "furing.hpp"
#include "kuring.hpp"
#include "tpcc/tpcc_workload.hpp"
#include "utils.hpp"
#include "utils/cpu_map.hpp"
#include "utils/my_logger.hpp"
#include "utils/stats_printer.hpp"
#include "utils/stopper.hpp"
#include "utils/utils.hpp"
#include "ycsb_workload.hpp"


template <class Record>
struct Adapter {
    BTree tree;

public:
    void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb) {
        if (bm.do_log)
            Logger::info("fiber=", *bm.my_id, " scan");

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
            Logger::info("fiber=", *bm.my_id, " scan done");
    }

    void scanDesc(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb) {
        if (bm.do_log)
            Logger::info("fiber=", *bm.my_id, " scanDesc");

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
            Logger::info("fiber=", *bm.my_id, " insert");

        // bm.ensureFreePages(); // previously in bm.alloc function

        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        tree.insert({k, l}, {(u8*)(&record), sizeof(Record)});
    }

    template <class Fn>
    void lookup1(const typename Record::Key& key, Fn fn) {
        if (bm.do_log)
            Logger::info("fiber=", *bm.my_id, " lookup1");

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
            Logger::info("fiber=", *bm.my_id, " update1");

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
            Logger::info("fiber=", *bm.my_id, " erase");

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


int tpcc() {
    auto& cfg = Config::get();

    // TPC-C

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


    TPCCWorkload<Adapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock, true, cfg.tpcc_warehouses, true);
    // TPCCWorkload<Adapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock, false, cfg.tpcc_warehouses, true);


    // bm.do_log = true;
    bool loaded = false;
    mini::Fiber loader([&] {
        bm.my_id.reset(new uint64_t{0xff}); // special id for loader

        tpcc.loadItem();
        tpcc.loadWarehouse();

        for (Integer w_id = 1; w_id < cfg.tpcc_warehouses + 1; w_id++) {
            tpcc.loadStock(w_id);
            tpcc.loadDistrinct(w_id);
            for (Integer d_id = 1; d_id <= 10; d_id++) {
                tpcc.loadCustomer(w_id, d_id);
                tpcc.loadOrders(w_id, d_id);
            }
        }
        loaded = true;
    });
    bm.r->run(loaded);
    // loader.join();
    Logger::info("space: ", (bm.allocCount * pageSize) / (float)1_GiB, " GB");
    Logger::info("buffer_load=", bm.page_table->size() / static_cast<double>(bm.page_count));
    // std::cin.get();


    TimedStopper stopper;
    stopper.after(std::chrono::milliseconds(cfg.duration));

    uint64_t tps = 0;


    auto& stats = StatsPrinter::get();

    StatsPrinter::Scope stats_scope;
    stats.register_var(stats_scope, tps, "tps");
    stats.register_var(stats_scope, bm.readCount, "reads");
    stats.register_var(stats_scope, bm.writeCount, "writes");
    stats.register_var(stats_scope, io_cycles, "io_cycles");
    stats.register_var(stats_scope, bm.allocCount, "allocs");
    stats.register_var(stats_scope, bm.r->get_events, "get_events");
    stats.register_func(stats_scope, [&](auto& ss) {
        ss << " pt_%=" << bm.page_table->load_factor();
        ss << " bm_%=" << bm.page_table->size() / static_cast<double>(bm.page_count);
        // ss << " io_out=" << furing_ptr->outstanding_io;
        ss << " io_out=" << bm.r->outstanding_io;

        static Diff<uint64_t> reads_diff;
        static Diff<uint64_t> submit_diff;
        ss << " reads/submit=" << reads_diff(bm.readCount) / static_cast<double>(submit_diff(bm.r->num_submits));


        static Diff<uint64_t> writes_cycles;
        static Diff<uint64_t> writes_diff;
        ss << " cycles/write=" << writes_cycles(write_cycles) / static_cast<double>(writes_diff(bm.writeCount));
    });
    // stats.register_var(stats_scope, bm.fixes, "fixes");
    stats.register_func(stats_scope, [&](auto& ss) {
        static Diff<uint64_t> tps_diff;
        static Diff<uint64_t> fixes_diff;
        static Diff<uint64_t> restarts_diff;
        auto _tps = tps_diff(tps);
        auto fixes_ps = _tps > 0 ? (fixes_diff(bm.fixes) / static_cast<double>(_tps)) : 0;
        auto restarts_ps = _tps > 0 ? (restarts_diff(bm.restarts) / static_cast<double>(_tps)) : 0;
        ss << " fixes/txn=" << fixes_ps;
        ss << " restarts/txn=" << restarts_ps;

        static Diff<uint64_t> io_diff;
        static Diff<uint64_t> get_diff;
        auto gets = get_diff(bm.r->get_events);
        ss << " gets=" << gets;
        ss << " cq/get=" << io_diff(bm.readCount + bm.writeCount) / static_cast<double>(gets);


        static RDTSCClock clock(2.4_GHz);
        clock.stop();
        ss << " total_cycles=" << clock.cycles();
        clock.start();
    });
    stats.register_var(stats_scope, bm.r->fiber_run, "fiber_run");

    // bm.do_log = true;

    std::vector<mini::Fiber> fibers;


    // bm.do_log = true;

    bm.readCount = 0;
    bm.writeCount = 0;

    auto fn = [&](int id) {
        Logger::info("Fiber: ", id, " starting...");
        bm.my_id.reset(new uint64_t{static_cast<uint64_t>(id)});

        // while (stopper.can_run()) {
        while (true) {
            int w_id = tpcc.urand(1, cfg.tpcc_warehouses); // wh crossing
            int tx_type = tpcc.tx(w_id);
            if (bm.do_log)
                Logger::info("fiber=", id, " ran tx_type=", tx_type);
            ++tps;

            mini::R->check_submit();
            mini::yield();
        }
    };


    for (int i = 0; i < cfg.concurrency; ++i) {
        fibers.emplace_back(fn, i);
    }

    bm.r->run(stopper.triggered);
    fibers.clear();

    // for (auto& f : fibers) {
    //     f.join();
    // }

    return 0;
}


u64 write_cycles = 0;
RDTSCClock write_clock(2.4_GHz);
u64 io_cycles = 0;

int ycsb() {
    auto& cfg = Config::get();

    Adapter<ycsb_t> table;

    YCSBWorkload<Adapter> ycsb(table, cfg.ycsb_tuple_count, cfg.ycsb_read_ratio);


    uint64_t tps = 0;


    auto& stats = StatsPrinter::get();

    StatsPrinter::Scope stats_scope;
    stats.register_var(stats_scope, tps, "tps");
    stats.register_var(stats_scope, bm.readCount, "reads");
    stats.register_var(stats_scope, bm.writeCount, "writes");
    stats.register_var(stats_scope, io_cycles, "io_cycles");
    stats.register_var(stats_scope, bm.allocCount, "allocs");
    stats.register_var(stats_scope, bm.r->get_events, "get_events");
    stats.register_func(stats_scope, [&](auto& ss) {
        ss << " pt_%=" << bm.page_table->load_factor();
        ss << " bm_%=" << bm.page_table->size() / static_cast<double>(bm.page_count);
        // ss << " io_out=" << furing_ptr->outstanding_io;
        ss << " io_out=" << bm.r->outstanding_io;

        static Diff<uint64_t> reads_diff;
        static Diff<uint64_t> submit_diff;
        ss << " reads/submit=" << reads_diff(bm.readCount) / static_cast<double>(submit_diff(bm.r->num_submits));


        static Diff<uint64_t> writes_cycles;
        static Diff<uint64_t> writes_diff;
        ss << " cycles/write=" << writes_cycles(write_cycles) / static_cast<double>(writes_diff(bm.writeCount));
    });
    // stats.register_var(stats_scope, bm.fixes, "fixes");
    stats.register_func(stats_scope, [&](auto& ss) {
        static Diff<uint64_t> tps_diff;
        static Diff<uint64_t> fixes_diff;
        static Diff<uint64_t> restarts_diff;
        auto _tps = tps_diff(tps);
        auto fixes_ps = _tps > 0 ? (fixes_diff(bm.fixes) / static_cast<double>(_tps)) : 0;
        auto restarts_ps = _tps > 0 ? (restarts_diff(bm.restarts) / static_cast<double>(_tps)) : 0;
        ss << " fixes/txn=" << fixes_ps;
        ss << " restarts/txn=" << restarts_ps;

        static Diff<uint64_t> io_diff;
        static Diff<uint64_t> get_diff;
        auto gets = get_diff(bm.r->get_events);
        ss << " gets=" << gets;
        ss << " cq/get=" << io_diff(bm.readCount + bm.writeCount) / static_cast<double>(gets);

        static RDTSCClock clock(2.4_GHz);
        clock.stop();
        ss << " total_cycles=" << clock.cycles();
        clock.start();
    });
    stats.register_var(stats_scope, bm.r->fiber_run, "fiber_run");


    bool loaded = false;
    mini::Fiber loader([&] {
        bm.my_id.reset(new uint64_t{0xff}); // special id for loader

        ycsb.loadTable();
        loaded = true;
    });
    bm.r->run(loaded);
    Logger::info("space: ", (bm.allocCount * pageSize) / (float)1_GiB, " GB");
    Logger::info("buffer_load=", bm.page_table->size() / static_cast<double>(bm.page_count));


    TimedStopper stopper;
    stopper.after(std::chrono::milliseconds(cfg.duration));

    std::vector<mini::Fiber> fibers;

    // bm.do_log = true;

    bm.readCount = 0;
    bm.writeCount = 0;

    auto fn = [&](int id) {
        Logger::info("Fiber: ", id, " starting...");
        bm.my_id.reset(new uint64_t{static_cast<uint64_t>(id)});

        // while (stopper.can_run()) {
        while (true) {
            int tx_type = ycsb.tx();
            if (bm.do_log)
                Logger::info("fiber=", id, " ran tx_type=", tx_type);
            ++tps;

            mini::R->check_submit();
            mini::yield();
        }
    };


    for (int i = 0; i < cfg.concurrency; ++i) {
        fibers.emplace_back(fn, i);
    }

    bm.r->run(stopper.triggered);
    fibers.clear();

    // for (auto& f : fibers) {
    //     f.join();
    // }

    return 0;
}


BufferManager bm;


int main(int argc, char** argv) {
    if (!jmp::init()) { // enables run-time code patching
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
        CPUMap::get().pin(cfg.core_id);
    }

    bm.init();


    if (cfg.workload == "tpcc") {
        return tpcc();
    } else if (cfg.workload == "ycsb") {
        return ycsb();
    }

    ensure(false, "unknown workload");
}
