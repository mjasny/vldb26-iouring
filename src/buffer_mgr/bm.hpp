#pragma once

#include "boost/fiber/fss.hpp"
#include "config.hpp"
#include "kuring.hpp"
#include "rh_backshift_u64_map.hpp"
#include "types.hpp"
#include "utils/jmp.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/utils.hpp"

#include <cassert>
#include <fstream>
#include <liburing.h>
#include <list>
#include <unordered_map>
#include <variant>
#include <vector>


struct SleepingFiber {
    bool stop = false;
    bool running = false;
    std::unique_ptr<mini::Fiber> fiber;
    Reactor::Fiber* ctx;

    template <typename SetupFn, typename LoopFn>
    void spawn(SetupFn&& setup_fn, LoopFn&& loop_fn) {
        fiber = std::make_unique<mini::Fiber>([&] {
            register_self();
            setup_fn();

            while (!stop) {
                if (loop_fn()) {
                    park();
                }
            }
        });
    }

    ~SleepingFiber() {
        stop = true;
        wakeup();
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
        mini::park();
    }

    void wakeup() {
        if (running) {
            return;
        }
        running = true;
        // FUring::g_algo->awakened(ctx);
        mini::wake(ctx);
    }
};

struct BufferFrame {
    PID pid; // page-id
    // FUring::Op* waiting = nullptr;
    mini::Op* waiting = nullptr;

    BufferFrame(PID pid) : pid(pid) {
    }
};

struct PageFaultException {
    PID pid;
};

struct RestartException {
    BID bid;
};

struct AllocException {};


struct BufTagged {
    static constexpr uint64_t kInUse = 1ull << 0;
    static constexpr uint64_t kDirty = 1ull << 1;
    static constexpr uint64_t kEvict = 1ull << 2;
    static constexpr uint64_t kIOLock = 1ull << 3;
    static constexpr uint64_t kMark = 1ull << 4;

    static constexpr uint64_t kFlagsMask = kInUse | kDirty | kEvict | kIOLock | kMark;
    static constexpr unsigned kShift = 5;
    static constexpr uint64_t kIdMask = ~kFlagsMask;

    uint64_t v = 0;

    BufTagged(BID bid) : v(bid << kShift) {}

    BID id() const { return (v & kIdMask) >> kShift; }
    bool in_use() const { return v & kInUse; }
    bool dirty() const { return v & kDirty; }
    bool evicting() const { return v & kEvict; }
    bool io_lock() const { return v & kIOLock; }
    bool marked() const { return v & kMark; }

    void set_in_use(bool b) { v = b ? (v | kInUse) : (v & ~kInUse); }
    void set_dirty(bool b) { v = b ? (v | kDirty) : (v & ~kDirty); }
    void set_evicting(bool b) { v = b ? (v | kEvict) : (v & ~kEvict); }
    void set_io_lock(bool b) { v = b ? (v | kIOLock) : (v & ~kIOLock); }
    void set_marked(bool b) { v = b ? (v | kMark) : (v & ~kMark); }
};


using PageTable = RHBSU64Map<BufTagged>;


struct BufferManager {
    static constexpr auto REG_BUF_SIZE = 1_GiB;
    struct io_uring ring;


    std::unique_ptr<Reactor> r;

    bool do_log = false;
    Config cfg;

    u64 page_count;

    int blockfd; // TODO replace with SSD Raid
    int ssd_fd;

    u64 allocCount = 1;    // pid 0 reserved for meta data
    u64 physUsedCount = 1; // metadata loaded

    // maps page_id (pid) to buffer_id (bid)
    std::unique_ptr<PageTable> page_table;

    BufferFrame* buffer_frames;
    Page* pages;

    // free physical slots (stack), buffer-ids
    std::vector<BID> freeList;

    u64 readCount = 0;
    u64 writeCount = 0;
    u64 fixes = 0;
    u64 restarts = 0;

    using Exception = std::variant<PageFaultException, RestartException, AllocException>;
    Exception restart_ctx;

    void handleRestart();

    static constexpr jmp::static_branch<bool> sync_variant = false;
    static constexpr jmp::static_branch<bool> posix_variant = false;


    std::vector<BID> toEvict; // physical slots
    std::vector<BID> toWrite; // physical slots

    SleepingFiber eviction_fiber;
    boost::fibers::fiber_specific_ptr<uint64_t> my_id;

    BufferManager();
    ~BufferManager() {}

    void init();

    Page* fixX(PID pid);
    void unfixX(PID pid);
    Page* fixS(PID pid);
    void unfixS(PID pid);


    bool isValidPtr(void* page) {
        return (page >= pages) && (page < (pages + page_count));
    }

    void ensureFreePages();
    Page* allocPage();

    void handleFault(PID pid);
    void handleWait(BID bid);

    void evict();


    // debug


    void dump_pt() {
        Logger::info("Dumping pt");
        std::ofstream f("page_table.csv");
        f << "key,bid,ideal_index,index\n";
        page_table->dump([&](PID pid, BufTagged& buf, uint64_t ideal_index, uint64_t index) {
            BID bid = buf.id();
            // Logger::info("pid=", pid, " bid=", bid, " slot=", slot);
            f << pid << "," << bid << "," << ideal_index << "," << index << "\n";
        });
        Logger::info("Done");
    }
};


extern BufferManager bm;
