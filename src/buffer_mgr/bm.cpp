#include "bm.hpp"

#include "btree_node.hpp"
#include "buffer_mgr/kuring.hpp"
#include "config.hpp"
#include "utils.hpp"
#include "utils/hugepages.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/nvme.hpp"
#include "utils/rdtsc_clock.hpp"

#include <boost/fiber/operations.hpp>
#include <chrono>
#include <fcntl.h>
#include <libaio.h>
#include <liburing.h>
#include <sys/mman.h>
#include <unistd.h>

// for utils/nvme.hpp
uint32_t nsid;
uint32_t lba_shift;

BufferManager::BufferManager() {}

void BufferManager::init() {
    cfg = Config::get();

    page_count = cfg.virt_size / pageSize; // physical slots we can keep in memory

    toEvict.reserve(cfg.evict_batch);
    toWrite.reserve(cfg.evict_batch);

    auto next_pow2 = [](u64 x) -> u64 {
        return 1 << (64 - __builtin_clzl(x - 1));
    };

    uint64_t page_table_sz = next_pow2(page_count * cfg.page_table_factor);
    Logger::info("page_count=", page_count, " page_table_sz=", page_table_sz,
                 " ratio=", page_table_sz / static_cast<double>(page_count));
    page_table = std::make_unique<PageTable>(page_table_sz);

    buffer_frames = HugePages::malloc_array<BufferFrame>(page_count);
    pages = HugePages::malloc_array<Page>(page_count);

    int open_flags = O_DIRECT | O_RDWR;
    if (cfg.nvme_cmds) {
        open_flags &= ~O_DIRECT;

        int fd = open(cfg.ssd.c_str(), open_flags);
        check_ret(fd);
        nvme_get_info(fd);
        close(fd);
    }

    const char* path = cfg.ssd.c_str();
    blockfd = open(path, open_flags, 0);
    check_ret(blockfd);

    freeList.reserve(page_count);
    // push free physical slots in descending order so pop_back gives 1,2,...
    for (u64 i = 0; i < page_count; ++i) {
        BID bid = page_count - i - 1;
        if (bid == 0) {
            break; // metadata page
        }
        ensure(isValidPtr(pages + bid));
        freeList.push_back(bid);
    }

    // allocate and map metadata page 0 to physical 0
    BufTagged buf(0);
    buf.set_dirty(true);
    buf.set_marked(true);
    bool inserted = page_table->insert(0, buf);
    ensure(inserted);

    auto& frame = buffer_frames[buf.id()];
    new (&frame) BufferFrame(0);

    // setup uring
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CLAMP;
    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = 131072;
    if (cfg.setup_mode == SetupMode::DEFER_TASKRUN) {
        params.flags |= IORING_SETUP_DEFER_TASKRUN;
    }
    if (cfg.setup_mode == SetupMode::SQPOLL) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 1000;
        if (cfg.core_id != -1) {
            params.sq_thread_cpu = cfg.core_id + 1;
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

    auto res = io_uring_queue_init_params(4096, &ring, &params);
    if (res < 0) {
        throw std::system_error(-res, std::system_category());
    }

    if (cfg.reg_ring) {
        ensure(io_uring_register_ring_fd(&ring) == 1);
    }

    ssd_fd = blockfd;
    if (cfg.reg_fds) {
        check_iou(io_uring_register_files_sparse(&ring, 1024));
        check_iou(io_uring_register_files_update(&ring, /*off*/ 0, &ssd_fd, 1));
        ssd_fd = 0;
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
        check_iou(io_uring_register_buffers(&ring, iov.data(), iov.size()));
    }


    r = std::make_unique<Reactor>(ring);
    mini::set_reactor(*r);
    r->total_io_fibers = cfg.concurrency;

    // non-main function
    eviction_fiber.spawn(
        [&] {
            bm.my_id.reset(new uint64_t{0xfe}); // special id for evictor
        },
        [&] {
            if (bm.freeList.size() <= bm.page_count * bm.cfg.free_target) {
                bm.evict();
                return false; // no park
            }
            return true; // park
        });
}

void BufferManager::ensureFreePages() {
    if (freeList.size() <= page_count * cfg.free_target) {
        eviction_fiber.wakeup();
    }
}

// allocated new page and fix it
Page* BufferManager::allocPage() {
    ensureFreePages(); // we cannot do this to avoid rescheduling/locks

    if (freeList.empty()) {
        // if (freeList.size() <= page_count * 0.01) {
        ++restarts;
        restart_ctx = AllocException{};
        return nullptr;
    }

    // acquire a free physical slot
    ensure(!freeList.empty());
    BID bid = freeList.back();
    freeList.pop_back();
    physUsedCount++;

    // assign a new logical PID
    PID pid = allocCount++;
    if (do_log)
        Logger::info("fiber=", *my_id, " alloc pid=", pid, " bid=", bid);

    BufTagged buf(bid);
    buf.set_dirty(true);
    buf.set_in_use(true);
    buf.set_marked(true);

    try {
        bool inserted = page_table->insert(pid, buf);
        ensure(inserted);
    } catch (...) {
        dump_pt();
        throw;
    }

    auto* page = pages + bid;
    std::memset(page, 0, pageSize);

    auto& frame = buffer_frames[bid];
    new (&frame) BufferFrame(pid);

    ensure(isValidPtr(page));
    return page;
}

void BufferManager::handleFault(PID pid) {
    ensureFreePages();


    if (freeList.empty()) {
        static int to_print = 10;
        if (to_print != 0) {
            Logger::info("evictor too slow");
            --to_print;
        }
        mini::yield();
        return;
    }

    ensure(!freeList.empty());
    BID bid = freeList.back();
    freeList.pop_back();
    physUsedCount++;

    if (do_log)
        Logger::info("fiber=", *my_id, " read pid=", pid, " bid=", bid);

    BufTagged buf(bid);
    buf.set_io_lock(true);
    buf.set_marked(true);
    bool inserted = page_table->insert(pid, buf);
    ensure(inserted);
    // Logger::info("fiber=", *my_id, " inserted pid=", pid);

    auto& frame = buffer_frames[bid];
    new (&frame) BufferFrame(pid);

    auto* page = pages + bid;

    auto prep_sqe = [&](struct io_uring_sqe* sqe) {
        auto offset = pid * pageSize;

        // Logger::info("read pid=", pid, " offset=", offset);
        if (cfg.nvme_cmds) {
            prep_nvme_read(sqe, ssd_fd, page, pageSize, offset);
            if (cfg.reg_bufs) {
                int buf_idx = (bid * pageSize) / REG_BUF_SIZE;
                sqe->uring_cmd_flags |= IORING_URING_CMD_FIXED;
                sqe->buf_index = buf_idx;
            }
        } else if (!cfg.reg_bufs) {
            io_uring_prep_read(sqe, ssd_fd, page, pageSize, offset);
        } else {
            int buf_idx = (bid * pageSize) / REG_BUF_SIZE;
            io_uring_prep_read_fixed(sqe, ssd_fd, page, pageSize, offset, buf_idx);
        }

        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
    };

    auto prep_libaio = [&](struct iocb* cb) {
        auto offset = pid * pageSize;
        io_prep_pread(cb, ssd_fd, page, pageSize, offset);
    };

    if (sync_variant) {
        RDTSCClock clock(2.4_GHz);
        clock.start();

        if (posix_variant) {
            auto offset = pid * pageSize;
            ensure(pread(ssd_fd, page, pageSize, offset) == pageSize);
        } else {
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            check_ptr(sqe);
            prep_sqe(sqe);

            int left = 1;
            while (true) {
                io_uring_submit_and_wait(&ring, left);

                int i = 0;
                uint32_t head;
                struct io_uring_cqe* cqe;
                io_uring_for_each_cqe(&ring, head, cqe) {
                    ++i;
                    check_iou(cqe->res);
                    if (!cfg.nvme_cmds) {
                        ensure(cqe->res == pageSize);
                    }
                }
                io_uring_cq_advance(&ring, i);
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
        frame.waiting = &op;
        int rc;
        if constexpr (mini::LIBAIO) {
            rc = mini::io(op, prep_libaio);
        } else {
            rc = mini::io(op, prep_sqe);
        }
        if (!cfg.nvme_cmds) {
            ensure(rc == pageSize);
        }

        ensure(frame.waiting == &op);
        frame.waiting = frame.waiting->next; // skip self
        while (frame.waiting) {
            mini::wake(frame.waiting->ctx);
            frame.waiting = frame.waiting->next;
        }
    }

    if (do_log)
        Logger::info("fiber=", *my_id, " read done pid=", pid, " bid=", bid);

    auto* buf_ptr = page_table->find(pid);
    assert(buf_ptr && "logical PID not resident");
    buf_ptr->set_io_lock(false);

    readCount++;
}

// helper type for the visitor
template <class... Ts>
struct overloads : Ts... {
    using Ts::operator()...;
};

void BufferManager::handleRestart() {
    std::visit(overloads{
                   [&](PageFaultException& e) { bm.handleFault(e.pid); },
                   [&](RestartException& e) { bm.handleWait(e.bid); },
                   [&](AllocException&) { mini::yield(); },
               },
               restart_ctx);
}

void BufferManager::handleWait(BID bid) {
    auto& frame = buffer_frames[bid];

    mini::Op op;
    ensure(frame.waiting);
    if (frame.waiting) {
        // keep the io-initiated fiber front
        op.next = frame.waiting->next;
        frame.waiting->next = &op;
    }

    // TODO remove this
    auto* buf_ptr = page_table->find(frame.pid);
    assert(buf_ptr && "logical PID not resident");
    ensure(buf_ptr->io_lock());

    op.ctx = mini::current();
    mini::park();
}

Page* BufferManager::fixS(PID pid) {
    if (do_log)
        Logger::info("fiber=", *my_id, " fixS pid=", pid);

    ++fixes;

    auto* buf_ptr = page_table->find(pid);
    if (!buf_ptr) {
        ++restarts;
        restart_ctx = PageFaultException(pid);
        return nullptr;
    }

    BID bid = buf_ptr->id();
    if (buf_ptr->io_lock()) {
        ++restarts;
        restart_ctx = RestartException(bid);
        return nullptr;
    }

    ensure(!buf_ptr->in_use());
    buf_ptr->set_in_use(true);
    buf_ptr->set_marked(true);

    auto* page = pages + bid;
    ensure(isValidPtr(pages));
    return page;
}

Page* BufferManager::fixX(PID pid) {
    if (do_log)
        Logger::info("fiber=", *my_id, " fixX pid=", pid);

    ++fixes;

    auto* buf_ptr = page_table->find(pid);
    if (!buf_ptr) {
        ++restarts;
        restart_ctx = PageFaultException(pid);
        return nullptr;
    }

    BID bid = buf_ptr->id();
    if (buf_ptr->io_lock()) {
        ++restarts;
        restart_ctx = RestartException(bid);
        return nullptr;
    }

    ensure(!buf_ptr->in_use());
    buf_ptr->set_in_use(true);
    buf_ptr->set_marked(true);
    buf_ptr->set_dirty(true);

    auto* page = pages + bid;
    ensure(isValidPtr(pages));
    return page;
}

void BufferManager::unfixS(PID pid) {
    if (do_log)
        Logger::info("fiber=", *my_id, " unfixS pid=", pid);

    auto* buf_ptr = page_table->find(pid);
    assert(buf_ptr && "logical PID not resident");

    ensure(buf_ptr->in_use());
    buf_ptr->set_in_use(false);
    buf_ptr->set_marked(true);
}

void BufferManager::unfixX(PID pid) {
    if (do_log)
        Logger::info("fiber=", *my_id, " unfixX pid=", pid);

    auto* buf_ptr = page_table->find(pid);
    assert(buf_ptr && "logical PID not resident");

    ensure(buf_ptr->in_use());
    buf_ptr->set_in_use(false);
    buf_ptr->set_dirty(true);
    buf_ptr->set_marked(true);

    ensure(!buf_ptr->io_lock());
}

void BufferManager::evict() {
    toEvict.clear();
    toWrite.clear();

    uint64_t visited = 0;

    write_clock.start();

    bool exhausted = page_table->clock_sweep_next([&](PID pid, BufTagged& buf) {
        BID bid = buf.id();
        auto& frame = buffer_frames[bid];
        ensure(frame.pid == pid); // invariant

        visited++;
        if (buf.in_use()) {
            return false;
        }
        if (buf.io_lock()) {
            // read into this frame in progress
            return false;
        }
        if (buf.evicting()) {
            return false;
        }

        // second chance eviction
        if (buf.marked()) {
            buf.set_marked(false);
            return false;
        }
        ensure(pid != 0);

        buf.set_evicting(true);

        if (buf.dirty()) {
            buf.set_dirty(false);
            toWrite.push_back(bid);
        } else {
            toEvict.push_back(bid);
        }
        bool done = (toEvict.size() + toWrite.size()) == cfg.evict_batch;
        return done;
    });
    if (do_log) {
        static u64 old_sweep = 0;
        if (page_table->sweep_ < old_sweep) {
            Logger::info("evict made round");
        }
        old_sweep = page_table->sweep_;
    }

    // if (toWrite.size() != cfg.evict_batch) {
    //     Logger::info("write=", toWrite.size());
    // }

    if (toWrite.size() > 0) {
        if (do_log)
            Logger::info("fiber=", *my_id, " evicting: ", toWrite.size(), " pages");

        auto prep_sqe = [&](int b, struct io_uring_sqe* sqe) {
            BID bid = toWrite[b];
            PID pid = buffer_frames[bid].pid;

            Page* page = pages + bid;
            u64 offset = pageSize * pid;

            // Logger::info("write pid=", pid);

            if (cfg.nvme_cmds) {
                prep_nvme_write(sqe, ssd_fd, page, pageSize, offset);
                if (cfg.reg_bufs) {
                    int buf_idx = (bid * pageSize) / REG_BUF_SIZE;
                    sqe->uring_cmd_flags |= IORING_URING_CMD_FIXED;
                    sqe->buf_index = buf_idx;
                }
            } else if (!cfg.reg_bufs) {
                io_uring_prep_write(sqe, ssd_fd, page, pageSize, offset);
            } else {
                int buf_idx = (bid * pageSize) / REG_BUF_SIZE;
                io_uring_prep_write_fixed(sqe, ssd_fd, page, pageSize, offset, buf_idx);
            }

            if (cfg.reg_fds) {
                sqe->flags |= IOSQE_FIXED_FILE;
            }
        };

        auto prep_libaio = [&](int b, struct iocb* cb) {
            BID bid = toWrite[b];
            PID pid =
                buffer_frames[bid]
                    .pid; // we can avoid this lookup by saving pid+bid in toWrite

            Page* page = pages + bid;
            u64 offset = pageSize * pid;

            io_prep_pwrite(cb, ssd_fd, page, pageSize, offset);
        };

        if (sync_variant) {
            RDTSCClock clock(2.4_GHz);
            clock.start();

            if (posix_variant) {
                for (size_t i = 0; i < toWrite.size(); ++i) {
                    BID bid = toWrite[i];
                    PID pid =
                        buffer_frames[bid]
                            .pid; // we can avoid this lookup by saving pid+bid in toWrite

                    Page* page = pages + bid;
                    u64 offset = pageSize * pid;

                    ensure(pwrite(ssd_fd, page, pageSize, offset) == pageSize);
                }
            } else {
                for (size_t i = 0; i < toWrite.size(); ++i) {
                    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
                    check_ptr(sqe);
                    prep_sqe(i, sqe);
                }

                int left = toWrite.size();
                while (true) {
                    io_uring_submit_and_wait(&ring, left);
                    int i = 0;
                    uint32_t head;
                    struct io_uring_cqe* cqe;
                    io_uring_for_each_cqe(&ring, head, cqe) {
                        ++i;
                        check_iou(cqe->res);
                        if (!cfg.nvme_cmds) {
                            ensure(cqe->res == pageSize);
                        }
                    }
                    io_uring_cq_advance(&ring, i);
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

        writeCount += toWrite.size();

        if (do_log)
            Logger::info("fiber=", *my_id, " eviction done: ", toWrite.size(),
                         " pages");
    }

    // 2. Evict chosen pages (based on timestamps)
    u64 evicted_count = 0;
    auto evictNow = [&](BID bid) {
        auto& frame = buffer_frames[bid];

        PID pid = buffer_frames[bid].pid;
        auto* buf_ptr = page_table->find(pid);
        assert(buf_ptr && "logical PID not resident");

        ensure(buf_ptr->evicting());
        buf_ptr->set_evicting(false);

        if (buf_ptr->in_use()) {
            Logger::info("evict and in_use bid=", bid);
            // referenced while evicting
            return;
        }
        if (buf_ptr->dirty()) {
            return;
        }

        ensure(!buf_ptr->io_lock());

        bool deleted = page_table->erase(pid);
        ensure(deleted);
        freeList.push_back(bid);

        evicted_count++;
    };

    for (auto bid : toEvict)
        evictNow(bid);
    for (auto bid : toWrite)
        evictNow(bid);

    physUsedCount -= evicted_count;
}
