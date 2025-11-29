#include "utils/cli_parser.hpp"
#include "utils/cpu_map.hpp"
#include "utils/literals.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/perfevent.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/stats_printer.hpp"
#include "utils/stopper.hpp"
#include "utils/types.hpp"
#include "utils/utils.hpp"

#include <chrono>
#include <cstring>
#include <liburing.h>
#include <stdio.h>


struct Config {
    SetupMode setup_mode = SetupMode::DEFAULT;
    bool reg_ring = false;
    bool reg_bufs = false;
    bool reg_fds = false;
    int nr_nops = 1;
    uint32_t core_id = 3;
    uint32_t duration = 10'000;
    uint32_t cq_entries = 0;
    bool inject_error = false;
    bool test_file = false;
    bool test_buf = false;
    bool test_tw = false;
    bool test_async = false;
    uint32_t max_workers = 0;
    bool measure_lat = false;
    bool perfevent = false;
    uint64_t stats_interval = 1'000'000; // microseconds
    bool pin_iowq = false;
    uint32_t sleep_us = 0;

    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--setup_mode", setup_mode, cli::Parser::optional);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--reg_ring", reg_ring, cli::Parser::optional);
        parser.parse("--reg_bufs", reg_bufs, cli::Parser::optional);
        parser.parse("--reg_fds", reg_fds, cli::Parser::optional);
        parser.parse("--nr_nops", nr_nops, cli::Parser::optional);
        parser.parse("--duration", duration, cli::Parser::optional);
        parser.parse("--cq_entries", cq_entries, cli::Parser::optional);
        parser.parse("--inject_error", inject_error, cli::Parser::optional);
        parser.parse("--test_file", test_file, cli::Parser::optional);
        parser.parse("--test_buf", test_buf, cli::Parser::optional);
        parser.parse("--test_tw", test_tw, cli::Parser::optional);
        parser.parse("--test_async", test_async, cli::Parser::optional);
        parser.parse("--max_workers", max_workers, cli::Parser::optional);
        parser.parse("--measure_lat", measure_lat, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--stats_interval", stats_interval, cli::Parser::optional);
        parser.parse("--pin_iowq", pin_iowq, cli::Parser::optional);
        parser.parse("--sleep_us", sleep_us, cli::Parser::optional);
        parser.check_unparsed();
        parser.print();
    }
};

struct alignas(4096) Page {
    uint8_t data[4096];
};


int main(int argc, char** argv) {
    Config cfg;
    cfg.parse(argc, argv);

    if (cfg.core_id != -1) {
        CPUMap::get().pin(cfg.core_id);
    }

    auto& stats = StatsPrinter::get();
    stats.interval = cfg.stats_interval;


    struct io_uring ring;
    struct io_uring_params params;


    memset(&params, 0, sizeof(params));
    params.flags |= IORING_SETUP_SINGLE_ISSUER;
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

    uint32_t entries = std::max(8, cfg.nr_nops);
    if (cfg.cq_entries > 0) {
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = cfg.cq_entries;
        ensure(entries <= cfg.cq_entries);
    }

    Logger::info("entries=", entries);
    int res = io_uring_queue_init_params(entries, &ring, &params);
    if (res < 0) {
        throw std::system_error(-res, std::system_category());
    }

    if (cfg.reg_ring) {
        if (!(ring.features & IORING_FEAT_REG_REG_RING)) {
            Logger::error("IORING_FEAT_REG_REG_RING not supported");
            return 1;
        }
        ensure(io_uring_register_ring_fd(&ring) == 1);
        Logger::info("registered ring fd");
    }

    if (cfg.max_workers > 0) {
        uint32_t old_values[2] = {0, 0};
        check_iou(io_uring_register_iowq_max_workers(&ring, old_values));
        Logger::info("bounded=", old_values[0], " unbounded=", old_values[1]);

        // nop is a bounded task
        uint32_t values[2] = {cfg.max_workers, 0};
        check_iou(io_uring_register_iowq_max_workers(&ring, values));
    }

    if (cfg.pin_iowq) {
        ensure(cfg.max_workers > 0);
        cpu_set_t set;
        CPU_ZERO(&set);
        for (int i = 0; i < cfg.max_workers; ++i) {
            CPU_SET(cfg.core_id + 2 + i, &set);
        }
        check_ret(io_uring_register_iowq_aff(&ring, sizeof(set), &set));
        Logger::info("registered iowq affinity");
    }


    auto buffers = std::make_unique<Page[]>(cfg.nr_nops);
    if (cfg.reg_bufs) {
        struct iovec iov[cfg.nr_nops];
        for (uint32_t i = 0; i < cfg.nr_nops; ++i) {
            iov[i].iov_base = &buffers[i];
            iov[i].iov_len = sizeof(Page);
        }
        check_iou(io_uring_register_buffers(&ring, iov, cfg.nr_nops));
    }

    int null_fd = -1;
    int fds[8];
    memset(fds, -1, sizeof(fds));
    if (cfg.reg_fds) {
        check_iou(io_uring_register_files(&ring, fds, 8)); // We can also use the sparse variant
        fds[0] = open("/dev/null", O_RDWR | O_EXCL);
        check_ret(fds[0]);
        check_iou(io_uring_register_files_update(&ring, /*off*/ 0, fds, 1));
        null_fd = 0;
    } else {
        null_fd = open("/dev/null", O_RDWR | O_EXCL);
        check_ret(null_fd);
    }

    uint64_t ops = 0;
    uint64_t latency = 0;

    StatsPrinter::Scope stats_scope;
    stats.register_var(stats_scope, ops, "ops");
    if (cfg.measure_lat) {
        stats.register_var(stats_scope, latency, "latency", false);
    }
    stats.start();


    std::unique_ptr<PerfEvent> e;
    if (cfg.perfevent) {
        e = std::make_unique<PerfEvent>();
    }


    TimedStopper stopper;
    stopper.after(std::chrono::milliseconds(cfg.duration));

    RDTSCClock clock(2.4_GHz);
    RDTSCClock lat_clock(2.4_GHz);

    clock.start();
    if (e) {
        e->startCounters();
    }


    // TODO make liburing patch for this
#define IORING_NOP_FILE (1U << 1)
#define IORING_NOP_FIXED_FILE (1U << 2)
#define IORING_NOP_FIXED_BUFFER (1U << 3)
#define IORING_NOP_TW (1U << 4)

    if (cfg.test_buf) {
        ensure(cfg.reg_bufs);
    }
    ensure(count_true(cfg.test_buf, cfg.test_file) <= 1);

    uint32_t outstanding = 0;
    while (stopper.can_run()) {
        struct io_uring_cqe* cqe;
        unsigned int head;


        if (outstanding == 0) {

            if (cfg.setup_mode == SetupMode::SQPOLL) {
                // wait for empty ring
                while (io_uring_sq_space_left(&ring) != ring.sq.ring_entries)
                    ;
            }
            if (cfg.sleep_us > 0) {
                busy_sleep(std::chrono::microseconds(cfg.sleep_us));
            }
            for (int i = 0; i < cfg.nr_nops; ++i) {
                struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
                // struct io_uring_sqe* sqe;
                // do {
                //    sqe = io_uring_get_sqe(&ring);
                //} while (!sqe);
                io_uring_prep_nop(sqe);
                io_uring_sqe_set_data64(sqe, i);

                // https://lore.gnuweeb.org/io-uring/20241028150437.387667-2-axboe@kernel.dk/
                if (cfg.inject_error) {
                    sqe->nop_flags = IORING_NOP_INJECT_RESULT;
                    sqe->len = -EFAULT;
                }

                if (cfg.test_buf && cfg.reg_bufs) {
                    sqe->nop_flags = IORING_NOP_FIXED_BUFFER;
                    sqe->buf_index = i;
                }

                if (cfg.test_file) {
                    sqe->nop_flags = IORING_NOP_FILE;
                    sqe->fd = null_fd;
                    if (cfg.reg_fds) {
                        sqe->nop_flags |= IORING_NOP_FIXED_FILE;
                    }
                }

                if (cfg.test_async) {
                    sqe->flags |= IOSQE_ASYNC;
                }

                if (cfg.test_tw) {
                    sqe->flags |= IORING_NOP_TW;
                }

                ++outstanding;
            }


            if (cfg.measure_lat) {
                lat_clock.start();
            }
        }


        // io_uring_wait_cqes(&ring, &cqe, to_wait, NULL, NULL);


        if (cfg.setup_mode == SetupMode::SQPOLL) {
            io_uring_submit(&ring);
        } else {
            io_uring_submit_and_wait(&ring, cfg.nr_nops);
        }


        uint32_t i = 0;
        io_uring_for_each_cqe(&ring, head, cqe) {
            if (!cfg.inject_error) {
                if (cqe->res < 0) {
                    fprintf(stderr, "cqe res %d %s\n", cqe->res, strerror(-cqe->res));
                    exit(1);
                }
            }

            ++i;
            ++ops;
        }
        io_uring_cq_advance(&ring, i);
        outstanding -= i;


        if (cfg.measure_lat && outstanding == 0) {
            lat_clock.stop();
            latency = lat_clock.as<std::chrono::nanoseconds, uint64_t>();
        }
    }


    clock.stop();
    stats.stop();

    if (e) {
        e->stopCounters();
        e->printReport(std::cout, ops);
    }


    Logger::info("cycles=", clock.cycles());
    double seconds = clock.as<std::chrono::microseconds, double>() / 1e6;
    Logger::info("secs=", seconds);
    Logger::info("ops=", ops);
    Logger::info("ops_per_sec=", static_cast<double>(ops) / seconds);
}
