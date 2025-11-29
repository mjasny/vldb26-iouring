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
#include <linux/fs.h>
#include <stdio.h>
#include <sys/uio.h>
#include <unistd.h>

// echo 1 > /sys/kernel/debug/tracing/events/nvme/enable
// echo "disk==nvme8n1" > /sys/kernel/debug/tracing/events/nvme/filter
// cat /sys/kernel/debug/tracing/trace

enum class Method {
    NONE,
    FSYNC,
    FSYNC_LINK,
    FSYNC_LINK2,
    OPEN_SYNC,
    OPEN_DSYNC,
    WRITE_SYNC,
    WRITE_DSYNC, // RWF_ATOMIC => Torn writes
    NVME_PASSTHRU,
    NVME_PASSTHRU_FLUSH,
};

std::ostream& operator<<(std::ostream& os, const Method& arg) {
    switch (arg) {
        case Method::NONE:
            os << "none";
            break;
        case Method::FSYNC:
            os << "fsync";
            break;
        case Method::FSYNC_LINK:
            os << "fsync_link";
            break;
        case Method::FSYNC_LINK2:
            os << "fsync_link2";
            break;
        case Method::OPEN_SYNC:
            os << "open_sync";
            break;
        case Method::OPEN_DSYNC:
            os << "open_dsync";
            break;
        case Method::WRITE_SYNC:
            os << "write_sync";
            break;
        case Method::WRITE_DSYNC:
            os << "write_dsync";
            break;
        case Method::NVME_PASSTHRU:
            os << "nvme_passthru";
            break;
        case Method::NVME_PASSTHRU_FLUSH:
            os << "nvme_passthru_flush";
            break;
    }
    return os;
}


std::istream& operator>>(std::istream& is, Method& method) {
    std::string token;
    is >> token;

    if (token == "none") {
        method = Method::NONE;
    } else if (token == "fsync") {
        method = Method::FSYNC;
    } else if (token == "fsync_link") {
        method = Method::FSYNC_LINK;
    } else if (token == "fsync_link2") {
        method = Method::FSYNC_LINK2;
    } else if (token == "open_sync") {
        method = Method::OPEN_SYNC;
    } else if (token == "open_dsync") {
        method = Method::OPEN_DSYNC;
    } else if (token == "write_sync") {
        method = Method::WRITE_SYNC;
    } else if (token == "write_dsync") {
        method = Method::WRITE_DSYNC;
    } else if (token == "nvme_passthru") {
        method = Method::NVME_PASSTHRU;
    } else if (token == "nvme_passthru_flush") {
        method = Method::NVME_PASSTHRU_FLUSH;
    } else {
        throw std::invalid_argument("Invalid input for Method: " + token);
    }
    return is;
}


struct Config {
    std::string ssd;
    SetupMode setup_mode = SetupMode::DEFER_TASKRUN;
    bool reg_ring = false;
    bool reg_bufs = false;
    bool reg_fds = false;
    uint32_t core_id = 3; // use 64 to avoid double IRQ mapping
    uint32_t duration = 10'000;
    uint32_t cq_entries = 0;
    uint32_t max_workers = 0;
    bool measure_lat = false;
    bool perfevent = false;
    bool pin_iowq = false;
    uint32_t write_size = 4096;
    Method method = Method::NONE;
    bool fsync_thread = false;
    bool iopoll = false;
    bool nvme_passthru = false;
    uint32_t stats_interval = 100'000;

    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--ssd", ssd);
        parser.parse("--setup_mode", setup_mode, cli::Parser::optional);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--reg_ring", reg_ring, cli::Parser::optional);
        parser.parse("--reg_bufs", reg_bufs, cli::Parser::optional);
        parser.parse("--reg_fds", reg_fds, cli::Parser::optional);
        parser.parse("--duration", duration, cli::Parser::optional);
        parser.parse("--cq_entries", cq_entries, cli::Parser::optional);
        parser.parse("--max_workers", max_workers, cli::Parser::optional);
        parser.parse("--measure_lat", measure_lat, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--pin_iowq", pin_iowq, cli::Parser::optional);
        parser.parse("--write_size", write_size, cli::Parser::optional);
        parser.parse("--method", method, cli::Parser::optional);
        parser.parse("--iopoll", iopoll, cli::Parser::optional);
        parser.parse("--fsync_thread", fsync_thread, cli::Parser::optional);
        parser.parse("--stats_interval", stats_interval, cli::Parser::optional);
        parser.check_unparsed();
        parser.print();

        if (method == Method::NVME_PASSTHRU || method == Method::NVME_PASSTHRU_FLUSH) {
            nvme_passthru = true;
            ensure(ssd.starts_with("/dev/ng"));
        }

        ensure(ssd.size() > 0);
    }
};

#include <linux/nvme_ioctl.h>
#include <nvme/types.h>
#include <sys/ioctl.h>

uint32_t nsid;
uint32_t lba_shift;

int nvme_get_info(int fd) {
    nsid = ioctl(fd, NVME_IOCTL_ID);
    check_ret(nsid);

    constexpr uint32_t NVME_DEFAULT_IOCTL_TIMEOUT = 0;
    constexpr uint32_t NVME_IDENTIFY_CSI_SHIFT = 24;

    struct nvme_id_ns ns;
    struct nvme_passthru_cmd cmd = {
        .opcode = nvme_admin_identify,
        .nsid = nsid,
        .addr = reinterpret_cast<uintptr_t>(&ns),
        .data_len = NVME_IDENTIFY_DATA_SIZE,
        .cdw10 = NVME_IDENTIFY_CNS_NS,
        .cdw11 = NVME_CSI_NVM << NVME_IDENTIFY_CSI_SHIFT,
        .timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT,
    };

    check_ret(ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd));

    uint32_t lba_size = 1 << ns.lbaf[(ns.flbas & 0x0f)].ds;
    lba_shift = ilog2(lba_size);

    return 0;
}

void prep_nvme_write(struct io_uring_sqe* sqe, int fd, void* buf, uint32_t len, uint64_t offset) {
    sqe->fd = fd;
    sqe->flags = 0;

    sqe->opcode = IORING_OP_URING_CMD;
    sqe->cmd_op = NVME_URING_CMD_IO;
    struct nvme_uring_cmd* cmd = reinterpret_cast<struct nvme_uring_cmd*>(sqe->cmd);
    memset(cmd, 0, sizeof(struct nvme_uring_cmd));

    uint64_t slba = offset >> lba_shift;
    uint32_t nlb = (len >> lba_shift) - 1;

    cmd->opcode = nvme_cmd_write;
    cmd->cdw10 = slba & 0xffffffff;
    cmd->cdw11 = slba >> 32;
    cmd->cdw12 = nlb;
    cmd->addr = reinterpret_cast<uintptr_t>(buf);
    cmd->data_len = len;
    cmd->nsid = nsid;
    // cmd->cdw13 = 1 << 6; // DSM Sequential Request
}

void prep_nvme_flush(struct io_uring_sqe* sqe, int fd) {
    sqe->fd = fd;
    sqe->flags = 0;
    sqe->opcode = IORING_OP_URING_CMD;
    sqe->cmd_op = NVME_URING_CMD_IO;
    struct nvme_uring_cmd* cmd = reinterpret_cast<struct nvme_uring_cmd*>(sqe->cmd);
    memset(cmd, 0, sizeof(struct nvme_uring_cmd));

    cmd->opcode = nvme_cmd_flush;
    cmd->nsid = nsid;
}


int main(int argc, char** argv) {
    Config cfg;
    cfg.parse(argc, argv);


    auto& stats = StatsPrinter::get();
    stats.interval = cfg.stats_interval;
    stats.start();

    if (cfg.core_id != -1) {
        CPUMap::get().pin(cfg.core_id);
    }

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
    if (cfg.iopoll) {
        params.flags |= IORING_SETUP_IOPOLL;
    }

    if (cfg.nvme_passthru) {
        params.flags |= IORING_SETUP_CQE32 | IORING_SETUP_SQE128;
    }

    uint32_t entries = 8; // std::max(8, cfg.nr_fsyncs);
    if (cfg.cq_entries > 0) {
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = cfg.cq_entries;
        ensure(entries <= cfg.cq_entries);
    }

    if (io_uring_queue_init_params(entries, &ring, &params) < 0) {
        fprintf(stderr, "%s\n", strerror(errno));
        exit(1);
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
        cpu_set_t set;
        CPU_ZERO(&set);
        // CPU_SET(cfg.core_id + 2, &set);
        if (cfg.max_workers == 0) {
            CPU_SET(cfg.core_id + 2, &set);
        } else {
            for (int i = 0; i < cfg.max_workers; ++i) {
                CPU_SET(cfg.core_id + 2 + i, &set);
            }
        }
        check_ret(io_uring_register_iowq_aff(&ring, sizeof(set), &set));
        Logger::info("registered iowq affinity");
    }


    int open_flags = O_DIRECT | O_RDWR; // | O_DSYNC;
    if (cfg.method == Method::OPEN_SYNC) {
        open_flags |= O_SYNC;
    } else if (cfg.method == Method::OPEN_DSYNC) {
        open_flags |= O_DSYNC;
    }
    if (cfg.nvme_passthru) {
        open_flags &= ~O_DIRECT;
    }

    int fd = open(cfg.ssd.c_str(), open_flags);
    check_ret(fd);
    nvme_get_info(fd);

    int fd_old = fd; // for fsync


    if (cfg.reg_fds) {
        check_iou(io_uring_register_files_sparse(&ring, 1));
        check_iou(io_uring_register_files_update(&ring, /*off*/ 0, &fd, 1));
        fd = 0;
    }


    uint64_t ops = 0;
    uint64_t latency = 0;

    StatsPrinter::Scope stats_scope;
    stats.register_var(stats_scope, ops, "ops");
    if (cfg.measure_lat) {
        stats.register_var(stats_scope, latency, "latency", false);
    }


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

    // alignas(64) uint8_t buffer[4096];

    uint8_t* buffer = reinterpret_cast<uint8_t*>(std::aligned_alloc(4096, cfg.write_size));
    ensure(!cfg.reg_bufs);


    struct iovec iov{.iov_base = buffer, .iov_len = cfg.write_size};
    uint64_t write_offset = 0;


    uint64_t fsync2_cntr = 0;

    uint32_t outstanding = 0;
    while (stopper.can_run()) {
        struct io_uring_cqe* cqe;
        unsigned int head;


        if (outstanding == 0) {

            if ((cfg.method == Method::FSYNC_LINK2 && !cfg.iopoll) || cfg.method == Method::NVME_PASSTHRU_FLUSH) {
                fsync2_cntr++;
                if (fsync2_cntr & 0x01) {
                    if (cfg.measure_lat) {
                        lat_clock.start();
                    }
                    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
                    if (cfg.nvme_passthru) {
                        prep_nvme_write(sqe, fd, buffer, cfg.write_size, write_offset);
                    } else if (cfg.method == Method::WRITE_SYNC || cfg.method == Method::WRITE_DSYNC) {
                        int flags = (cfg.method == Method::WRITE_SYNC) ? RWF_SYNC : RWF_DSYNC;
                        io_uring_prep_writev2(sqe, fd, &iov, 1, write_offset, flags);
                    } else {
                        io_uring_prep_write(sqe, fd, buffer, cfg.write_size, write_offset);
                    }
                    io_uring_sqe_set_data64(sqe, 1);
                    if (cfg.reg_fds) {
                        sqe->flags |= IOSQE_FIXED_FILE;
                    }
                    ++ops;
                    ++outstanding;
                    write_offset += cfg.write_size;
                } else {
                    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
                    if (cfg.nvme_passthru) {
                        prep_nvme_flush(sqe, fd);
                    } else {
                        io_uring_prep_fsync(sqe, fd, 0);
                    }
                    io_uring_sqe_set_data64(sqe, 2);
                    if (cfg.reg_fds) {
                        sqe->flags |= IOSQE_FIXED_FILE;
                    }
                    ++outstanding;
                }
            } else {
                if (cfg.measure_lat) {
                    lat_clock.start();
                }


                struct io_uring_sqe* write_sqe = nullptr;
                if (cfg.write_size > 0) {
                    write_sqe = io_uring_get_sqe(&ring);

                    if (cfg.nvme_passthru) {
                        prep_nvme_write(write_sqe, fd, buffer, cfg.write_size, write_offset);
                    } else if (cfg.method == Method::WRITE_SYNC || cfg.method == Method::WRITE_DSYNC) {
                        int flags = (cfg.method == Method::WRITE_SYNC) ? RWF_SYNC : RWF_DSYNC;
                        io_uring_prep_writev2(write_sqe, fd, &iov, 1, write_offset, flags);
                    } else {
                        io_uring_prep_write(write_sqe, fd, buffer, cfg.write_size, write_offset);
                    }

                    io_uring_sqe_set_data64(write_sqe, 1);
                    if (cfg.reg_fds) {
                        write_sqe->flags |= IOSQE_FIXED_FILE;
                    }
                    // write_sqe->flags |= IOSQE_ASYNC;
                    ++ops;
                    ++outstanding;
                    write_offset += cfg.write_size;
                }

                if (cfg.method == Method::FSYNC || cfg.method == Method::FSYNC_LINK) {
                    if (cfg.method == Method::FSYNC_LINK) {
                        write_sqe->flags |= IOSQE_IO_LINK;
                    }

                    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_fsync(sqe, fd, 0);
                    // io_uring_prep_nop(sqe);
                    io_uring_sqe_set_data64(sqe, 2);
                    if (cfg.reg_fds) {
                        sqe->flags |= IOSQE_FIXED_FILE;
                    }
                    ++outstanding;
                }
            }
        }

        // io_uring_wait_cqes(&ring, &cqe, to_wait, NULL, NULL);

        if (cfg.setup_mode == SetupMode::SQPOLL) {
            io_uring_submit(&ring);
        } else {
            io_uring_submit_and_wait(&ring, outstanding);
        }


        uint64_t user_data = 0;
        uint32_t i = 0;
        io_uring_for_each_cqe(&ring, head, cqe) {
            if (cqe->res < 0) {
                fprintf(stderr, "cqe ud=%llu res %d %s\n", cqe->user_data, cqe->res, strerror(-cqe->res));
                exit(1);
            }
            user_data = cqe->user_data;

            ++i;
        }
        io_uring_cq_advance(&ring, i);
        outstanding -= i;

        if (i > 0 && outstanding == 0) {
            if (cfg.iopoll && cfg.method == Method::FSYNC_LINK2) {
                check_ret(fsync(fd_old)); // out of ring fsync
            }
            if ((cfg.method == Method::FSYNC_LINK2 && !cfg.iopoll) || cfg.method == Method::NVME_PASSTHRU_FLUSH) {
                if ((fsync2_cntr & 0x01) == 1) {
                    goto skip_latency; // avoid latency measure
                }
            }
            if (cfg.measure_lat) {
                lat_clock.stop();
                latency = lat_clock.as<std::chrono::nanoseconds, uint64_t>();
            }
        skip_latency:
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
