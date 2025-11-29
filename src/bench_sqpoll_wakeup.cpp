#include "utils/cli_parser.hpp"
#include "utils/cpu_map.hpp"
#include "utils/literals.hpp"
#include "utils/my_asserts.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/utils.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <errno.h>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <liburing.h>
#include <linux/fs.h>
#include <random>
#include <string.h>
#include <string>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

using namespace std::chrono;

static constexpr size_t PAGE_SIZE = 4096;

struct Config {
    std::string file;
    size_t ops = 1000;
    bool reg_fds = true;
    int core_id = 3;
    uint32_t idle_ms = 20;
    uint32_t interval_ms = 5;
    // size_t max_offset = 10_GiB;
    size_t max_offset = 100_GiB;
    bool do_nops = false;

    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--file", file, cli::Parser::optional);
        parser.parse("--ops", ops, cli::Parser::optional);
        parser.parse("--reg_fds", reg_fds, cli::Parser::optional);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--idle_ms", idle_ms, cli::Parser::optional);
        parser.parse("--interval_ms", interval_ms, cli::Parser::optional);
        parser.parse("--max_offset", max_offset, cli::Parser::optional);
        parser.parse("--do_nops", do_nops, cli::Parser::optional);
        parser.check_unparsed();
        parser.print();

        if (!do_nops) {
            ensure(file.size() > 0);
        }
    }
};

static size_t round_down(size_t x, size_t align) { return x & ~(align - 1); }


static void print_stats(std::vector<uint64_t>& vals) {
    if (vals.empty())
        return;
    std::sort(vals.begin(), vals.end());
    auto pct = [&](double p) { size_t idx = (size_t)std::clamp((double)vals.size()*p/100.0, 0.0, (double)vals.size()-1); return vals[idx]; };
    uint64_t minv = vals.front();
    uint64_t maxv = vals.back();
    uint64_t p50 = pct(50.0);
    uint64_t p90 = pct(90.0);
    uint64_t p95 = pct(95.0);
    uint64_t p99 = pct(99.0);
    double avg = 0.0;
    for (auto v : vals)
        avg += v;
    avg /= (double)vals.size();

    std::printf("\nlatency (nsec) over %zu ops\n", vals.size());
    std::printf("  avg=%.2f  min=%" PRIu64 "  p50=%" PRIu64 "  p90=%" PRIu64 "  p95=%" PRIu64 "  p99=%" PRIu64 "  max=%" PRIu64 "\n",
                avg, minv, p50, p90, p95, p99, maxv);
}

enum class OpType : uint8_t { READ = 1,
                              NOP = 2 };
static inline uint64_t pack_udata(OpType t, uint64_t id) { return (uint64_t(t) << 56) | (id & 0x00FFFFFFFFFFFFFFull); }
static inline OpType udata_type(uint64_t u) { return OpType((u >> 56) & 0xFF); }

int main(int argc, char** argv) {
    Config cfg;
    cfg.parse(argc, argv);


    if (cfg.core_id != -1) {
        CPUMap::get().pin(cfg.core_id);
    }


    void* buf = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
    check_ptr(buf);
    memset(buf, 0, PAGE_SIZE);

    // Prepare ring with SQPOLL (and optional SQ_AFF)
    io_uring ring{};
    io_uring_params p{};
    p.flags = IORING_SETUP_SQPOLL;
    if (cfg.core_id > 0) {
        p.flags |= IORING_SETUP_SQ_AFF;
        p.sq_thread_cpu = cfg.core_id + 1;
    }
    p.sq_thread_idle = cfg.idle_ms; // in ms

    int ret = io_uring_queue_init_params(256 /*queue depth*/, &ring, &p);
    if (ret < 0) {
        throw std::system_error(-ret, std::system_category());
    }


    int fd = -1;
    int file_index = -1;
    if (!cfg.do_nops) {
        int open_flags = O_RDONLY | O_DIRECT;
        fd = open(cfg.file.c_str(), open_flags);
        check_ret(fd);

        // Optionally register the file for IOSQE_FIXED_FILE
        if (cfg.reg_fds) {
            int fds[1] = {fd};
            ret = io_uring_register_files(&ring, fds, 1);
            if (ret == 0)
                file_index = 0;
            else
                std::cerr << "[warn] register_files failed: " << strerror(-ret) << "\n";
        }
    }

    // Precompute random, aligned offsets
    std::mt19937_64 rng(0);
    size_t max_off = cfg.max_offset - PAGE_SIZE;
    std::uniform_int_distribution<size_t> dist(0, max_off);

    auto next_offset = [&]() {
        size_t off = round_down(dist(rng), PAGE_SIZE);
        return (off_t)off;
    };

    auto do_one_io = [&](uint64_t id, off_t off) -> int {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        if (!sqe)
            return -EAGAIN;

        if (cfg.do_nops) {
            io_uring_prep_nop(sqe);
        } else {
            if (file_index >= 0) {
                io_uring_prep_read(sqe, file_index, buf, PAGE_SIZE, off);
                sqe->flags |= IOSQE_FIXED_FILE;
            } else {
                io_uring_prep_read(sqe, fd, buf, PAGE_SIZE, off);
            }
        }

        // io_uring_prep_nop(sqe);
        sqe->user_data = pack_udata(OpType::READ, id);
        int sret = io_uring_submit(&ring);
        if (sret < 0)
            return sret;
        return 0;
    };


    std::vector<uint64_t> latency;
    latency.clear();
    latency.reserve(cfg.ops);


    RDTSCClock clock(2.4_GHz);

    for (size_t i = 0; i < cfg.ops; ++i) {
        busy_sleep(std::chrono::milliseconds(cfg.interval_ms));

        off_t off = next_offset();
        clock.start();
        int r = do_one_io(i, off);
        check_iou(r);

        while (true) {
            io_uring_cqe* cqe = nullptr;
            int w = io_uring_wait_cqe(&ring, &cqe);
            if (w < 0) {
                if (w == -EINTR)
                    continue;
                check_iou(-w);
                throw std::runtime_error("never reach");
            }
            OpType t = udata_type(cqe->user_data);
            int res = cqe->res;
            io_uring_cqe_seen(&ring, cqe);
            if (t == OpType::NOP) {
                // drain keepalive completions
                continue;
            }
            check_iou(res);
            clock.stop();
            uint64_t lat = clock.as<std::chrono::nanoseconds, uint64_t>();
            latency.push_back(lat);
            break;
        }
    }


    for (size_t i = 0; i < latency.size(); ++i) {
        std::cout << "latency=" << latency[i] << "\n";
    }

    print_stats(latency);

    if (!cfg.do_nops) {
        if (file_index >= 0) {
            (void)io_uring_unregister_files(&ring);
        }
        close(fd);
    }
    io_uring_queue_exit(&ring);
    free(buf);
    return 0;
}
