#include "utils/cli_parser.hpp"
#include "utils/cpu_map.hpp"
#include "utils/literals.hpp"
#include "utils/mem_dump.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/random.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/ring.hpp"
#include "utils/stats_printer.hpp"
#include "utils/stopper.hpp"
#include "utils/types.hpp"
#include "utils/utils.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <ratio>
#include <stdexcept>
#include <stdio.h>
#include <thread>


struct Config {
    std::vector<std::string> ssds;
    int core_id = 3;
    bool reg_bufs = true;
    bool reg_ring = true;
    bool reg_fds = true;
    bool iopoll = false;
    uint32_t duration = 10'000;

    int num_threads = 1;
    uint64_t ssd_size = 10_GiB;
    // bool hybrid_iopoll = false;
    uint64_t stats_interval = 1'000'000; // microseconds
    uint32_t batch_size = 1;
    std::string label;
    uint64_t samples = 1e4;

    bool spiky = false;
    uint64_t target_rate = 0;


    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--reg_ring", reg_ring, cli::Parser::optional);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--ssds", ssds);
        parser.parse("--reg_bufs", reg_bufs, cli::Parser::optional);
        parser.parse("--reg_fds", reg_fds, cli::Parser::optional);
        parser.parse("--iopoll", iopoll, cli::Parser::optional);
        parser.parse("--duration", duration, cli::Parser::optional);
        parser.parse("--num_threads", num_threads, cli::Parser::optional);
        parser.parse("--stats_interval", stats_interval, cli::Parser::optional);
        parser.parse("--ssd_size", ssd_size, cli::Parser::optional);
        parser.parse("--batch_size", batch_size, cli::Parser::optional);
        parser.parse("--label", label);
        parser.parse("--samples", samples, cli::Parser::optional);
        parser.parse("--spiky", spiky, cli::Parser::optional);
        parser.parse("--target_rate", target_rate, cli::Parser::optional);

        parser.check_unparsed();
        parser.print();

        ensure(ssds.size() > 0);
    }
};


struct alignas(4096) Page {
    uint8_t data[4096];

    operator void*() {
        return data;
    }
};


class RateLimiter {
public:
    RateLimiter(double rate, uint64_t threads, uint64_t thread_id, bool spiky) {
        double rate_per_thread = rate / threads;
        inter_arrival_time = 1e6 / rate_per_thread; // microseconds
        double inter_arrival_time_offset = (inter_arrival_time / threads) * thread_id;
        if (spiky) {
            inter_arrival_time_offset = 0.0;
        }
        Logger::info("offset=", inter_arrival_time_offset);
        next_time = std::chrono::steady_clock::now() + std::chrono::microseconds(static_cast<uint64_t>(inter_arrival_time_offset + inter_arrival_time));
    }

    uint64_t wait() {
        next_time += std::chrono::microseconds(static_cast<uint64_t>(inter_arrival_time));
        auto now = std::chrono::steady_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(now - next_time).count();
        wait_until(next_time);
        return diff;
    }


    void run(std::function<void()> action, std::function<void(uint64_t)> sampling) {
        next_time += std::chrono::microseconds(static_cast<uint64_t>(inter_arrival_time));
        auto now = std::chrono::steady_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(now - next_time).count();
        wait_until(next_time);
        auto begin = std::chrono::steady_clock::now();
        action();
        auto end = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count();
        if (diff > 0) {
            end += diff;
        }
        sampling(end);
    }

private:
    double inter_arrival_time;
    std::chrono::steady_clock::time_point next_time;

    static void wait_until(std::chrono::steady_clock::time_point next) {
        auto current = std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::duration<double>>(next - current).count() > 0.0) {
            current = std::chrono::steady_clock::now();
            _mm_pause(); // spin-wait
        }
    }
};


struct Worker {
    struct io_uring ring;
    StatsPrinter::Scope stats_scope;
    std::jthread thread;

    int id;
    Config cfg; // make local copy

    uint64_t ops = 0;
    int fd;
    std::atomic<bool> wait = true;
    bool record;
    std::unique_ptr<Page[]> buffers;
    uint64_t outstanding_ios = 0;

    std::vector<uint64_t> latencies;

    Worker(Config& cfg, int id) : id(id), cfg(cfg) {
        thread = std::jthread([&, id](std::stop_token token) {
            if (cfg.core_id != -1) {
                CPUMap::get().pin(cfg.core_id + id);
            }

            latencies.reserve(cfg.samples);

            init();
            run(token);
        });
    }

    ~Worker() {
        if (thread.joinable()) {
            thread.join();
        }
        io_uring_queue_exit(&ring);
    }


    void init() {
        struct io_uring_params params;
        memset(&params, 0, sizeof(params));
        params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CLAMP;
        params.flags |= IORING_SETUP_DEFER_TASKRUN;
        if (cfg.iopoll) {
            params.flags |= IORING_SETUP_IOPOLL;
        }


        uint32_t entries = std::max(uint32_t{8}, cfg.batch_size);
        if (io_uring_queue_init_params(entries, &ring, &params) < 0) {
            fprintf(stderr, "%s\n", strerror(errno));
            exit(1);
        }

        if (cfg.reg_ring) {
            if (!(ring.features & IORING_FEAT_REG_REG_RING)) {
                throw std::runtime_error("IORING_FEAT_REG_REG_RING not supported");
            }
            ensure(io_uring_register_ring_fd(&ring) == 1);
        }


        auto ssd_path = cfg.ssds.at(0);
        int open_flags = O_DIRECT | O_RDWR;
        fd = open(ssd_path.c_str(), open_flags);
        check_ret(fd);

        if (cfg.reg_fds) {
            check_iou(io_uring_register_files_sparse(&ring, 1024));
            check_iou(io_uring_register_files_update(&ring, /*off*/ 0, &fd, 1));
            fd = 0;
        }


        buffers = std::make_unique<Page[]>(cfg.batch_size);
        if (cfg.reg_bufs) {
            std::vector<struct iovec> iov(cfg.batch_size);
            for (uint32_t i = 0; i < cfg.batch_size; ++i) {
                iov[i].iov_base = &buffers[i];
                iov[i].iov_len = sizeof(Page);
            }
            check_iou(io_uring_register_buffers(&ring, iov.data(), iov.size()));
        }
    }


    void start() {
        wait = false;
    }

    void request_stop() {
        if (thread.joinable()) {
            thread.request_stop();
        }
    }

    void stop() {
        if (thread.joinable()) {
            thread.join();
        }
    }

    void run(std::stop_token token) {

        uint64_t submits = 0;
        uint64_t offset = 0;
        uint64_t max_offset = cfg.ssd_size;

        while (wait)
            ;

        RDTSCClock clock(2.4_GHz);
        clock.start();

        ensure(cfg.target_rate > 0);

        RateLimiter ratelimiter(cfg.target_rate / cfg.batch_size, cfg.num_threads, id, cfg.spiky);

        while (!token.stop_requested()) {
            auto time_offset = ratelimiter.wait();

            uint64_t start_ts = RDTSCClock::read();
            for (auto i = 0; i < cfg.batch_size; ++i) {
                struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
                auto& buffer = buffers[i];
                io_uring_sqe_set_data64(sqe, start_ts);

                if (!cfg.reg_bufs) {
                    io_uring_prep_write(sqe, fd, buffer, sizeof(Page), offset);
                } else {
                    io_uring_prep_write_fixed(sqe, fd, buffer, sizeof(Page), offset, i);
                }
                if (cfg.reg_fds) {
                    sqe->flags |= IOSQE_FIXED_FILE;
                }

                offset += sizeof(Page);
                if (offset >= max_offset) {
                    offset = 0;
                }
                ++ops;
                ++outstanding_ios;
            }
            ++submits;

            // io_uring_submit(&ring);

            uint32_t done = 0;
            while (done < cfg.batch_size) {
                io_uring_submit_and_wait(&ring, 1);
                // io_uring_wait_cqe

                struct io_uring_cqe* cqe;
                uint32_t head;
                uint32_t i = 0;
                uint64_t end_ts = RDTSCClock::read();
                io_uring_for_each_cqe(&ring, head, cqe) {
                    uint64_t data = io_uring_cqe_get_data64(cqe);
                    if (cqe->res < 0) {
                        Logger::info("ops=", ops);
                        Logger::info("data=", data);
                    }
                    check_iou(cqe->res);
                    --outstanding_ios;

                    uint64_t diff = end_ts - data;
                    if (record && latencies.size() < latencies.capacity()) {
                        auto ns = clock.convert<std::chrono::nanoseconds, uint64_t>(diff);
                        // latencies.push_back(ns + time_offset);
                        // Logger::info(time_offset);
                        latencies.push_back(ns);
                    }

                    ++i;
                }
                io_uring_cq_advance(&ring, i);
                done += i;
            }
        }

        clock.stop();

        // Logger::info("cycles=", clock.cycles());
        double seconds = clock.as<std::chrono::microseconds, double>() / 1e6;
        Logger::info("secs=", seconds);
        Logger::info("ops=", ops);
        // Logger::info("submits=", submits);
        Logger::info("ops_per_sec=", static_cast<double>(ops) / seconds);

        // close(fd);
    }
};


int main(int argc, char** argv) {
    Config cfg;
    cfg.parse(argc, argv);


    auto& stats = StatsPrinter::get();
    if (cfg.stats_interval > 0) {
        stats.interval = cfg.stats_interval;
        stats.start();
    }


    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(cfg.num_threads);
    for (int i = 0; i < cfg.num_threads; ++i) {
        auto w = std::make_unique<Worker>(cfg, i);
        stats.register_aggr(w->stats_scope, w->ops, "ops");
        // stats.register_aggr(w->stats_scope, w->outstanding_ios, "outstanding", false);
        workers.push_back(std::move(w));
    }

    for (auto& w : workers) {
        w->start();
    }

    // warmup
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    for (auto& w : workers) {
        w->record = true;
    }

    std::jthread iops_tracker([&](std::stop_token token) {
        std::vector<uint16_t> ios;
        ios.reserve(1e5);
        while (!token.stop_requested()) {
            auto start = std::chrono::high_resolution_clock::now();

            uint64_t ios_sum = 0;
            for (auto& w : workers) {
                ios_sum += w->outstanding_ios;
            }
            ios.push_back(ios_sum);

            if (ios.size() == ios.capacity()) {
                break;
            }

            auto end = std::chrono::high_resolution_clock::now();
            auto duration = end - start;
            auto target = std::chrono::microseconds(1) - duration;
            busy_sleep(target);
        }

        std::ofstream f("outstanding_io.csv", std::ios::app);
        if (f.tellp() == 0) {
            f << "label,ts,ios\n";
        }
        for (size_t i = 0; auto& val : ios) {
            f << cfg.label << "," << i++ << "," << val << "\n";
        }
        Logger::info("outstanding_ios.csv written");
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(cfg.duration));

    stats.stop();
    for (auto& w : workers) {
        w->request_stop();
    }
    for (auto& w : workers) {
        w->stop();
    }


    std::ofstream f("latencies.csv", std::ios::app);
    if (f.tellp() == 0) {
        f << "label,worker,lat\n";
    }
    for (size_t wid = 0; auto& w : workers) {
        for (size_t i = 0; i < w->latencies.size(); ++i) {
            auto us = static_cast<double>(w->latencies[i]) / 1e3;
            f << cfg.label << "," << wid << "," << us << "\n";
            if (i < 2) {
                std::cout << wid << " " << us << "\n";
            }
        }
        wid++;
    }


    return 0;
}
