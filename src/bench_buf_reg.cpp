#include "utils/cli_parser.hpp"
#include "utils/cpu_map.hpp"
#include "utils/hugepages.hpp"
#include "utils/literals.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/perfevent.hpp"
#include "utils/range_helper.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/ring.hpp"
#include "utils/singleton.hpp"
#include "utils/stopper.hpp"
#include "utils/threadpool.hpp"
#include "utils/types.hpp"

#include <chrono>
#include <cstring>
#include <iostream>
#include <liburing.h>
#include <memory>
#include <pthread.h>
#include <sys/socket.h>
#include <thread>


enum class Mode {
    NAIVE,
    CLONE,
};

std::ostream& operator<<(std::ostream& os, const Mode& arg) {
    switch (arg) {
        case Mode::NAIVE:
            os << "naive";
            break;
        case Mode::CLONE:
            os << "clone";
            break;
    }
    return os;
}

std::istream& operator>>(std::istream& is, Mode& mode) {
    std::string token;
    is >> token;

    if (token == "naive") {
        mode = Mode::NAIVE;
    } else if (token == "clone") {
        mode = Mode::CLONE;
    } else {
        throw std::invalid_argument("Invalid input for Mode: " + token);
    }
    return is;
}


struct Config : Singleton<Config> {
    int core_id = 3;
    bool perfevent = false;
    Mode mode;
    uint64_t mem_size = 1_GiB;
    uint32_t num_threads = 1;
    bool use_hugepages = false;
    uint64_t chunk_size = 1_GiB;

    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--mode", mode);
        parser.parse("--mem_size", mem_size, cli::Parser::optional);
        parser.parse("--num_threads", num_threads, cli::Parser::optional);
        parser.parse("--use_hugepages", use_hugepages, cli::Parser::optional);
        parser.parse("--chunk_size", chunk_size, cli::Parser::optional);
        parser.check_unparsed();
        parser.print();
    }
};

struct Buffer {
    uint8_t data[65536];
};


int main(int argc, char** argv) {
    auto& cfg = Config::get();
    cfg.parse(argc, argv);


    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, cfg.num_threads + 1);


    Logger::info("allocating memory");
    HugePages huge_pages;
    // std::unique_ptr<class Tp> // align to 4KB
    uint8_t* mem;
    if (cfg.use_hugepages) {
        new (&huge_pages) HugePages(cfg.mem_size);
        // huge_pages.malloc(cfg.mem_size);
        mem = huge_pages.as<uint8_t*>();
    } else {
        mem = reinterpret_cast<uint8_t*>(std::aligned_alloc(4096, cfg.mem_size));
        memset(mem, 0, cfg.mem_size);
    }


    auto num_chunks = (cfg.mem_size + cfg.chunk_size - 1) / cfg.chunk_size;
    Logger::info("num_chunks=", num_chunks);

    std::vector<struct iovec> iov(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i) {
        iov[i].iov_base = reinterpret_cast<uint8_t*>(mem) + (i * cfg.chunk_size);
        size_t remaining = cfg.mem_size - i * cfg.chunk_size;
        iov[i].iov_len = (remaining >= cfg.chunk_size) ? cfg.chunk_size : remaining;
        // Logger::info("i=", i, " addr=", iov[i].iov_base, " len=", iov[i].iov_len);
    }
    Logger::info("alloc done");


    RDTSCClock reg_clock(2.4_GHz);
    RDTSCClock clone_clock(2.4_GHz);

    for (int iter = 0; iter < 10; ++iter) {
        // struct io_uring rings[cfg.num_threads];
        struct io_uring* src_ring;
        pthread_barrier_t clone_barrier;
        pthread_barrier_init(&clone_barrier, nullptr, cfg.num_threads);

        ThreadPool tp;
        tp.parallel_n(cfg.num_threads, [&](std::stop_token token, int id) {
            if (cfg.core_id != -1) {
                CPUMap::get().pin(cfg.core_id + id);
            }


            struct io_uring ring;
            struct io_uring_params params{};
            params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CLAMP;
            params.flags |= IORING_SETUP_CQSIZE;
            params.cq_entries = 131072;
            params.flags |= IORING_SETUP_DEFER_TASKRUN;
            // params.flags |= IORING_SETUP_R_DISABLED;
            check_ret(io_uring_queue_init_params(4096, &ring, &params));

            // ensure(io_uring_register_ring_fd(ring) == 1);


            std::unique_ptr<PerfEvent> e;
            if (cfg.perfevent) {
                e = std::make_unique<PerfEvent>();
            }


            RDTSCClock clock(2.4_GHz);
            pthread_barrier_wait(&barrier);
            if (e) {
                e->startCounters();
            }


            if (cfg.mode == Mode::NAIVE) {
                check_iou(io_uring_register_buffers(&ring, iov.data(), iov.size()));
            } else if (cfg.mode == Mode::CLONE) {
                if (id == 0) {
                    reg_clock.start();
                    check_iou(io_uring_register_buffers(&ring, iov.data(), iov.size()));
                    reg_clock.stop();
                    src_ring = &ring;
                    clone_clock.start();
                }
                pthread_barrier_wait(&clone_barrier);
                if (id != 0) {
                    // Logger::info(io_uring_clone_buffers(ring, &rings[0]) == -ENXIO);
                    check_iou(io_uring_clone_buffers(&ring, src_ring));
                }
                pthread_barrier_wait(&clone_barrier);
                if (id == 0) {
                    clone_clock.stop();
                }
            }


            if (e) {
                e->stopCounters();
                e->printReport(std::cout, 1);
                e->printReport(std::cout, iov.size());
            }

            pthread_barrier_wait(&barrier);

            // auto lat = clock.as<std::chrono::microseconds, uint64_t>();
            // if (cfg.num_threads == 1 && id == 0) {
            //     Logger::info("duration=", lat, "µs");
            //     Logger::info("cycles=", clock.cycles());
            //     Logger::info("cycles_per_byte=", static_cast<double>(clock.cycles()) / cfg.mem_size);
            // }


            io_uring_queue_exit(&ring);
        });

        RDTSCClock clock(2.4_GHz);
        clock.start();
        pthread_barrier_wait(&barrier);
        // does work
        pthread_barrier_wait(&barrier);
        clock.stop();

        auto lat = clock.as<std::chrono::microseconds, uint64_t>();
        Logger::info("outer_duration=", lat, "µs");
        auto reg_lat = reg_clock.as<std::chrono::microseconds, uint64_t>();
        Logger::info("reg_duration=", reg_lat, "µs");
        auto clone_lat = clone_clock.as<std::chrono::microseconds, uint64_t>();
        Logger::info("clone_duration=", clone_lat, "µs");
    }

    pthread_barrier_destroy(&barrier);
    Logger::info("Done");

    if (!cfg.use_hugepages) {
        std::free(mem);
    }

    return 0;
}
