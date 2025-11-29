#include "shuffle/utils.hpp"
#include "utils/cli_parser.hpp"
#include "utils/cpu_map.hpp"
#include "utils/hugepages.hpp"
#include "utils/literals.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/perfevent.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/singleton.hpp"
#include "utils/socket.hpp"
#include "utils/stats_printer.hpp"
#include "utils/stopper.hpp"
#include "utils/types.hpp"
#include "utils/utils.hpp"

#include <chrono>
#include <cstring>
#include <liburing.h>
#include <memory>
#include <netinet/tcp.h>
#include <ratio>
#include <stdio.h>
#include <sys/socket.h>


struct Config : Singleton<Config> {
    std::string ip = "127.0.0.1";
    uint16_t port = 1234;
    SetupMode setup_mode = SetupMode::DEFAULT;
    int core_id = 3;
    bool napi = false;
    bool reg_ring = false;
    bool reg_bufs = false;
    bool reg_fds = false;
    uint32_t num_threads = 1;
    size_t size = 1024;
    bool tcp = true;
    bool pingpong = true;
    bool perfevent = false;
    uint32_t duration = 0;
    uint32_t conn_per_thread = 1;
    bool send_zc = false;
    bool hugepages = false;
    bool pin_queues = false;


    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--ip", ip, cli::Parser::optional);
        parser.parse("--port", port, cli::Parser::optional);
        parser.parse("--setup_mode", setup_mode, cli::Parser::optional);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--napi", napi, cli::Parser::optional);
        parser.parse("--reg_ring", reg_ring, cli::Parser::optional);
        parser.parse("--reg_bufs", reg_bufs, cli::Parser::optional);
        parser.parse("--reg_fds", reg_fds, cli::Parser::optional);
        parser.parse("--num_threads", num_threads, cli::Parser::optional);
        parser.parse("--size", size, cli::Parser::optional);
        parser.parse("--tcp", tcp, cli::Parser::optional);
        parser.parse("--pingpong", pingpong, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--duration", duration, cli::Parser::optional);
        parser.parse("--conn_per_thread", conn_per_thread, cli::Parser::optional);
        parser.parse("--send_zc", send_zc, cli::Parser::optional);
        parser.parse("--hugepages", hugepages, cli::Parser::optional);
        parser.parse("--pin_queues", pin_queues, cli::Parser::optional);

        parser.check_unparsed();
        parser.print();
    }
};

struct Connection {
    int fd;
    uint16_t buf_idx;

    std::unique_ptr<uint8_t[]> send_buf;
    size_t buf_size;

    Connection() {
        auto& cfg = Config::get();

        if (cfg.hugepages) {
            auto* ptr = reinterpret_cast<uint8_t*>(HugePages::malloc(cfg.size));
            send_buf = std::unique_ptr<uint8_t[]>(ptr);
        } else {
            send_buf = std::make_unique<uint8_t[]>(cfg.size);
        }


        buf_size = cfg.size;
    }

    ~Connection() {
        auto& cfg = Config::get();
        if (cfg.hugepages) {
            auto ptr = send_buf.release();
            HugePages::free(ptr, cfg.size);
        }
    }
};

struct Worker {
    struct io_uring ring;
    StatsPrinter::Scope stats_scope;
    uint64_t bytes_sent = 0;
    std::jthread thread;

    int id;
    Config& cfg;
    std::vector<Connection*> conns; // for cleanup

    Worker(int id) : id(id), cfg(Config::get()) {
        thread = std::jthread([&, id](std::stop_token token) {
            if (cfg.core_id != -1) {
                CPUMap::get().pin(cfg.core_id + id);
            }
            init();
            run(token);
        });
    }

    ~Worker() {
        if (thread.joinable()) {
            thread.join();
        }
        for (auto& conn : conns) {
            delete conn;
        }
        io_uring_queue_exit(&ring);
    }


    void init() {
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


        if (io_uring_queue_init_params(4096, &ring, &params) < 0) {
            fprintf(stderr, "%s\n", strerror(errno));
            exit(1);
        }

        if (cfg.reg_ring) {
            if (!(ring.features & IORING_FEAT_REG_REG_RING)) {
                Logger::error("IORING_FEAT_REG_REG_RING not supported");
                exit(1);
            }
            ensure(io_uring_register_ring_fd(&ring) == 1);
            Logger::info("registered ring fd");
        }

        if (cfg.napi) {
            struct io_uring_napi napi = {};
            napi.prefer_busy_poll = 1;
            napi.busy_poll_to = 50;
            check_iou(io_uring_register_napi(&ring, &napi));
            Logger::info("enabled napi");
        }


        conns.reserve(cfg.conn_per_thread);
        for (int i = 0; i < cfg.conn_per_thread; ++i) {
            auto conn = new Connection{};
            conns.push_back(conn);
        }

        if (cfg.reg_bufs) {
            // br = std::make_unique<BufRing>(&ring, cfg.max_clients, cfg.size);
            // Logger::info("bufring ", br->avail());
            //
            auto iovs = std::vector<struct iovec>(cfg.conn_per_thread);
            for (int i = 0; i < cfg.conn_per_thread; ++i) {
                auto conn = conns.at(i);
                iovs[i].iov_base = conn->send_buf.get();
                iovs[i].iov_len = conn->buf_size;
                conn->buf_idx = i;
            }
            check_iou(io_uring_register_buffers(&ring, iovs.data(), iovs.size()));
        }

        if (cfg.reg_fds) {
            check_iou(io_uring_register_files_sparse(&ring, cfg.conn_per_thread));
        }


        for (int i = 0; i < cfg.conn_per_thread; ++i) {
            auto conn = conns.at(i);
            int fd = connect_to(cfg.ip.c_str(), cfg.port);

            if (cfg.pin_queues) {
                ensure(cfg.num_threads == 1);
                static std::mutex mutex;
                const std::lock_guard<std::mutex> lock(mutex);
                int rx_queue = cfg.core_id + this->id + 1;
                assign_flow_to_rx_queue(fd, rx_queue);
            }

            if (cfg.reg_fds) {
                check_iou(io_uring_register_files_update(&ring, /*off*/ i, &fd, 1));
                fd = i;
            }
            conn->fd = fd;
            prep_send(conn);
        }
    }


    void prep_send(Connection* conn) {
        auto sqe = io_uring_get_sqe(&ring);
        if (cfg.send_zc) {
            // io_uring_prep_send_zc_fixed(sqe, fd, buf, cfg.ping_size, MSG_WAITALL, 0, buf_idx);
            if (cfg.reg_bufs) {
                // https://lore.kernel.org/io-uring/fef75ea0-11b4-4815-8c66-7b19555b279d@kernel.dk/?s=09
                io_uring_prep_send_zc_fixed(sqe, conn->fd, conn->send_buf.get(), conn->buf_size, MSG_WAITALL, 0, conn->buf_idx);
            } else {
                io_uring_prep_send_zc(sqe, conn->fd, conn->send_buf.get(), conn->buf_size, MSG_WAITALL, 0);
            }
        } else {
            io_uring_prep_send(sqe, conn->fd, conn->send_buf.get(), conn->buf_size, MSG_WAITALL);
        }
        io_uring_sqe_set_data(sqe, conn);
        if (cfg.reg_bufs) {
            // br->set_bg(sqe);
        }
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
    }

    void run(std::stop_token token) {
        std::unique_ptr<PerfEvent> e;
        if (cfg.perfevent) {
            e = std::make_unique<PerfEvent>();
            e->startCounters();
        }


        while (!token.stop_requested()) {
            if (cfg.setup_mode == SetupMode::SQPOLL) {
                io_uring_submit(&ring);
            } else {
                io_uring_submit_and_wait(&ring, 1);
            }

            int i = 0;
            uint32_t head;
            struct io_uring_cqe* cqe;
            io_uring_for_each_cqe(&ring, head, cqe) {
                ++i;

                if (cqe->res < 0) {
                    if (cqe->res == -ENOBUFS) {
                        Logger::info("out of bufs");
                    } else {
                        fprintf(stderr, "Server startup failed. step %d got %d \n", head, cqe->res);
                        Logger::error("CQE Userdata: ", io_uring_cqe_get_data64(cqe));
                        Logger::error("CQE Error: ", std::strerror(-cqe->res));
                        check_iou(cqe->res);
                    }
                }


                if (cqe->flags & IORING_CQE_F_NOTIF) {
                    // notification that zc buffer can be re-used
                    continue;
                }


                auto user_data = io_uring_cqe_get_data64(cqe);
                auto conn = reinterpret_cast<Connection*>(io_uring_cqe_get_data(cqe));

                ensure(cqe->res == cfg.size);
                bytes_sent += cqe->res;

                prep_send(conn);
            }
            io_uring_cq_advance(&ring, i);
        }

        Logger::info("Worker exit ", id);

        if (e) {
            e->stopCounters();
            e->printReport(std::cout, bytes_sent);
        }
    }
};


int main(int argc, char** argv) {
    auto& cfg = Config::get();
    cfg.parse(argc, argv);


    ensure(cfg.tcp);

    if (!cfg.send_zc) {
        ensure(!cfg.reg_bufs);
    }

    auto& stats = StatsPrinter::get();
    stats.start();
    StatsPrinter::Scope stats_scope;

    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(cfg.num_threads);
    for (int i = 0; i < cfg.num_threads; ++i) {
        auto w = std::make_unique<Worker>(i);
        stats.register_aggr(w->stats_scope, w->bytes_sent, "bw");
        workers.push_back(std::move(w));
    }

    stats.register_func(stats_scope, [&](auto& ss) {
        static Diff<uint64_t> diff;
        uint64_t sum = 0;
        for (auto& w : workers) {
            sum += w->bytes_sent;
        }
        ss << " bw_mib=" << diff(sum) / (1UL << 20);
    });


    if (cfg.duration > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.duration));
    } else {
        std::this_thread::sleep_until(std::chrono::time_point<std::chrono::system_clock>::max());
    }

    Logger::info("Stopping");
    for (auto& w : workers) {
        w->thread.request_stop();
    }
    Logger::info("Exit");
}
