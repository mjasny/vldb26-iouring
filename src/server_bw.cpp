#include "shuffle/utils.hpp"
#include "utils/cli_parser.hpp"
#include "utils/cpu_map.hpp"
#include "utils/hugepages.hpp"
#include "utils/iou_bufring.hpp"
#include "utils/literals.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/perfevent.hpp"
#include "utils/singleton.hpp"
#include "utils/socket.hpp"
#include "utils/stats_printer.hpp"
#include "utils/types.hpp"
#include "utils/utils.hpp"

#include <algorithm>
#include <asm-generic/socket.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <liburing.h>
#include <memory>
#include <netinet/tcp.h>
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
    bool tcp = true;
    bool poll_first = false;
    bool perfevent = false;
    bool stop_after_last = false;
    uint32_t max_clients = 1024;
    bool mshot_recv = false;
    size_t size = 1024;
    bool recv_bundle = false;
    bool hugepages = false;
    bool pin_queues = false;
    int num_brs = 1;


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
        parser.parse("--tcp", tcp, cli::Parser::optional);
        parser.parse("--poll_first", poll_first, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--max_clients", max_clients, cli::Parser::optional);
        parser.parse("--stop_after_last", stop_after_last, cli::Parser::optional);
        parser.parse("--mshot_recv", mshot_recv, cli::Parser::optional);
        parser.parse("--size", size, cli::Parser::optional);
        parser.parse("--recv_bundle", recv_bundle, cli::Parser::optional);
        parser.parse("--hugepages", hugepages, cli::Parser::optional);
        parser.parse("--pin_queues", pin_queues, cli::Parser::optional);
        parser.parse("--num_brs", num_brs, cli::Parser::optional);
        parser.check_unparsed();
        parser.print();
    }
};


struct Client {
    int fd = -1;
    std::unique_ptr<uint8_t[]> buf;
    size_t buf_size;
    std::unique_ptr<BufRing> br;

    uint64_t bytes = 0;
    StatsPrinter::Scope stats_scope;


    Client() {
        auto& cfg = Config::get();

        if (cfg.hugepages) {
            auto* ptr = reinterpret_cast<uint8_t*>(HugePages::malloc(cfg.size));
            buf = std::unique_ptr<uint8_t[]>(ptr);
        } else {
            buf = std::make_unique<uint8_t[]>(cfg.size);
        }

        buf_size = cfg.size;

        auto& stats = StatsPrinter::get();
        static int id = 0;
        stats.register_var(stats_scope, bytes, "bw_" + std::to_string(id++));
    }

    ~Client() {
        auto& cfg = Config::get();
        if (cfg.hugepages) {
            auto ptr = buf.release();
            HugePages::free(ptr, cfg.size);
        }
    }
};


constexpr uint64_t NEW_CLIENT = 0xffffffff'ffffffff;
constexpr uint64_t MSG_SENT = 0xffffffff'fffffffe;
constexpr uint64_t MSG_CLIENT = 0xffffffff'fffffffd;
constexpr uint64_t MSG_WAKE = 0xffffffff'fffffffc;

struct Worker {
    struct io_uring ring;
    StatsPrinter::Scope stats_scope;
    uint64_t bytes_recv = 0;
    std::jthread thread;

    bool do_listen;
    int server_fd = -1;
    std::function<std::vector<std::unique_ptr<Worker>>&()> get_workers;
    // std::unique_ptr<BufRing> br;
    std::vector<std::unique_ptr<BufRing>> brs;
    // std::unique_ptr<uint8_t[]> recv_buf;
    // size_t recv_buf_size;

    uint64_t nr_conns = 0;
    Config& cfg;
    uint32_t my_id;

    int fixed_fd_offset = 0;

    Worker(int core_id, bool do_listen) : do_listen(do_listen), cfg(Config::get()) {
        my_id = core_id;
        thread = std::jthread([&, core_id](std::stop_token token) {
            if (core_id != -1) {
                CPUMap::get().pin(core_id);
            }
            init();
            run(token);
        });
    }

    ~Worker() {
        if (thread.joinable()) {
            thread.join();
        }
        if (server_fd != -1) {
            close(server_fd);
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

        if (cfg.reg_bufs) {
            brs.reserve(cfg.num_brs);
            for (int i = 0; i < cfg.num_brs; ++i) {
                auto br = std::make_unique<BufRing>(&ring, cfg.max_clients * 32, cfg.size);
                Logger::info("bufring[", i + 1, "/", cfg.num_brs, "]", br->avail());
                brs.push_back(std::move(br));
            }
        }


        if (cfg.reg_fds) {
            check_iou(io_uring_register_files_sparse(&ring, cfg.max_clients + 1));
        }


        if (do_listen) {
            server_fd = listen_on(cfg.ip.c_str(), cfg.port);
            if (cfg.reg_fds) {
                check_iou(io_uring_register_files_update(&ring, /*off*/ fixed_fd_offset, &server_fd, 1));
                server_fd = fixed_fd_offset;
                fixed_fd_offset++;
            }

            auto sqe = io_uring_get_sqe(&ring);
            // if (!cfg.reg_fds) {
            //     io_uring_prep_multishot_accept(sqe, server_fd, nullptr, nullptr, 0);
            // } else {
            //     io_uring_prep_multishot_accept_direct(sqe, server_fd, nullptr, nullptr, 0);
            //     sqe->flags |= IOSQE_FIXED_FILE;
            // }
            Logger::info("server_fd=", server_fd);
            io_uring_prep_multishot_accept(sqe, server_fd, nullptr, nullptr, 0);
            if (cfg.reg_fds) {
                sqe->flags |= IOSQE_FIXED_FILE;
            }
            io_uring_sqe_set_data64(sqe, NEW_CLIENT);
        }

        Logger::info("init done");
        Logger::flush();
    }


    void prep_recv(Client* client) {
        auto sqe = io_uring_get_sqe(&ring);
        if (cfg.mshot_recv) {
            io_uring_prep_recv_multishot(sqe, client->fd, nullptr, 0, 0);
        } else {
            io_uring_prep_recv(sqe, client->fd, client->buf.get(), client->buf_size, MSG_WAITALL);
        }
        io_uring_sqe_set_data(sqe, client);
        if (cfg.reg_bufs) {
            client->br->set_bg(sqe);
        }
        if (cfg.recv_bundle) {
            sqe->ioprio |= IORING_RECVSEND_BUNDLE;
        }
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        // check flag and set: sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    };


    void run(std::stop_token token) {
        bool perf_running = false;
        std::unique_ptr<PerfEvent> e;
        if (cfg.perfevent) {
            e = std::make_unique<PerfEvent>();
        }

        uint64_t inflight_msgs = 0;
        int next_worker = 0;

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
                        static bool printed = false;
                        if (!printed) {
                            Logger::info("out of bufs");
                            printed = true;
                        }
                    } else {
                        fprintf(stderr, "Server startup failed. step %d got %d \n", head, cqe->res);
                        Logger::error("CQE Userdata: ", io_uring_cqe_get_data64(cqe));
                        Logger::error("CQE Error: ", std::strerror(-cqe->res));
                        check_iou(cqe->res);
                    }
                }

                auto user_data = io_uring_cqe_get_data64(cqe);
                switch (user_data) {
                    case NEW_CLIENT: {
                        int fd = cqe->res;
                        Logger::info("Client ", fd, " connected");

                        ensure(static_cast<bool>(get_workers));
                        auto& workers = get_workers();

                        auto& target = workers.at(next_worker);
                        int ring_fd = target->ring.ring_fd;

                        auto sqe = io_uring_get_sqe(&ring);
                        // if (!cfg.reg_fds || next_worker == 0) { // cannot send fixed fd to ourself
                        //     io_uring_prep_msg_ring(sqe, ring_fd, fd, MSG_CLIENT, 0);
                        // } else {
                        //     io_uring_prep_msg_ring_fd_alloc(sqe, ring_fd, fd, MSG_CLIENT, 0);
                        // }
                        io_uring_prep_msg_ring(sqe, ring_fd, fd, MSG_CLIENT, 0);
                        io_uring_sqe_set_data64(sqe, MSG_SENT);
                        ++inflight_msgs;

                        if (++next_worker == workers.size()) {
                            next_worker = 0;
                        }
                        break;
                    }
                    case MSG_WAKE: {
                        break;
                    }
                    case MSG_SENT: {
                        /// Logger::info("Sent msg");
                        --inflight_msgs;
                        break;
                    }
                    case MSG_CLIENT: {
                        auto client = new Client();
                        if (cfg.reg_bufs) {
                            ensure(brs.size() > 0);
                            client->br = std::move(brs.back());
                            brs.pop_back();
                        }
                        Logger::info("Client ", client->fd, " via msg my_id:", my_id);


                        int fd = cqe->res;

                        if (0) {
                            int sz = 32_MiB;
                            check_ret(setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz)));
                            int val = 0;
                            socklen_t len = sizeof(val);
                            getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &val, &len);
                            printf("SO_RCVBUF (effective) = %d bytes\n", val);
                            ensure(val == sz * 2); // OS doubles
                        }


                        if (cfg.pin_queues) {
                            static std::mutex mutex;
                            const std::lock_guard<std::mutex> lock(mutex);
                            assign_flow_to_rx_queue(fd, this->my_id + 1);
                        }

                        if (cfg.reg_fds) {
                            check_iou(io_uring_register_files_update(&ring, /*off*/ fixed_fd_offset, &fd, 1));
                            fd = fixed_fd_offset;
                            fixed_fd_offset++;
                        }
                        client->fd = fd;


                        ++nr_conns;
                        prep_recv(client);
                        if (e && !perf_running) {
                            e->startCounters();
                            perf_running = true;
                        }
                        break;
                    }
                    default: {
                        auto client = reinterpret_cast<Client*>(io_uring_cqe_get_data(cqe));


                        if (cqe->res == 0) {
                            Logger::info("Client ", client->fd, " disconnected");
                            --nr_conns;
                            delete client;
                            continue;
                        }

                        static int recvs = 0;
                        recvs++;

                        bool out_of_bufs = (cqe->res == -ENOBUFS);
                        if (out_of_bufs) {
                            // Logger::info("bufring ", client->br->avail());

                            // ensure(cfg.mshot_recv);
                            // prep_recv(client);
                            // Logger::info("rearm");
                            // Logger::info("recvs=", recvs);
                            // break;
                        }
                        // ensure(!out_of_bufs);

                        if (cfg.reg_bufs) {
                            if (!cfg.recv_bundle) {
                                // ensure(cqe->res <= cfg.size, [&] {
                                //     return std::to_string(cqe->res);
                                // });
                                client->br->add_from_cqe(cqe);
                            } else {
                                client->br->add_bundle_from_cqe(cqe, cqe->res);
                            }
                            // Logger::info("added back ", client->br->avail());
                        }

                        bytes_recv += cqe->res;
                        client->bytes += cqe->res;
                        // last_recv = cqe->res;

                        if (cfg.mshot_recv) {
                            if (!(cqe->flags & IORING_CQE_F_MORE)) {
                                prep_recv(client);
                            }
                        } else {
                            prep_recv(client);
                        }
                        break;
                    }
                }
            }
            io_uring_cq_advance(&ring, i);

            if (cfg.stop_after_last && nr_conns == 0) {
                if (do_listen && inflight_msgs == 0) {
                    ensure(static_cast<bool>(get_workers));
                    auto& workers = get_workers();

                    Logger::info("sending wakeups");
                    for (size_t i = 1; i < workers.size(); ++i) {
                        auto& target = workers.at(i);
                        int ring_fd = target->ring.ring_fd;
                        auto sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_msg_ring(sqe, ring_fd, 0, MSG_WAKE, 0);
                        io_uring_sqe_set_data64(sqe, MSG_SENT);
                        ++inflight_msgs;
                    }
                    do_listen = false;
                }

                if (inflight_msgs == 0) {
                    break;
                }
            }
        }

        Logger::info("Worker exit ", my_id);

        if (e) {
            e->stopCounters();
            e->printReport(std::cout, bytes_recv);
        }
    }
};


int main(int argc, char** argv) {
    auto& cfg = Config::get();
    cfg.parse(argc, argv);


    // if (cfg.core_id != -1) {
    //     CPUMap::get().pin(cfg.core_id);
    // }


    struct io_uring_sqe* sqe;
    int val, submitted;
    unsigned head;


    ensure(cfg.tcp);
    if (cfg.mshot_recv) {
        ensure(cfg.reg_bufs);
    }
    if (cfg.recv_bundle) {
        ensure(cfg.reg_bufs);
    }

    auto& stats = StatsPrinter::get();
    stats.start();


    StatsPrinter::Scope stats_scope;

    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(cfg.num_threads);
    for (int i = 0; i < cfg.num_threads; ++i) {
        auto core_id = (cfg.core_id == -1) ? -1 : cfg.core_id + i * 2;
        auto w = std::make_unique<Worker>(core_id, i == 0);

        stats.register_aggr(w->stats_scope, w->bytes_recv, "bw");
        stats.register_aggr(w->stats_scope, w->nr_conns, "nr_cons", false);
        // if (cfg.num_threads > 1) {
        //     stats.register_var(w->stats_scope, w->bytes_recv, "bw_" + std::to_string(i));
        // }
        workers.push_back(std::move(w));
    }

    auto& w = workers.at(0);
    w->get_workers = [&]() {
        return std::ref(workers);
    };


    stats.register_func(stats_scope, [&](auto& ss) {
        static Diff<uint64_t> diff;
        uint64_t sum = 0;
        for (auto& w : workers) {
            sum += w->bytes_recv;
        }
        ss << " bw_mib=" << diff(sum) / (1UL << 20);
    });

    workers.clear();


    Logger::info("Exit");
}
