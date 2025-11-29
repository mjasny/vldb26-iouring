#include "utils/cli_parser.hpp"
#include "utils/cpu_map.hpp"
#include "utils/my_logger.hpp"
#include "utils/perfevent.hpp"
#include "utils/socket.hpp"
#include "utils/stats_printer.hpp"
#include "utils/utils.hpp"

#include <cerrno>
#include <cstring>
#include <liburing.h>
#include <memory>
#include <netinet/tcp.h>
#include <stdio.h>
#include <sys/socket.h>
#include <utils/singleton.hpp>


struct Config : Singleton<Config> {
    std::string ip = "127.0.0.1";
    uint16_t port = 1234;
    int core_id = 3;
    uint32_t num_threads = 1;
    size_t size = 1024;
    bool tcp = true;
    bool pingpong = true;
    bool perfevent = false;
    uint32_t duration = 0;
    uint32_t conn_per_thread = 1;


    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--ip", ip, cli::Parser::optional);
        parser.parse("--port", port, cli::Parser::optional);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--num_threads", num_threads, cli::Parser::optional);
        parser.parse("--size", size, cli::Parser::optional);
        parser.parse("--tcp", tcp, cli::Parser::optional);
        parser.parse("--pingpong", pingpong, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--duration", duration, cli::Parser::optional);
        parser.parse("--conn_per_thread", conn_per_thread, cli::Parser::optional);
        parser.check_unparsed();
        parser.print();
    }
};

#include <sys/epoll.h>

constexpr size_t MAX_EVENTS = 256;


struct Connection {
    int fd;

    std::unique_ptr<uint8_t[]> send_buf;
    size_t buf_size;
    uint64_t bytes_send = 0;
    bool can_send = true;
    size_t send_offset = 0;

    Connection() {
        auto& cfg = Config::get();
        send_buf = std::make_unique<uint8_t[]>(cfg.size);
        buf_size = cfg.size;
    }
};


struct Worker {
    uint32_t id;
    int epoll_fd = -1;
    StatsPrinter::Scope stats_scope;
    uint64_t bytes_send = 0;
    std::jthread thread;

    Config& cfg;
    std::vector<Connection*> conns;

    static constexpr bool NO_EPOLL = false;

    Worker(int id) : id(id), cfg(Config::get()) {

        epoll_fd = epoll_create1(0); // create1 is newer api
        check_ret(epoll_fd);

        auto& stats = StatsPrinter::get();

        for (uint32_t i = 0; i < cfg.conn_per_thread; ++i) {
            int fd = connect_to(cfg.ip.c_str(), cfg.port);

            auto conn = new Connection{};
            conn->fd = fd;
            conns.push_back(conn);

            if constexpr (NO_EPOLL) {
                break;
            }
            set_nodelay(fd);
            set_nonblocking(fd);
            struct epoll_event event;
            event.events = EPOLLOUT | EPOLLET; // Edge-triggered mode
            event.data.ptr = conn;
            check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn->fd, &event));

            // stats.register_var(stats_scope, conn->bytes_send, "bw_" + std::to_string(i));
        }
    }

    ~Worker() {
        if (thread.joinable()) {
            thread.join();
        }
        for (auto conn : conns) {
            close(conn->fd);
            delete conn;
        }
        close(epoll_fd);
    }

    void start() {
        thread = std::jthread([&](std::stop_token token) {
            if (cfg.core_id != -1) {
                CPUMap::get().pin(cfg.core_id + id);
            }
            run(token);
        });
    }

    void run(std::stop_token token) {
        if constexpr (NO_EPOLL) {
            auto conn = conns.at(0);
            while (!token.stop_requested()) {
                ssize_t len = send(conn->fd, conn->send_buf.get(), conn->buf_size, MSG_WAITALL);
                ensure(len == conn->buf_size); // disable non-blocking above
                bytes_send += len;
            }
            Logger::info("Worker exit");
            return;
        }

        bool perf_running = false;
        std::unique_ptr<PerfEvent> e;
        if (cfg.perfevent) {
            e = std::make_unique<PerfEvent>();
        }

        int msg_flags = 0; // MSG_ZEROCOPY;

        struct epoll_event events[MAX_EVENTS];
        while (!token.stop_requested()) {
            if (e && !perf_running) {
                e->startCounters();
                perf_running = true;
            }

            bool blocked = false;
            for (auto conn : conns) {
                if (!conn->can_send) {
                    blocked = true;
                    continue;
                }
                auto start = conn->send_buf.get() + conn->send_offset;
                auto send_len = conn->buf_size - conn->send_offset;
                ssize_t len = send(conn->fd, start, send_len, msg_flags);
                if (len == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        conn->can_send = false;
                        continue;
                    }

                    check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->fd, NULL));
                    Logger::error("error with: ", conn->fd);
                    continue;
                }
                conn->send_offset += len;
                if (conn->send_offset == conn->buf_size) {
                    conn->send_offset = 0;
                }
                bytes_send += len;
                conn->bytes_send += len;
                conn->can_send = true; // have second try before epoll
            }
            if (!blocked) {
                continue; // EPOLLOUT is only triggered if we actually have a blocked
            }

            int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
            check_ret(num_events);

            for (int i = 0; i < num_events; i++) {
                if (events[i].events & EPOLLOUT) {
                    auto conn = reinterpret_cast<Connection*>(events[i].data.ptr);
                    conn->can_send = true;
                }
            }
        }
        Logger::info("Worker exit");

        if (e) {
            e->stopCounters();
            e->printReport(std::cout, bytes_send);
        }
    }
};

int main(int argc, char** argv) {
    auto& cfg = Config::get();
    cfg.parse(argc, argv);

    auto& stats = StatsPrinter::get();
    stats.start();

    StatsPrinter::Scope stats_scope;


    if constexpr (Worker::NO_EPOLL) {
        ensure(cfg.conn_per_thread == 1);
    }

    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(cfg.num_threads);
    for (int i = 0; i < cfg.num_threads; ++i) {
        auto w = std::make_unique<Worker>(i);
        // stats.register_var(w->stats_scope, w->bytes_send, "bw_" + std::to_string(i));
        w->start();
        stats.register_aggr(w->stats_scope, w->bytes_send, "bw");
        workers.push_back(std::move(w));
    }

    stats.register_func(stats_scope, [&](auto& ss) {
        static Diff<uint64_t> diff;
        uint64_t sum = 0;
        for (auto& w : workers) {
            sum += w->bytes_send;
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

    return 0;
}
