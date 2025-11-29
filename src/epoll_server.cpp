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
#include <unistd.h>


struct Config : Singleton<Config> {
    std::string ip = "127.0.0.1";
    uint16_t port = 1234;
    int core_id = 3;
    uint32_t num_threads = 1;
    size_t size = 1024;
    bool tcp = true;
    bool perfevent = false;
    bool stop_after_last = false;


    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--ip", ip, cli::Parser::optional);
        parser.parse("--port", port, cli::Parser::optional);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--num_threads", num_threads, cli::Parser::optional);
        parser.parse("--size", size, cli::Parser::optional);
        parser.parse("--tcp", tcp, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--stop_after_last", stop_after_last, cli::Parser::optional);
        parser.check_unparsed();
        parser.print();
    }
};

#include <sys/epoll.h>

constexpr size_t MAX_EVENTS = 256;
constexpr size_t MAX_CONNS = 1024 * 8;

std::atomic<uint64_t> open_conns;

void handle_client_unfair(int epoll_fd, int client_fd) {
    auto& cfg = Config::get();
    auto buf = std::make_unique<uint8_t[]>(cfg.size);
    while (true) {
        ssize_t bytes_received = recv(client_fd, buf.get(), cfg.size, 0);
        if (bytes_received > 0) {
            // printf("Client %d: %s\n", client_fd, buffer);

            // send(client_fd, buffer, bytes_received, 0);
        } else if (bytes_received == 0) {
            printf("Client %d disconnected\n", client_fd);
            close(client_fd);
            break;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            perror("recv error");
            close(client_fd);
            break;
        }
    }
}


// sudo ip l set dev ens3np0 mtu 9000 => makes it slower?
// sudo ip l set dev ens3np0 mtu 1500
// ip l show dev ens3np0

// parallel -N0 'cat /dev/zero | nc 192.168.1.13 1234' ::: {1..127}
// do not put pv in-between, limits overall throughput


struct Worker {
    int epoll_fd;
    StatsPrinter::Scope stats_scope;
    uint64_t bytes_recv = 0;
    std::jthread thread;

    Config& cfg;
    int server_fd = -1;
    std::function<void(int)> on_accept;
    std::unique_ptr<uint8_t[]> recv_buf;
    size_t recv_buf_size;
    std::unique_ptr<PerfEvent> e;

    Worker() : cfg(Config::get()) {
        epoll_fd = epoll_create1(0); // create1 is newer api
        check_ret(epoll_fd);
        recv_buf = std::make_unique<uint8_t[]>(cfg.size);
        recv_buf_size = cfg.size;
    }

    ~Worker() {
        if (thread.joinable()) {
            thread.join();
        }
        close(epoll_fd);
    }

    void add_accept(int fd, std::function<void(int)> on_accept) {
        server_fd = fd;
        this->on_accept = on_accept;
        struct epoll_event event;
        event.events = EPOLLIN; // Listen for incoming connections
        event.data.fd = server_fd;
        check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event));
    }

    void start(int core_id) {
        thread = std::jthread([&, core_id](std::stop_token token) {
            if (core_id != -1) {
                CPUMap::get().pin(core_id);
            }
            run(token);
        });
    }

    void run(std::stop_token token) {
        bool perf_running = false;
        if (cfg.perfevent) {
            e = std::make_unique<PerfEvent>();
        }

        struct epoll_event events[MAX_EVENTS];
        while (!token.stop_requested()) {
            int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
            check_ret(num_events);

            for (int i = 0; i < num_events; i++) {
                int event_fd = events[i].data.fd;

                if (event_fd == server_fd) {
                    while (true) {
                        int fd = accept(server_fd, nullptr, 0);
                        if (fd == -1) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break; // No more incoming connections
                            }
                            check_ret(fd);
                        }
                        set_nonblocking(fd);
                        set_nodelay(fd);

                        ensure(static_cast<bool>(on_accept));
                        on_accept(fd);

                        if (e && !perf_running) {
                            e->startCounters();
                            perf_running = true;
                        }
                    }
                } else {
                    handle_client(event_fd);
                }
            }
        }


        Logger::info("Worker exit");
    }

    void add_client(int fd) {
        struct epoll_event event{};
        // event.events = EPOLLIN | EPOLLET; // Edge-triggered. An event is triggered only when new data arrives.
        // The server must read all available data in a loop (while(recv() > 0) {}).
        event.events = EPOLLIN | EPOLLONESHOT;
        event.data.fd = fd;
        check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event));
    }


    void handle_client(int client_fd) {
        ssize_t bytes_received = recv(client_fd, recv_buf.get(), recv_buf_size, 0);
        if (bytes_received == 0 || (bytes_received == -1 && errno != EAGAIN)) {
            Logger::info("close: ", client_fd);
            close(client_fd);
            --open_conns;
            if (cfg.stop_after_last) {
                if (open_conns == 0) {
                    if (e) {
                        e->stopCounters();
                        e->printReport(std::cout, bytes_recv);
                    }
                    exit(0);
                }
            }
            return;
        }

        bytes_recv += bytes_received;

        // Re-arm the client socket for the next event
        struct epoll_event event;
        event.events = EPOLLIN | EPOLLONESHOT;
        event.data.fd = client_fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &event);
    }
};

int main(int argc, char** argv) {
    auto& cfg = Config::get();
    cfg.parse(argc, argv);

    if (cfg.core_id != -1) {
        CPUMap::get().pin(cfg.core_id);
    }

    auto& stats = StatsPrinter::get();

    int server_fd = listen_on(cfg.ip.c_str(), cfg.port);
    set_nonblocking(server_fd);

    Logger::info("Server is running on port ", cfg.port, "...");


    uint64_t nr_conns = 0;
    StatsPrinter::Scope stats_scope;
    stats.register_var(stats_scope, nr_conns, "conns", false);
    stats.start();


    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(cfg.num_threads);
    for (int i = 0; i < cfg.num_threads; ++i) {
        auto w = std::make_unique<Worker>();
        if (i > 0) {
            w->start((cfg.core_id == -1) ? -1 : cfg.core_id + i);
        }
        // stats.register_var(w->stats_scope, w->bytes_recv, "bw_" + std::to_string(i));
        stats.register_aggr(w->stats_scope, w->bytes_recv, "bw");
        workers.push_back(std::move(w));
    }

    stats.register_func(stats_scope, [&](auto& ss) {
        static Diff<uint64_t> diff;
        uint64_t sum = 0;
        for (auto& w : workers) {
            sum += w->bytes_recv;
        }
        ss << " bw_mib=" << diff(sum) / (1UL << 20);
    });

    auto& w = workers.at(0);
    std::stop_source stop_source;
    std::stop_token stop_token = stop_source.get_token();

    int next_worker = 0;
    w->add_accept(server_fd, [&](int fd) {
        ensure(nr_conns < MAX_CONNS, "max clients reached");
        Logger::info("New client: ", nr_conns, "/", fd, " to worker: ", next_worker);
        ++nr_conns;
        ++open_conns;

        workers.at(next_worker)->add_client(fd);
        if (++next_worker == workers.size()) {
            next_worker = 0;
        }
    });
    w->run(stop_token);


    Logger::info("Exit");
    close(server_fd);
    return 0;
}
