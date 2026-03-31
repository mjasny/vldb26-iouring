#pragma once

#include "remotescan/common.hpp"
#include "remotescan/config.hpp"
#include "remotescan/protocol.hpp"
#include "remotescan/raid.hpp"
#include "remotescan/stats.hpp"

#include <libaio.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <stop_token>
#include <vector>

namespace remotescan {

class EpollLibaioServer {
public:
    EpollLibaioServer(const Config& cfg, size_t worker_idx);
    ~EpollLibaioServer();

    void run();
    const WorkerStats& stats() const;

private:
    struct Connection {
        int fd = -1;
        size_t req_bytes = 0;
        uint32_t send_offset = 0;
        bool reading = false;
        bool sending = false;
        bool closed = false;

        ScanRequest request {};
        AlignedBuffer payload;
        iocb cb {};

        explicit Connection(uint32_t request_size);
        ~Connection();
    };

    const Config& cfg;
    size_t worker_idx;
    RaidLayout raid;
    uint16_t port;
    uint32_t num_sockets;
    uint64_t target_bytes;
    int listen_fd = -1;
    int epoll_fd = -1;
    int aio_event_fd = -1;
    io_context_t aio_ctx {};

    std::vector<std::unique_ptr<Connection>> conns;
    uint64_t total_bytes = 0;
    uint64_t ssd_read_bytes = 0;
    uint64_t send_bytes = 0;
    size_t next_conn = 0;
    size_t closed_conns = 0;
    std::chrono::steady_clock::time_point start_tp {};
    std::chrono::steady_clock::time_point end_tp {};

    void init();
    void handle_accept();
    void handle_conn_read(Connection& conn);
    void submit_read(Connection& conn);
    void handle_aio_completions();
    void handle_conn_write(Connection& conn);
    void reset_for_next_request(Connection& conn);
    void close_conn(Connection& conn);
    void update_interest(Connection& conn, uint32_t events);
    WorkerStats stats_ {};
};

class EpollLibaioClient {
public:
    EpollLibaioClient(const Config& cfg, size_t worker_idx);
    ~EpollLibaioClient();

    void run(std::stop_token stop_token = {});
    const WorkerStats& stats() const;

private:
    struct Connection {
        int fd = -1;
        size_t send_offset = 0;
        size_t recv_offset = 0;
        bool sending = true;
        bool receiving = false;
        bool finished = false;

        ScanRequest request {};
        AlignedBuffer payload;

        explicit Connection(uint32_t request_size);
        ~Connection();
    };

    const Config& cfg;
    size_t worker_idx;
    uint16_t port;
    uint32_t num_sockets;
    int epoll_fd = -1;
    std::vector<std::unique_ptr<Connection>> conns;
    uint64_t next_offset;
    uint64_t end_offset;
    uint64_t total_bytes = 0;
    uint64_t recv_bytes = 0;
    std::chrono::steady_clock::time_point start_tp {};
    std::chrono::steady_clock::time_point end_tp {};

    void init();
    bool assign_next_request(Connection& conn);
    void handle_conn_write(Connection& conn);
    void handle_conn_read(Connection& conn, std::stop_token stop_token);
    void update_interest(Connection& conn, uint32_t events);
    bool should_issue_more(std::stop_token stop_token) const;
    bool all_finished() const;
    WorkerStats stats_ {};
};

} // namespace remotescan
