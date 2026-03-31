#pragma once

#include "remotescan/common.hpp"
#include "remotescan/protocol.hpp"
#include "remotescan/raid.hpp"
#include "remotescan/stats.hpp"
#include "remotescan/worker_base.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

namespace remotescan {

struct ServerConnection {
    int raw_fd = -1;
    int ring_fd = -1;
    uint32_t fd_slot = 0;
    uint32_t payload_buf_index = 0;

    AlignedBuffer payload;
    ScanRequest request {};

    uint32_t bytes_sent = 0;
    uint64_t requests_done = 0;
    bool recv_posted = false;
    bool read_posted = false;
    bool send_posted = false;
    bool waiting_for_zc_notif = false;

    explicit ServerConnection(uint32_t request_size);
    ~ServerConnection();
};

class ServerWorker final : public WorkerBase {
public:
    ServerWorker(const Config& cfg, size_t worker_idx);
    ~ServerWorker() override;

    void run();
    const WorkerStats& stats() const;

private:
    RaidLayout raid;
    uint16_t port;
    uint32_t num_sockets;
    uint64_t target_bytes;
    int listen_fd = -1;
    uint32_t listen_slot = 0;
    uint32_t ssd_fixed_base = 1;
    uint32_t conn_fixed_base = 0;

    std::vector<std::unique_ptr<ServerConnection>> conns;
    uint64_t total_bytes = 0;
    uint64_t ssd_read_bytes = 0;
    uint64_t send_bytes = 0;
    uint64_t accepted = 0;
    uint64_t closed = 0;
    std::chrono::steady_clock::time_point start_tp {};
    std::chrono::steady_clock::time_point end_tp {};

    void init_server();
    void prep_accept();
    void prep_recv_request(uint32_t idx);
    void prep_read(uint32_t idx);
    void prep_send(uint32_t idx);
    void finish_request(ServerConnection& conn);
    void event_loop();
    WorkerStats stats_ {};
};

} // namespace remotescan
