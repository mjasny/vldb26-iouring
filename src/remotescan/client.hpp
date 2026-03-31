#pragma once

#include "remotescan/common.hpp"
#include "remotescan/protocol.hpp"
#include "remotescan/stats.hpp"
#include "remotescan/worker_base.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <stop_token>
#include <vector>

namespace remotescan {

struct ClientConnection {
    int raw_fd = -1;
    int ring_fd = -1;
    uint32_t fd_slot = 0;
    uint32_t payload_buf_index = 0;

    AlignedBuffer payload;
    ScanRequest request {};

    uint64_t bytes_recv = 0;
    uint64_t requests_done = 0;
    bool send_posted = false;
    bool recv_posted = false;
    bool finished = false;

    explicit ClientConnection(uint32_t request_size);
    ~ClientConnection();
};

class ClientWorker final : public WorkerBase {
public:
    ClientWorker(const Config& cfg, size_t worker_idx);
    void run(std::stop_token stop_token = {});
    const WorkerStats& stats() const;

private:
    uint16_t port;
    uint32_t num_sockets;
    uint64_t start_offset;
    uint64_t end_offset;
    std::vector<std::unique_ptr<ClientConnection>> conns;
    uint64_t next_offset;
    uint64_t total_bytes = 0;
    uint64_t recv_bytes = 0;
    std::chrono::steady_clock::time_point start_tp {};
    std::chrono::steady_clock::time_point end_tp {};

    void init_client();
    bool assign_next_request(ClientConnection& conn);
    void prep_send_request(uint32_t idx);
    void prep_recv_payload(uint32_t idx);
    void event_loop(std::stop_token stop_token);
    bool should_issue_more(std::stop_token stop_token) const;
    bool all_finished() const;
    WorkerStats stats_ {};
};

} // namespace remotescan
