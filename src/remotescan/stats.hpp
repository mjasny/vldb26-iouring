#pragma once

#include <chrono>
#include <cstdint>

namespace remotescan {

struct WorkerStats {
    uint64_t bytes = 0;
    uint64_t ssd_read_bytes = 0;
    uint64_t send_bytes = 0;
    uint64_t recv_bytes = 0;
    std::chrono::steady_clock::time_point start_tp {};
    std::chrono::steady_clock::time_point end_tp {};
};

inline WorkerStats aggregate_worker_stats(const WorkerStats& lhs, const WorkerStats& rhs) {
    WorkerStats out;
    out.bytes = lhs.bytes + rhs.bytes;
    out.ssd_read_bytes = lhs.ssd_read_bytes + rhs.ssd_read_bytes;
    out.send_bytes = lhs.send_bytes + rhs.send_bytes;
    out.recv_bytes = lhs.recv_bytes + rhs.recv_bytes;
    out.start_tp = lhs.start_tp == std::chrono::steady_clock::time_point {} ? rhs.start_tp
                  : rhs.start_tp == std::chrono::steady_clock::time_point {} ? lhs.start_tp
                  : std::min(lhs.start_tp, rhs.start_tp);
    out.end_tp = std::max(lhs.end_tp, rhs.end_tp);
    return out;
}

} // namespace remotescan
