#pragma once

#include <array>
#include <cstddef>

namespace remotescan {

struct WorkerPinInfo {
    int core_id;
    int tx_queue;
    int rx_queue;
};

extern std::array<WorkerPinInfo, 32> pin_info_rr;
extern std::array<WorkerPinInfo, 32>& pin_info;

void pin_socket_tx_queue(int fd, size_t idx);
void pin_socket_rx_queue(int fd, size_t idx);
void pin_socket_queues(int fd, size_t idx);

} // namespace remotescan
