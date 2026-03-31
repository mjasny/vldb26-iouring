#include "remotescan/pinning.hpp"

#include "shuffle/utils.hpp"

#include <mutex>

namespace remotescan {

std::array<WorkerPinInfo, 32> pin_info_rr = {{
    {.core_id = 2, .tx_queue = 2, .rx_queue = 3},
    {.core_id = 4, .tx_queue = 4, .rx_queue = 5},
    {.core_id = 8, .tx_queue = 6, .rx_queue = 7},
    {.core_id = 10, .tx_queue = 8, .rx_queue = 9},
    {.core_id = 12, .tx_queue = 10, .rx_queue = 11},
    {.core_id = 16, .tx_queue = 12, .rx_queue = 13},
    {.core_id = 18, .tx_queue = 14, .rx_queue = 15},
    {.core_id = 20, .tx_queue = 16, .rx_queue = 17},
    {.core_id = 24, .tx_queue = 18, .rx_queue = 19},
    {.core_id = 26, .tx_queue = 20, .rx_queue = 21},
    {.core_id = 28, .tx_queue = 22, .rx_queue = 23},
    {.core_id = 32, .tx_queue = 24, .rx_queue = 25},
    {.core_id = 34, .tx_queue = 26, .rx_queue = 27},
    {.core_id = 36, .tx_queue = 28, .rx_queue = 29},
    {.core_id = 40, .tx_queue = 30, .rx_queue = 31},
    {.core_id = 42, .tx_queue = 32, .rx_queue = 33},
    {.core_id = 44, .tx_queue = 34, .rx_queue = 35},
    {.core_id = 48, .tx_queue = 36, .rx_queue = 37},
    {.core_id = 50, .tx_queue = 38, .rx_queue = 39},
    {.core_id = 52, .tx_queue = 40, .rx_queue = 41},
    {.core_id = 56, .tx_queue = 42, .rx_queue = 43},
    {.core_id = 58, .tx_queue = 44, .rx_queue = 45},
    {.core_id = 60, .tx_queue = 46, .rx_queue = 47},
    {.core_id = 64, .tx_queue = 48, .rx_queue = 49},
    {.core_id = 66, .tx_queue = 50, .rx_queue = 51},
    {.core_id = 72, .tx_queue = 52, .rx_queue = 53},
    {.core_id = 74, .tx_queue = 54, .rx_queue = 55},
    {.core_id = 80, .tx_queue = 56, .rx_queue = 57},
    {.core_id = 82, .tx_queue = 58, .rx_queue = 59},
    {.core_id = 88, .tx_queue = 60, .rx_queue = 61},
    {.core_id = 90, .tx_queue = 62, .rx_queue = 62},
    {.core_id = 0, .tx_queue = 0, .rx_queue = 1},
}};

std::array<WorkerPinInfo, 32>& pin_info = pin_info_rr;

namespace {
std::mutex mutex;
}

void pin_socket_tx_queue(int fd, size_t idx) {
    int tx_queue = pin_info.at(idx % pin_info.size()).tx_queue;
    const std::lock_guard<std::mutex> lock(mutex);
    assign_flow_to_rx_queue(fd, tx_queue);
}

void pin_socket_rx_queue(int fd, size_t idx) {
    int rx_queue = pin_info.at(idx % pin_info.size()).rx_queue;
    const std::lock_guard<std::mutex> lock(mutex);
    assign_flow_to_rx_queue(fd, rx_queue);
}

void pin_socket_queues(int fd, size_t idx) {
    pin_socket_tx_queue(fd, idx);
    pin_socket_rx_queue(fd, idx);
}

} // namespace remotescan
