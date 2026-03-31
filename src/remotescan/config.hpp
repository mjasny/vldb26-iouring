#pragma once

#include "utils/cli_parser.hpp"
#include "utils/literals.hpp"
#include "utils/types.hpp"

#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

namespace remotescan {

constexpr uint32_t kDefaultRequestSize = 1_MiB;
constexpr uint32_t kDefaultStripeSize = 1_MiB;
constexpr uint32_t kBlockSize = 4096;
constexpr uint32_t kDefaultIoUringRingEntries = 4096;
constexpr uint32_t kDefaultEpollLibaioRingEntries = 64;

enum class Mode {
    SERVER,
    CLIENT,
};

enum class Backend {
    IO_URING,
    EPOLL_LIBAIO,
};

std::ostream& operator<<(std::ostream& os, const Mode& mode);
std::istream& operator>>(std::istream& is, Mode& mode);
std::ostream& operator<<(std::ostream& os, const Backend& backend);
std::istream& operator>>(std::istream& is, Backend& backend);

struct Config {
    Mode mode = Mode::CLIENT;
    Backend backend = Backend::IO_URING;
    std::string ip = "127.0.0.1";
    uint16_t port = 1234;
    SetupMode setup_mode = SetupMode::DEFER_TASKRUN;
    uint32_t num_workers = 1;

    bool reg_ring = false;
    bool reg_fds = false;
    bool reg_bufs = false;
    bool napi = false;
    bool send_zc = false;
    bool pin_queues = false;

    uint32_t ring_entries = 0;
    uint32_t num_sockets = 8;
    uint64_t scan_bytes = 0;
    uint64_t duration = 0;
    uint32_t request_size = kDefaultRequestSize;
    uint32_t stripe_size = kDefaultStripeSize;
    uint32_t connect_retries = 30;
    uint32_t socket_sndbuf = 0;
    uint32_t socket_rcvbuf = 0;

    std::vector<std::string> ssds;

    void parse(int argc, char** argv);
};

uint64_t total_requests(const Config& cfg);
uint32_t worker_num_sockets(const Config& cfg, size_t worker_idx);
uint64_t worker_num_requests(const Config& cfg, size_t worker_idx);
uint64_t worker_start_request(const Config& cfg, size_t worker_idx);
uint64_t worker_start_offset(const Config& cfg, size_t worker_idx);
uint64_t worker_scan_bytes(const Config& cfg, size_t worker_idx);
bool has_scan_target(const Config& cfg);
bool has_duration_target(const Config& cfg);

} // namespace remotescan
