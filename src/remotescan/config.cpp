#include "remotescan/config.hpp"

#include "utils/my_asserts.hpp"

#include <istream>
#include <ostream>
#include <string>

namespace remotescan {

std::ostream& operator<<(std::ostream& os, const Mode& mode) {
    switch (mode) {
    case Mode::SERVER:
        os << "server";
        break;
    case Mode::CLIENT:
        os << "client";
        break;
    }
    return os;
}

std::istream& operator>>(std::istream& is, Mode& mode) {
    std::string value;
    is >> value;
    if (value == "server") {
        mode = Mode::SERVER;
    } else if (value == "client") {
        mode = Mode::CLIENT;
    } else {
        is.setstate(std::ios::failbit);
    }
    return is;
}

std::ostream& operator<<(std::ostream& os, const Backend& backend) {
    switch (backend) {
    case Backend::IO_URING:
        os << "io_uring";
        break;
    case Backend::EPOLL_LIBAIO:
        os << "epoll_libaio";
        break;
    }
    return os;
}

std::istream& operator>>(std::istream& is, Backend& backend) {
    std::string value;
    is >> value;
    if (value == "io_uring") {
        backend = Backend::IO_URING;
    } else if (value == "epoll_libaio") {
        backend = Backend::EPOLL_LIBAIO;
    } else {
        is.setstate(std::ios::failbit);
    }
    return is;
}

void Config::parse(int argc, char** argv) {
    cli::Parser parser(argc, argv);
    parser.parse("--mode", mode);
    parser.parse("--backend", backend, cli::Parser::optional);
    parser.parse("--ip", ip, cli::Parser::optional);
    parser.parse("--port", port, cli::Parser::optional);
    parser.parse("--setup_mode", setup_mode, cli::Parser::optional);
    parser.parse("--num_workers", num_workers, cli::Parser::optional);

    parser.parse("--reg_ring", reg_ring, cli::Parser::optional);
    parser.parse("--reg_fds", reg_fds, cli::Parser::optional);
    parser.parse("--reg_bufs", reg_bufs, cli::Parser::optional);
    parser.parse("--napi", napi, cli::Parser::optional);
    parser.parse("--send_zc", send_zc, cli::Parser::optional);
    parser.parse("--pin_queues", pin_queues, cli::Parser::optional);

    parser.parse("--ring_entries", ring_entries, cli::Parser::optional);
    parser.parse("--num_sockets", num_sockets, cli::Parser::optional);
    parser.parse("--scan_bytes", scan_bytes, cli::Parser::optional);
    parser.parse("--duration", duration, cli::Parser::optional);
    parser.parse("--request_size", request_size, cli::Parser::optional);
    parser.parse("--stripe_size", stripe_size, cli::Parser::optional);
    parser.parse("--connect_retries", connect_retries, cli::Parser::optional);
    parser.parse("--socket_sndbuf", socket_sndbuf, cli::Parser::optional);
    parser.parse("--socket_rcvbuf", socket_rcvbuf, cli::Parser::optional);
    parser.parse("--ssds", ssds, cli::Parser::optional);
    parser.check_unparsed();

    if (ring_entries == 0) {
        ring_entries = backend == Backend::IO_URING ? kDefaultIoUringRingEntries
                                                    : kDefaultEpollLibaioRingEntries;
    }

    ensure(num_sockets > 0);
    ensure(num_workers > 0);
    ensure(request_size > 0);
    ensure(stripe_size >= request_size);
    ensure(request_size % kBlockSize == 0);
    ensure(stripe_size % kBlockSize == 0);
    ensure(scan_bytes % request_size == 0);
    ensure(scan_bytes > 0 || duration > 0);
    if (mode == Mode::CLIENT) {
        ensure((scan_bytes > 0) != (duration > 0));
    }
    if (backend == Backend::IO_URING) {
        ensure(ring_entries >= 2 * num_sockets + 64);
    } else {
        ensure(ring_entries >= num_sockets);
    }

    if (mode == Mode::SERVER) {
        ensure(!ssds.empty());
    }
    if (send_zc) {
        ensure(mode == Mode::SERVER);
    }

    parser.print();
}

uint64_t total_requests(const Config& cfg) {
    ensure(cfg.scan_bytes > 0);
    return cfg.scan_bytes / cfg.request_size;
}

uint32_t worker_num_sockets(const Config& cfg, size_t worker_idx) {
    static_cast<void>(worker_idx);
    return cfg.num_sockets;
}

uint64_t worker_num_requests(const Config& cfg, size_t worker_idx) {
    if (!has_scan_target(cfg)) {
        static_cast<void>(worker_idx);
        return 0;
    }
    const uint64_t total = total_requests(cfg);
    const uint64_t base = total / cfg.num_workers;
    const uint64_t rem = total % cfg.num_workers;
    return base + (worker_idx < rem ? 1 : 0);
}

uint64_t worker_start_request(const Config& cfg, size_t worker_idx) {
    if (!has_scan_target(cfg)) {
        static_cast<void>(worker_idx);
        return 0;
    }
    uint64_t start = 0;
    for (size_t i = 0; i < worker_idx; ++i) {
        start += worker_num_requests(cfg, i);
    }
    return start;
}

uint64_t worker_start_offset(const Config& cfg, size_t worker_idx) {
    if (!has_scan_target(cfg)) {
        return static_cast<uint64_t>(worker_idx) * 10_GiB;
    }
    return worker_start_request(cfg, worker_idx) * cfg.request_size;
}

uint64_t worker_scan_bytes(const Config& cfg, size_t worker_idx) {
    return worker_num_requests(cfg, worker_idx) * cfg.request_size;
}

bool has_scan_target(const Config& cfg) {
    return cfg.scan_bytes > 0;
}

bool has_duration_target(const Config& cfg) {
    return cfg.duration > 0;
}

} // namespace remotescan
