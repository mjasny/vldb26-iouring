#include "remotescan/epoll_libaio.hpp"

#include "remotescan/common.hpp"
#include "remotescan/pinning.hpp"
#include "shuffle/utils.hpp"
#include "utils/cpu_map.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/socket.hpp"

#include <cerrno>
#include <cstring>
#include <netinet/tcp.h>
#include <sstream>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace remotescan {

namespace {
constexpr int kMaxEvents = 256;

void check_libaio(int res, const char* what) {
    if (res >= 0) {
        return;
    }
    std::stringstream ss;
    ss << what << " failed: " << std::strerror(-res) << " (" << -res << ")";
    throw std::runtime_error(ss.str());
}
}

EpollLibaioServer::Connection::Connection(uint32_t request_size)
    : payload(round_up(request_size, kAlignment)) {
}

EpollLibaioServer::Connection::~Connection() {
    if (fd >= 0) {
        close(fd);
    }
}

EpollLibaioServer::EpollLibaioServer(const Config& cfg, size_t worker_idx)
    : cfg(cfg),
      worker_idx(worker_idx),
      raid(cfg),
      port(cfg.port + static_cast<uint16_t>(worker_idx)),
      num_sockets(worker_num_sockets(cfg, worker_idx)),
      target_bytes(worker_scan_bytes(cfg, worker_idx)) {
}

EpollLibaioServer::~EpollLibaioServer() {
    if (aio_event_fd >= 0) {
        close(aio_event_fd);
    }
    if (epoll_fd >= 0) {
        close(epoll_fd);
    }
    if (listen_fd >= 0) {
        close(listen_fd);
    }
    if (aio_ctx) {
        io_destroy(aio_ctx);
    }
}

void EpollLibaioServer::init() {
    CPUMap::get().pin(pin_info.at(worker_idx % pin_info.size()).core_id);

    epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    check_ret(epoll_fd);

    aio_event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    check_ret(aio_event_fd);

    check_libaio(io_setup(static_cast<unsigned>(cfg.ring_entries), &aio_ctx), "io_setup");

    epoll_event aio_ev {};
    aio_ev.events = EPOLLIN;
    aio_ev.data.fd = aio_event_fd;
    check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, aio_event_fd, &aio_ev));

    listen_fd = listen_on(cfg.ip.c_str(), port, num_sockets);
    set_nonblocking(listen_fd);
    epoll_event listen_ev {};
    listen_ev.events = EPOLLIN;
    listen_ev.data.fd = listen_fd;
    check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &listen_ev));

    conns.reserve(num_sockets);
    for (uint32_t i = 0; i < num_sockets; ++i) {
        conns.push_back(std::make_unique<Connection>(cfg.request_size));
    }
}

void EpollLibaioServer::update_interest(Connection& conn, uint32_t events) {
    epoll_event ev {};
    ev.events = events | EPOLLONESHOT;
    ev.data.ptr = &conn;
    check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn.fd, &ev));
}

void EpollLibaioServer::close_conn(Connection& conn) {
    if (conn.closed) {
        return;
    }
    conn.closed = true;
    ++closed_conns;
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn.fd, nullptr);
    close(conn.fd);
    conn.fd = -1;
}

void EpollLibaioServer::handle_accept() {
    while (next_conn < num_sockets) {
        int fd = accept(listen_fd, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            check_ret(fd);
        }

        auto& conn = *conns[next_conn++];
        conn.fd = fd;
        set_nonblocking(fd);
        set_nodelay(fd);
        set_quickack(fd);
        set_socket_buffer(fd, SO_RCVBUF, cfg.socket_rcvbuf);
        set_socket_buffer(fd, SO_SNDBUF, cfg.socket_sndbuf);
        if (cfg.pin_queues) {
            pin_socket_queues(fd, worker_idx);
        }

        epoll_event ev {};
        ev.events = EPOLLIN | EPOLLONESHOT;
        ev.data.ptr = &conn;
        check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev));
    }
}

void EpollLibaioServer::handle_conn_read(Connection& conn) {
    auto* dst = reinterpret_cast<std::byte*>(&conn.request) + conn.req_bytes;
    const size_t left = sizeof(ScanRequest) - conn.req_bytes;
    ssize_t ret = recv(conn.fd, dst, left, 0);
    if (ret == 0) {
        close_conn(conn);
        return;
    }
    if (ret < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            update_interest(conn, EPOLLIN);
            return;
        }
        check_ret(ret);
    }

    conn.req_bytes += static_cast<size_t>(ret);
    if (conn.req_bytes < sizeof(ScanRequest)) {
        update_interest(conn, EPOLLIN);
        return;
    }

    ensure(conn.request.length > 0);
    ensure(conn.request.length <= cfg.request_size);
    if (has_scan_target(cfg)) {
        ensure(conn.request.offset + conn.request.length <= cfg.scan_bytes);
    }
    submit_read(conn);
}

void EpollLibaioServer::submit_read(Connection& conn) {
    const int fd = raid.fd_for_request(conn.request.offset, conn.request.length);
    const uint64_t offset = raid.offset_for_request(conn.request.offset, conn.request.length);

    io_prep_pread(&conn.cb, fd, conn.payload.data, conn.request.length, offset);
    conn.cb.data = &conn;
    io_set_eventfd(&conn.cb, aio_event_fd);

    iocb* cbs[1] = {&conn.cb};
    const int ret = io_submit(aio_ctx, 1, cbs);
    check_libaio(ret, "io_submit");
    ensure(ret == 1);
    conn.reading = true;
}

void EpollLibaioServer::handle_aio_completions() {
    uint64_t completed = 0;
    ssize_t ret = read(aio_event_fd, &completed, sizeof(completed));
    if (ret < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        check_ret(ret);
    }

    std::vector<io_event> events(completed ? completed : 1);
    timespec ts {};
    while (true) {
        int n = io_getevents(aio_ctx, 0, static_cast<long>(events.size()), events.data(), &ts);
        check_libaio(n, "io_getevents");
        if (n <= 0) {
            break;
        }
        for (int i = 0; i < n; ++i) {
            auto& ev = events[i];
            auto* conn = static_cast<Connection*>(ev.data);
            check_libaio(static_cast<int>(ev.res), "libaio pread completion");
            ensure(static_cast<uint32_t>(ev.res) == conn->request.length);
            conn->reading = false;
            conn->sending = true;
            conn->send_offset = 0;
            ssd_read_bytes += static_cast<uint32_t>(ev.res);
            update_interest(*conn, EPOLLOUT);
        }
    }
}

void EpollLibaioServer::reset_for_next_request(Connection& conn) {
    total_bytes += conn.request.length;
    conn.req_bytes = 0;
    conn.send_offset = 0;
    conn.sending = false;
    std::memset(&conn.request, 0, sizeof(conn.request));
    update_interest(conn, EPOLLIN);
}

void EpollLibaioServer::handle_conn_write(Connection& conn) {
    while (conn.send_offset < conn.request.length) {
        auto* ptr = conn.payload.as<std::byte>() + conn.send_offset;
        const uint32_t left = conn.request.length - conn.send_offset;
        ssize_t ret = send(conn.fd, ptr, left, 0);
        if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                update_interest(conn, EPOLLOUT);
                return;
            }
            check_ret(ret);
        }
        if (ret == 0) {
            close_conn(conn);
            return;
        }
        conn.send_offset += static_cast<uint32_t>(ret);
        send_bytes += static_cast<uint32_t>(ret);
    }
    reset_for_next_request(conn);
}

const WorkerStats& EpollLibaioServer::stats() const {
    return stats_;
}

void EpollLibaioServer::run() {
    init();
    start_tp = std::chrono::steady_clock::now();
    if (num_sockets == 0) {
        end_tp = start_tp;
        stats_.bytes = total_bytes;
        stats_.ssd_read_bytes = ssd_read_bytes;
        stats_.send_bytes = send_bytes;
        stats_.start_tp = start_tp;
        stats_.end_tp = end_tp;
        return;
    }

    std::array<epoll_event, kMaxEvents> events {};
    while ((target_bytes > 0 && total_bytes < target_bytes) || (target_bytes == 0 && closed_conns < num_sockets)) {
        int n = epoll_wait(epoll_fd, events.data(), static_cast<int>(events.size()), -1);
        check_ret(n);
        for (int i = 0; i < n; ++i) {
            auto& ev = events[i];
            if (ev.data.fd == listen_fd) {
                handle_accept();
                continue;
            }
            if (ev.data.fd == aio_event_fd) {
                handle_aio_completions();
                continue;
            }

            auto& conn = *static_cast<Connection*>(ev.data.ptr);
            if (ev.events & EPOLLIN) {
                handle_conn_read(conn);
            } else if (ev.events & EPOLLOUT) {
                handle_conn_write(conn);
            }
        }
    }
    end_tp = std::chrono::steady_clock::now();
    stats_.bytes = total_bytes;
    stats_.ssd_read_bytes = ssd_read_bytes;
    stats_.send_bytes = send_bytes;
    stats_.start_tp = start_tp;
    stats_.end_tp = end_tp;
}

EpollLibaioClient::Connection::Connection(uint32_t request_size)
    : payload(round_up(request_size, kAlignment)) {
}

EpollLibaioClient::Connection::~Connection() {
    if (fd >= 0) {
        close(fd);
    }
}

EpollLibaioClient::EpollLibaioClient(const Config& cfg, size_t worker_idx)
    : cfg(cfg),
      worker_idx(worker_idx),
      port(cfg.port + static_cast<uint16_t>(worker_idx)),
      num_sockets(worker_num_sockets(cfg, worker_idx)),
      next_offset(worker_start_offset(cfg, worker_idx)),
      end_offset(next_offset + worker_scan_bytes(cfg, worker_idx)) {
}

EpollLibaioClient::~EpollLibaioClient() {
    if (epoll_fd >= 0) {
        close(epoll_fd);
    }
}

void EpollLibaioClient::init() {
    CPUMap::get().pin(pin_info.at(worker_idx % pin_info.size()).core_id);
    epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    check_ret(epoll_fd);

    conns.reserve(num_sockets);
    for (uint32_t i = 0; i < num_sockets; ++i) {
        auto conn = std::make_unique<Connection>(cfg.request_size);
        conn->fd = connect_to(cfg.ip.c_str(), port, cfg.connect_retries);
        set_nonblocking(conn->fd);
        set_nodelay(conn->fd);
        set_quickack(conn->fd);
        set_socket_buffer(conn->fd, SO_RCVBUF, cfg.socket_rcvbuf);
        set_socket_buffer(conn->fd, SO_SNDBUF, cfg.socket_sndbuf);
        if (cfg.pin_queues) {
            pin_socket_queues(conn->fd, worker_idx);
        }
        assign_next_request(*conn);

        epoll_event ev {};
        ev.events = EPOLLOUT | EPOLLONESHOT;
        ev.data.ptr = conn.get();
        check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn->fd, &ev));
        conns.push_back(std::move(conn));
    }
}

bool EpollLibaioClient::assign_next_request(Connection& conn) {
    if (has_scan_target(cfg) && next_offset >= end_offset) {
        conn.finished = true;
        return false;
    }
    conn.request.offset = next_offset;
    conn.request.length = cfg.request_size;
    conn.request.reserved = 0;
    conn.send_offset = 0;
    conn.recv_offset = 0;
    conn.sending = true;
    conn.receiving = false;
    conn.finished = false;
    next_offset += cfg.request_size;
    return true;
}

void EpollLibaioClient::update_interest(Connection& conn, uint32_t events) {
    epoll_event ev {};
    ev.events = events | EPOLLONESHOT;
    ev.data.ptr = &conn;
    check_ret(epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn.fd, &ev));
}

bool EpollLibaioClient::should_issue_more(std::stop_token stop_token) const {
    if (has_duration_target(cfg)) {
        return !stop_token.stop_requested();
    }
    return total_bytes < (end_offset - worker_start_offset(cfg, worker_idx));
}

bool EpollLibaioClient::all_finished() const {
    for (const auto& conn : conns) {
        if (!conn->finished) {
            return false;
        }
    }
    return true;
}

void EpollLibaioClient::handle_conn_write(Connection& conn) {
    while (conn.send_offset < sizeof(ScanRequest)) {
        auto* ptr = reinterpret_cast<std::byte*>(&conn.request) + conn.send_offset;
        const size_t left = sizeof(ScanRequest) - conn.send_offset;
        ssize_t ret = send(conn.fd, ptr, left, 0);
        if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                update_interest(conn, EPOLLOUT);
                return;
            }
            check_ret(ret);
        }
        conn.send_offset += static_cast<size_t>(ret);
    }
    conn.sending = false;
    conn.receiving = true;
    update_interest(conn, EPOLLIN);
}

void EpollLibaioClient::handle_conn_read(Connection& conn, std::stop_token stop_token) {
    while (conn.recv_offset < conn.request.length) {
        auto* ptr = conn.payload.as<std::byte>() + conn.recv_offset;
        const uint32_t left = conn.request.length - conn.recv_offset;
        ssize_t ret = recv(conn.fd, ptr, left, 0);
        if (ret == 0) {
            throw std::runtime_error("server closed client socket");
        }
        if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                update_interest(conn, EPOLLIN);
                return;
            }
            check_ret(ret);
        }
        conn.recv_offset += static_cast<size_t>(ret);
        recv_bytes += static_cast<size_t>(ret);
    }

    total_bytes += conn.request.length;
    if (should_issue_more(stop_token) && assign_next_request(conn)) {
        update_interest(conn, EPOLLOUT);
    } else {
        conn.finished = true;
    }
}

const WorkerStats& EpollLibaioClient::stats() const {
    return stats_;
}

void EpollLibaioClient::run(std::stop_token stop_token) {
    init();
    start_tp = std::chrono::steady_clock::now();
    if (num_sockets == 0 || (has_scan_target(cfg) && next_offset == end_offset)) {
        end_tp = start_tp;
        stats_.bytes = total_bytes;
        stats_.recv_bytes = recv_bytes;
        stats_.start_tp = start_tp;
        stats_.end_tp = end_tp;
        return;
    }

    std::array<epoll_event, kMaxEvents> events {};
    while (!all_finished()) {
        int n = epoll_wait(epoll_fd, events.data(), static_cast<int>(events.size()), -1);
        check_ret(n);
        for (int i = 0; i < n; ++i) {
            auto& conn = *static_cast<Connection*>(events[i].data.ptr);
            if (events[i].events & EPOLLOUT) {
                handle_conn_write(conn);
            } else if (events[i].events & EPOLLIN) {
                handle_conn_read(conn, stop_token);
            }
        }
    }
    end_tp = std::chrono::steady_clock::now();
    stats_.bytes = total_bytes;
    stats_.recv_bytes = recv_bytes;
    stats_.start_tp = start_tp;
    stats_.end_tp = end_tp;
}

} // namespace remotescan
