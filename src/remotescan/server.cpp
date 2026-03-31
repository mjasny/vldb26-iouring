#include "remotescan/server.hpp"

#include "remotescan/pinning.hpp"
#include "shuffle/utils.hpp"
#include "utils/cpu_map.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/socket.hpp"

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netinet/tcp.h>
#include <sstream>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace remotescan {

ServerConnection::ServerConnection(uint32_t request_size)
    : payload(round_up(request_size, kAlignment)) {
}

ServerConnection::~ServerConnection() {
    if (raw_fd >= 0) {
        close(raw_fd);
    }
}

ServerWorker::ServerWorker(const Config& cfg, size_t worker_idx)
    : WorkerBase(cfg, worker_idx),
      raid(cfg),
      port(cfg.port + static_cast<uint16_t>(worker_idx)),
      num_sockets(worker_num_sockets(cfg, worker_idx)),
      target_bytes(worker_scan_bytes(cfg, worker_idx)) {
}

ServerWorker::~ServerWorker() {
    if (listen_fd >= 0) {
        close(listen_fd);
    }
}

void ServerWorker::run() {
    CPUMap::get().pin(worker_pin().core_id);
    init_ring();
    init_server();
    event_loop();
    stats_.bytes = total_bytes;
    stats_.ssd_read_bytes = ssd_read_bytes;
    stats_.send_bytes = send_bytes;
    stats_.start_tp = start_tp;
    stats_.end_tp = end_tp;
}

const WorkerStats& ServerWorker::stats() const {
    return stats_;
}

void ServerWorker::prep_accept() {
    auto* sqe = io_uring_get_sqe(&reg_ring.ring);
    ensure(sqe != nullptr);
    io_uring_prep_multishot_accept(sqe, cfg.reg_fds ? static_cast<int>(listen_slot) : listen_fd, nullptr, nullptr, 0);
    if (cfg.reg_fds) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data64(sqe, encode_user_data(OpKind::ACCEPT, 0));
}

void ServerWorker::init_server() {
    if (num_sockets == 0) {
        start_tp = std::chrono::steady_clock::now();
        end_tp = start_tp;
        return;
    }
    conn_fixed_base = ssd_fixed_base + raid.ssds.size();
    const uint32_t total_fixed = conn_fixed_base + num_sockets;
    if (cfg.reg_fds) {
        check_iou(io_uring_register_files_sparse(&reg_ring.ring, total_fixed));
    }

    conns.reserve(num_sockets);
    if (cfg.reg_bufs) {
        std::vector<iovec> iov(num_sockets);
        for (uint32_t i = 0; i < num_sockets; ++i) {
            auto conn = std::make_unique<ServerConnection>(cfg.request_size);
            conn->payload_buf_index = i;
            iov[i].iov_base = conn->payload.data;
            iov[i].iov_len = conn->payload.size;
            conns.push_back(std::move(conn));
        }
        check_iou(io_uring_register_buffers(&reg_ring.ring, iov.data(), iov.size()));
    } else {
        for (uint32_t i = 0; i < num_sockets; ++i) {
            auto conn = std::make_unique<ServerConnection>(cfg.request_size);
            conn->payload_buf_index = i;
            conns.push_back(std::move(conn));
        }
    }

    listen_fd = listen_on(cfg.ip.c_str(), port, num_sockets);
    set_socket_buffer(listen_fd, SO_RCVBUF, cfg.socket_rcvbuf);
    set_socket_buffer(listen_fd, SO_SNDBUF, cfg.socket_sndbuf);

    if (cfg.reg_fds) {
        int fd = listen_fd;
        check_iou(io_uring_register_files_update(&reg_ring.ring, listen_slot, &fd, 1));
        raid.register_files(reg_ring.ring, ssd_fixed_base);
    }

    prep_accept();
    check_iou(io_uring_submit(&reg_ring.ring));

    while (accepted < num_sockets) {
        io_uring_cqe* cqe = nullptr;
        check_iou(io_uring_wait_cqe(&reg_ring.ring, &cqe));
        ensure(cqe != nullptr);
        ensure(decode_kind(cqe->user_data) == OpKind::ACCEPT);
        const bool rearm_accept = !(cqe->flags & IORING_CQE_F_MORE);
        check_iou(cqe->res);

        ServerConnection& conn = *conns[accepted];
        conn.raw_fd = cqe->res;
        conn.fd_slot = conn_fixed_base + accepted;
        conn.ring_fd = cfg.reg_fds ? static_cast<int>(conn.fd_slot) : conn.raw_fd;
        set_nodelay(conn.raw_fd);
        set_quickack(conn.raw_fd);
        set_socket_buffer(conn.raw_fd, SO_RCVBUF, cfg.socket_rcvbuf);
        set_socket_buffer(conn.raw_fd, SO_SNDBUF, cfg.socket_sndbuf);
        if (cfg.pin_queues) {
            pin_socket_queues(conn.raw_fd, worker_idx);
        }
        if (cfg.reg_fds) {
            int fd = conn.raw_fd;
            check_iou(io_uring_register_files_update(&reg_ring.ring, conn.fd_slot, &fd, 1));
        }

        ++accepted;
        io_uring_cqe_seen(&reg_ring.ring, cqe);
        if (rearm_accept && accepted < num_sockets) {
            prep_accept();
            check_iou(io_uring_submit(&reg_ring.ring));
        }
    }

    for (uint32_t i = 0; i < num_sockets; ++i) {
        prep_recv_request(i);
    }
    check_iou(io_uring_submit(&reg_ring.ring));
    start_tp = std::chrono::steady_clock::now();
}

void ServerWorker::prep_recv_request(uint32_t idx) {
    ServerConnection& conn = *conns[idx];
    auto* sqe = io_uring_get_sqe(&reg_ring.ring);
    io_uring_prep_recv(sqe, conn.ring_fd, &conn.request, sizeof(conn.request), MSG_WAITALL);
    if (cfg.reg_fds) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    io_uring_sqe_set_data64(sqe, encode_user_data(OpKind::RECV_REQ, idx));
    conn.recv_posted = true;
}

void ServerWorker::prep_read(uint32_t idx) {
    ServerConnection& conn = *conns[idx];
    ensure(conn.request.length <= cfg.request_size);
    const int fd = raid.fd_for_request(conn.request.offset, conn.request.length);
    const uint64_t offset = raid.offset_for_request(conn.request.offset, conn.request.length);

    auto* sqe = io_uring_get_sqe(&reg_ring.ring);
    if (cfg.reg_bufs) {
        io_uring_prep_read_fixed(sqe, fd, conn.payload.data, conn.request.length, offset, conn.payload_buf_index);
    } else {
        io_uring_prep_read(sqe, fd, conn.payload.data, conn.request.length, offset);
    }
    if (cfg.reg_fds) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data64(sqe, encode_user_data(OpKind::READ, idx));
    conn.read_posted = true;
}

void ServerWorker::prep_send(uint32_t idx) {
    ServerConnection& conn = *conns[idx];
    const uint32_t remaining = conn.request.length - conn.bytes_sent;
    auto* sqe = io_uring_get_sqe(&reg_ring.ring);
    auto* ptr = conn.payload.as<std::byte>() + conn.bytes_sent;
    if (cfg.send_zc && conn.bytes_sent == 0) {
        if (cfg.reg_bufs) {
            io_uring_prep_send_zc_fixed(sqe, conn.ring_fd, ptr, remaining, 0, 0, conn.payload_buf_index);
        } else {
            io_uring_prep_send_zc(sqe, conn.ring_fd, ptr, remaining, 0, 0);
        }
    } else {
        io_uring_prep_send(sqe, conn.ring_fd, ptr, remaining, 0);
    }
    if (cfg.reg_fds) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    io_uring_sqe_set_data64(sqe, encode_user_data(OpKind::SEND, idx));
    conn.send_posted = true;
}

void ServerWorker::finish_request(ServerConnection& conn) {
    total_bytes += conn.request.length;
    conn.requests_done++;
    conn.bytes_sent = 0;
    conn.recv_posted = false;
    conn.read_posted = false;
    conn.send_posted = false;
    conn.waiting_for_zc_notif = false;
}

void ServerWorker::event_loop() {
    while ((target_bytes > 0 && total_bytes < target_bytes) || (target_bytes == 0 && closed < num_sockets)) {
        check_iou(io_uring_submit_and_wait(&reg_ring.ring, 1));

        io_uring_cqe* cqe = nullptr;
        int i = 0;
        unsigned head = 0;
        io_uring_for_each_cqe(&reg_ring.ring, head, cqe) {
            ++i;
            const auto kind = decode_kind(cqe->user_data);
            const auto idx = decode_index(cqe->user_data);
            ServerConnection& conn = *conns[idx];

            if (has_cqe_notification(cqe)) {
                conn.waiting_for_zc_notif = false;
                continue;
            }

            if (kind == OpKind::RECV_REQ && cqe->res == 0) {
                if (conn.raw_fd >= 0) {
                    close(conn.raw_fd);
                    conn.raw_fd = -1;
                    conn.ring_fd = -1;
                    ++closed;
                }
                conn.recv_posted = false;
                continue;
            }

            if (cqe->res <= 0) {
                std::stringstream ss;
                ss << "server completion failed kind=" << static_cast<int>(kind)
                   << " idx=" << idx
                   << " res=" << cqe->res
                   << " errno=" << strerror(-cqe->res);
                throw std::runtime_error(ss.str());
            }

            switch (kind) {
            case OpKind::RECV_REQ:
                ensure(static_cast<size_t>(cqe->res) == sizeof(ScanRequest));
                ensure(conn.request.length > 0);
                ensure(conn.request.length <= cfg.request_size);
                if (has_scan_target(cfg)) {
                    ensure(conn.request.offset + conn.request.length <= cfg.scan_bytes);
                }
                conn.recv_posted = false;
                prep_read(idx);
                break;
            case OpKind::READ:
                ensure(static_cast<uint32_t>(cqe->res) == conn.request.length);
                ssd_read_bytes += static_cast<uint32_t>(cqe->res);
                conn.read_posted = false;
                prep_send(idx);
                break;
            case OpKind::SEND:
                send_bytes += static_cast<uint32_t>(cqe->res);
                conn.bytes_sent += static_cast<uint32_t>(cqe->res);
                if (conn.bytes_sent < conn.request.length) {
                    prep_send(idx);
                } else {
                    finish_request(conn);
                    if ((target_bytes > 0 && total_bytes < target_bytes) || target_bytes == 0) {
                        prep_recv_request(idx);
                    }
                }
                break;
            case OpKind::ACCEPT:
            case OpKind::RECV_PAYLOAD:
            case OpKind::SEND_REQ:
                ensure(false);
                break;
            }
        }
        io_uring_cq_advance(&reg_ring.ring, i);
    }
    end_tp = std::chrono::steady_clock::now();
}

} // namespace remotescan
