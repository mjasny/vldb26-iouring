#include "remotescan/client.hpp"

#include "remotescan/pinning.hpp"
#include "shuffle/utils.hpp"
#include "utils/cpu_map.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/socket.hpp"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <netinet/tcp.h>
#include <sstream>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace remotescan {

ClientConnection::ClientConnection(uint32_t request_size)
    : payload(round_up(request_size, kAlignment)) {
}

ClientConnection::~ClientConnection() {
    if (raw_fd >= 0) {
        close(raw_fd);
    }
}

ClientWorker::ClientWorker(const Config& cfg, size_t worker_idx)
    : WorkerBase(cfg, worker_idx),
      port(cfg.port + static_cast<uint16_t>(worker_idx)),
      num_sockets(worker_num_sockets(cfg, worker_idx)),
      start_offset(worker_start_offset(cfg, worker_idx)),
      end_offset(start_offset + worker_scan_bytes(cfg, worker_idx)),
      next_offset(start_offset) {
}

void ClientWorker::run(std::stop_token stop_token) {
    CPUMap::get().pin(worker_pin().core_id);
    init_ring();
    init_client();
    event_loop(stop_token);
    stats_.bytes = total_bytes;
    stats_.recv_bytes = recv_bytes;
    stats_.start_tp = start_tp;
    stats_.end_tp = end_tp;
}

const WorkerStats& ClientWorker::stats() const {
    return stats_;
}

void ClientWorker::init_client() {
    conns.reserve(num_sockets);
    if (cfg.reg_bufs) {
        std::vector<iovec> iov(num_sockets);
        for (uint32_t i = 0; i < num_sockets; ++i) {
            auto conn = std::make_unique<ClientConnection>(cfg.request_size);
            conn->payload_buf_index = i;
            iov[i].iov_base = conn->payload.data;
            iov[i].iov_len = conn->payload.size;
            conns.push_back(std::move(conn));
        }
        check_iou(io_uring_register_buffers(&reg_ring.ring, iov.data(), iov.size()));
    } else {
        for (uint32_t i = 0; i < num_sockets; ++i) {
            auto conn = std::make_unique<ClientConnection>(cfg.request_size);
            conn->payload_buf_index = i;
            conns.push_back(std::move(conn));
        }
    }

    if (cfg.reg_fds) {
        check_iou(io_uring_register_files_sparse(&reg_ring.ring, num_sockets));
    }

    for (uint32_t i = 0; i < num_sockets; ++i) {
        ClientConnection& conn = *conns[i];
        conn.raw_fd = connect_to(cfg.ip.c_str(), port, cfg.connect_retries);
        conn.fd_slot = i;
        conn.ring_fd = cfg.reg_fds ? static_cast<int>(i) : conn.raw_fd;
        set_nodelay(conn.raw_fd);
        set_quickack(conn.raw_fd);
        set_socket_buffer(conn.raw_fd, SO_RCVBUF, cfg.socket_rcvbuf);
        set_socket_buffer(conn.raw_fd, SO_SNDBUF, cfg.socket_sndbuf);
        if (cfg.pin_queues) {
            pin_socket_queues(conn.raw_fd, worker_idx);
        }
        if (cfg.reg_fds) {
            int fd = conn.raw_fd;
            check_iou(io_uring_register_files_update(&reg_ring.ring, i, &fd, 1));
        }
    }

    for (uint32_t i = 0; i < num_sockets; ++i) {
        if (!assign_next_request(*conns[i])) {
            break;
        }
        prep_send_request(i);
    }
    check_iou(io_uring_submit(&reg_ring.ring));
    start_tp = std::chrono::steady_clock::now();
}

bool ClientWorker::assign_next_request(ClientConnection& conn) {
    if (has_scan_target(cfg) && next_offset >= end_offset) {
        conn.finished = true;
        return false;
    }
    conn.request.offset = next_offset;
    conn.request.length = cfg.request_size;
    conn.request.reserved = 0;
    next_offset += cfg.request_size;
    conn.finished = false;
    return true;
}

void ClientWorker::prep_send_request(uint32_t idx) {
    ClientConnection& conn = *conns[idx];
    auto* sqe = io_uring_get_sqe(&reg_ring.ring);
    io_uring_prep_send(sqe, conn.ring_fd, &conn.request, sizeof(conn.request), 0);
    if (cfg.reg_fds) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    io_uring_sqe_set_data64(sqe, encode_user_data(OpKind::SEND_REQ, idx));
    conn.send_posted = true;
}

void ClientWorker::prep_recv_payload(uint32_t idx) {
    ClientConnection& conn = *conns[idx];
    auto* sqe = io_uring_get_sqe(&reg_ring.ring);
    io_uring_prep_recv(sqe, conn.ring_fd, conn.payload.data, conn.request.length, MSG_WAITALL);
    if (cfg.reg_fds) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    io_uring_sqe_set_data64(sqe, encode_user_data(OpKind::RECV_PAYLOAD, idx));
    conn.recv_posted = true;
}

bool ClientWorker::should_issue_more(std::stop_token stop_token) const {
    if (has_duration_target(cfg)) {
        return !stop_token.stop_requested();
    }
    return total_bytes < (end_offset - start_offset);
}

bool ClientWorker::all_finished() const {
    for (const auto& conn : conns) {
        if (!conn->finished) {
            return false;
        }
    }
    return true;
}

void ClientWorker::event_loop(std::stop_token stop_token) {
    while (!all_finished()) {
        check_iou(io_uring_submit_and_wait(&reg_ring.ring, 1));

        io_uring_cqe* cqe = nullptr;
        int i = 0;
        unsigned head = 0;
        io_uring_for_each_cqe(&reg_ring.ring, head, cqe) {
            ++i;
            const auto kind = decode_kind(cqe->user_data);
            const auto idx = decode_index(cqe->user_data);
            ClientConnection& conn = *conns[idx];

            if (cqe->res <= 0) {
                std::stringstream ss;
                ss << "client completion failed kind=" << static_cast<int>(kind)
                   << " idx=" << idx
                   << " res=" << cqe->res
                   << " errno=" << strerror(-cqe->res);
                throw std::runtime_error(ss.str());
            }

            switch (kind) {
            case OpKind::SEND_REQ:
                ensure(static_cast<size_t>(cqe->res) == sizeof(ScanRequest));
                conn.send_posted = false;
                prep_recv_payload(idx);
                break;
            case OpKind::RECV_PAYLOAD:
                ensure(static_cast<uint32_t>(cqe->res) == conn.request.length);
                conn.recv_posted = false;
                recv_bytes += static_cast<uint32_t>(cqe->res);
                conn.bytes_recv += conn.request.length;
                conn.requests_done++;
                total_bytes += conn.request.length;
                if (should_issue_more(stop_token) && assign_next_request(conn)) {
                    prep_send_request(idx);
                } else {
                    conn.finished = true;
                }
                break;
            case OpKind::ACCEPT:
            case OpKind::RECV_REQ:
            case OpKind::READ:
            case OpKind::SEND:
                ensure(false);
                break;
            }
        }
        io_uring_cq_advance(&reg_ring.ring, i);
    }
    end_tp = std::chrono::steady_clock::now();
}

} // namespace remotescan
