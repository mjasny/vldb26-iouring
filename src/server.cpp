#include "cfg_net.hpp"
#include "shuffle/utils.hpp"
#include "utils/cpu_map.hpp"
#include "utils/my_logger.hpp"
#include "utils/socket.hpp"
#include "utils/types.hpp"

#include <cerrno>
#include <cstring>
#include <liburing.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <sys/socket.h>


int main(int argc, char** argv) {
    Config cfg;
    cfg.parse(argc, argv);


    if (cfg.core_id != -1) {
        CPUMap::get().pin(cfg.core_id);
    }

    if (cfg.tcp) {
        struct io_uring_probe* probe = io_uring_get_probe();
        check_ptr(probe);
        if (!io_uring_opcode_supported(probe, IORING_OP_LISTEN)) {
            Logger::error("IORING_OP_LISTEN not supported");
            return 1;
        }
    }


    struct io_uring ring;
    struct io_uring_params params;

    memset(&params, 0, sizeof(params));
    params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CLAMP;
    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = 131072;
    if (cfg.setup_mode == SetupMode::DEFER_TASKRUN) {
        params.flags |= IORING_SETUP_DEFER_TASKRUN;
    }
    if (cfg.setup_mode == SetupMode::SQPOLL) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 1000;
        if (cfg.core_id != -1) {
            params.sq_thread_cpu = cfg.core_id + 1;
            params.flags |= IORING_SETUP_SQ_AFF;
        }
    }
    if (cfg.setup_mode == SetupMode::COOP_TASKRUN) {
        params.flags |= IORING_SETUP_COOP_TASKRUN;
    }


    if (io_uring_queue_init_params(4096, &ring, &params) < 0) {
        fprintf(stderr, "%s\n", strerror(errno));
        exit(1);
    }

    if (cfg.reg_ring) {
        if (!(ring.features & IORING_FEAT_REG_REG_RING)) {
            Logger::error("IORING_FEAT_REG_REG_RING not supported");
            return 1;
        }
        ensure(io_uring_register_ring_fd(&ring) == 1);
        Logger::info("registered ring fd");
    }

    if (cfg.napi) {
        struct io_uring_napi napi = {};
        napi.prefer_busy_poll = 1;
        napi.busy_poll_to = 50;
        check_iou(io_uring_register_napi(&ring, &napi));
        Logger::info("enabled napi");
    }


    if (cfg.reg_fds) {
        check_iou(io_uring_register_files_sparse(&ring, 8));
    }

    struct io_uring_sqe* sqe;
    struct io_uring_cqe* cqe;
    int ret, val, submitted;
    unsigned head;

    int server_fd = -1;
    if (cfg.tcp) {
        server_fd = listen_on(cfg.ip.c_str(), cfg.port);
    } else {
        server_fd = bind_udp(cfg.ip.c_str(), cfg.port);

        if (cfg.pin_queues) {
            assign_flow_to_rx_queue(server_fd, cfg.rx_queue);
        }
    }

    if (cfg.reg_fds) {
        check_iou(io_uring_register_files_update(&ring, /*off*/ 0, &server_fd, 1));
        server_fd = 0;
    }

    // receive
    int client_fd;
    if (cfg.tcp) {
        sqe = io_uring_get_sqe(&ring);
        // io_uring_prep_accept_direct(sqe, SRV_INDEX, NULL, NULL, 0, CONN_INDEX); // if index is specific, client_fd below is 0
        io_uring_prep_accept(sqe, server_fd, nullptr, nullptr, 0);
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }

        io_uring_submit(&ring);
        io_uring_wait_cqe(&ring, &cqe);
        check_iou(cqe->res);
        client_fd = cqe->res;
        Logger::info("client fd: ", client_fd);
        io_uring_cqe_seen(&ring, cqe);


        if (cfg.pin_queues) {
            assign_flow_to_rx_queue(client_fd, cfg.rx_queue);
        }

        if (cfg.reg_fds) {
            check_iou(io_uring_register_files_update(&ring, /*off*/ 1, &client_fd, 1));
            client_fd = 1;
        }


        // set no-delay
        sqe = io_uring_get_sqe(&ring);
        val = 1;
        io_uring_prep_cmd_sock(sqe, SOCKET_URING_OP_SETSOCKOPT, client_fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        io_uring_sqe_set_data64(sqe, 143);
        io_uring_submit(&ring);
        io_uring_wait_cqe(&ring, &cqe);
        check_iou(cqe->res);
    }


    // constexpr size_t MAX_CLIENTS = 8;
    // struct Client {
    //     int fd = -1;
    //     uint64_t bytes_recv = 0;
    //     uint64_t bytes_send = 0;
    //     uint8_t buf[65536];

    //    // SKIP CQE Request
    //};

    // size_t clients = 0;
    // auto client = std::make_unique<Client[]>(MAX_CLIENTS);

    uint8_t buf[65536];
    ensure(cfg.ping_size <= sizeof(buf));
    if (cfg.reg_bufs) {
        struct iovec iov[1];
        for (uint32_t i = 0; i < 1; ++i) {
            iov[i].iov_base = &buf[i];
            iov[i].iov_len = sizeof(buf);
        }
        check_iou(io_uring_register_buffers(&ring, iov, 1));
    }
    uint32_t buf_idx = 0;


    bool done = false;
    size_t total_recv = 0;

    bool pingpong = true;


    struct msghdr msg;
    struct sockaddr_in sender_addr;
    struct iovec iov[1];
    if (cfg.tcp) {
        // initial recv
        sqe = io_uring_get_sqe(&ring);
        io_uring_prep_recv(sqe, client_fd, buf, cfg.ping_size, MSG_WAITALL);
        io_uring_sqe_set_data64(sqe, 1);
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
    } else {
        sqe = io_uring_get_sqe(&ring);
        io_uring_prep_recvmsg(sqe, server_fd, &msg, 0);
        io_uring_sqe_set_data64(sqe, 1);
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }

        for (uint32_t i = 0; i < 1; ++i) {
            iov[i].iov_base = &buf[i];
            iov[i].iov_len = cfg.ping_size;
        }

        memset(&msg, 0, sizeof(msg));
        msg.msg_name = &sender_addr;
        msg.msg_namelen = sizeof(struct sockaddr_in);
        msg.msg_iov = iov;
        msg.msg_iovlen = 1;
    }

    while (true) {
        if (cfg.setup_mode == SetupMode::SQPOLL) {
            io_uring_submit(&ring);
        } else {
            io_uring_submit_and_wait(&ring, 1);
        }

        bool done = false;
        int i = 0;
        io_uring_for_each_cqe(&ring, head, cqe) {
            if (cqe->res < 0) {
                fprintf(stderr, "Server startup failed. step %d got %d \n", head, ret);
                Logger::error("CQE Userdata: ", io_uring_cqe_get_data64(cqe));
                Logger::error("CQE Error: ", std::strerror(-cqe->res));
                // if (cqe->res != -ECONNRESET) {
                //     return 1;
                // }
                return 0;
            }
            auto user_data = io_uring_cqe_get_data64(cqe);

            if (user_data == 1) {
                if (cqe->res == 0) {
                    done = true;
                }
                if (cfg.tcp) {
                    if (cfg.pingpong) {
                        auto sqe = io_uring_get_sqe(&ring);
                        if (cfg.reg_bufs) {
                            io_uring_prep_send_zc_fixed(sqe, client_fd, buf, cfg.ping_size, MSG_WAITALL, 0, buf_idx);
                        } else {
                            io_uring_prep_send(sqe, client_fd, buf, cfg.ping_size, MSG_WAITALL);
                        }
                        io_uring_sqe_set_data64(sqe, 2);
                        if (cfg.reg_fds) {
                            sqe->flags |= IOSQE_FIXED_FILE;
                        }
                        if (cfg.poll_first) {
                            sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
                        }
                    }

                    // new recv
                    auto sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_recv(sqe, client_fd, buf, cfg.ping_size, MSG_WAITALL);
                    io_uring_sqe_set_data64(sqe, 1);
                    if (cfg.reg_fds) {
                        sqe->flags |= IOSQE_FIXED_FILE;
                    }
                    if (cfg.poll_first) {
                        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
                    }
                } else {
                    // previously recv was before send
                    if (cfg.pingpong) {
                        auto sqe = io_uring_get_sqe(&ring);
                        if (cfg.reg_bufs) {
                            io_uring_prep_sendmsg_zc(sqe, server_fd, &msg, 0);
                            // sqe->ioprio |= IORING_RECVSEND_FIXED_BUF;
                            // sqe->buf_index = buf_idx;
                        } else {
                            io_uring_prep_sendmsg(sqe, server_fd, &msg, 0);
                        }
                        io_uring_sqe_set_data64(sqe, 2);
                        if (cfg.reg_fds) {
                            sqe->flags |= IOSQE_FIXED_FILE;
                        }
                    }

                    auto sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_recvmsg(sqe, server_fd, &msg, 0);
                    io_uring_sqe_set_data64(sqe, 1);
                    if (cfg.reg_fds) {
                        sqe->flags |= IOSQE_FIXED_FILE;
                        // sqe->flags |= IOSQE_IO_LINK;
                    }
                }
            }

            ++i;
            ++total_recv;
        }
        io_uring_cq_advance(&ring, i);

        if (done) {
            break;
        }
    }

    Logger::info("total_recv=", total_recv);


    io_uring_queue_exit(&ring);

    Logger::info("Exit");
}
