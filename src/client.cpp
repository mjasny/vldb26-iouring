#include "cfg_net.hpp"
#include "shuffle/utils.hpp"
#include "utils/cpu_map.hpp"
#include "utils/literals.hpp"
#include "utils/my_logger.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/socket.hpp"
#include "utils/stats_printer.hpp"
#include "utils/stopper.hpp"
#include "utils/types.hpp"
#include "utils/utils.hpp"

#include <chrono>
#include <cstring>
#include <liburing.h>
#include <netinet/tcp.h>
#include <ratio>
#include <stdio.h>
#include <sys/socket.h>


int main(int argc, char** argv) {
    Config cfg;
    cfg.parse(argc, argv);


    if (cfg.core_id != -1) {
        CPUMap::get().pin(cfg.core_id);
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

    if (false) { // for testing
        auto fd = connect_to("127.0.0.1", 1234);
        Logger::info("connected");
        std::cin.get();
        for (int j = 0; j < 1; ++j) {
            auto depth = 1;
            for (int i = 0; i < depth; ++i) {
                auto sqe = io_uring_get_sqe(&ring);
                uint8_t buf[1];
                io_uring_prep_send(sqe, fd, buf, sizeof(buf), MSG_WAITALL);
                if (cfg.tcp) {
                    sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
                }
            }
            // io_uring_submit(&ring);
            RDTSCClock clock(2.4_GHz);
            clock.start();
            io_uring_submit_and_wait(&ring, depth);
            clock.stop();

            Logger::info("cycles=", clock.cycles());
            double seconds = clock.as<std::chrono::nanoseconds, double>();
            Logger::info("secs=", seconds);
        }

        return 0;
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

    Logger::info("Config: ", cfg.setup_mode);

    int ret, val, submitted;
    unsigned head;
    struct io_uring_cqe* cqe;

    if (cfg.reg_fds) {
        check_iou(io_uring_register_files_sparse(&ring, 8));
    }

    int fd = -1;

    if (cfg.tcp) {
        fd = connect_to(cfg.ip.c_str(), cfg.port);

        set_nodelay(fd);
        if (cfg.pin_queues) {
            assign_flow_to_rx_queue(fd, cfg.rx_queue);
        }
    } else {
        ensure(cfg.local_ip.size() > 0);
        fd = bind_udp(cfg.local_ip.c_str(), cfg.port);
        // fd = socket(AF_INET, SOCK_DGRAM, 0);
        check_ret(fd);


        if (cfg.pin_queues) {
            assign_flow_to_rx_queue(fd, cfg.rx_queue);
        }
    }

    if (cfg.reg_fds) {
        check_iou(io_uring_register_files_update(&ring, /*off*/ 0, &fd, 1));
        fd = 0;
    }


    // set no-delay
    struct io_uring_sqe* sqe;
    if (cfg.tcp) {
        auto sqe = io_uring_get_sqe(&ring);
        val = 1;
        io_uring_prep_cmd_sock(sqe, SOCKET_URING_OP_SETSOCKOPT, fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        sqe->flags |= IOSQE_IO_LINK;
        io_uring_sqe_set_data64(sqe, 143);
    }


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


    struct msghdr msg;
    struct msghdr send_msg;
    struct sockaddr_in recv_addr;
    struct sockaddr_in sender_addr;
    struct iovec iov[1];
    if (cfg.tcp) {
        // post send
        sqe = io_uring_get_sqe(&ring);
        if (cfg.reg_bufs) {
            io_uring_prep_send_zc_fixed(sqe, fd, buf, cfg.ping_size, MSG_WAITALL, 0, buf_idx);
        } else {
            io_uring_prep_send(sqe, fd, buf, cfg.ping_size, MSG_WAITALL);
        }
        io_uring_sqe_set_data64(sqe, 2);
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        sqe->flags |= IOSQE_IO_LINK;

        if (cfg.pingpong) {
            // post recv
            sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv(sqe, fd, buf, cfg.ping_size, MSG_WAITALL);
            io_uring_sqe_set_data64(sqe, 1);
            if (cfg.reg_fds) {
                sqe->flags |= IOSQE_FIXED_FILE;
            }
            sqe->flags |= IOSQE_IO_LINK;
        }
    } else {
        for (uint32_t i = 0; i < 1; ++i) {
            iov[i].iov_base = &buf[i];
            iov[i].iov_len = cfg.ping_size;
        }

        if (cfg.pingpong) {
            sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recvmsg(sqe, fd, &msg, 0);
            io_uring_sqe_set_data64(sqe, 1);
            if (cfg.reg_fds) {
                sqe->flags |= IOSQE_FIXED_FILE;
            }


            memset(&msg, 0, sizeof(msg));
            msg.msg_name = &recv_addr;
            msg.msg_namelen = sizeof(struct sockaddr_in);
            msg.msg_iov = iov;
            msg.msg_iovlen = 1;
        }

        sqe = io_uring_get_sqe(&ring);
        if (cfg.reg_bufs) {
            io_uring_prep_sendmsg_zc(sqe, fd, &send_msg, MSG_WAITALL);
            // sqe->ioprio |= IORING_RECVSEND_FIXED_BUF;
            // sqe->buf_index = buf_idx;
        } else {
            io_uring_prep_sendmsg(sqe, fd, &send_msg, MSG_WAITALL);
        }
        io_uring_sqe_set_data64(sqe, 2);
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }


        memset(&sender_addr, 0, sizeof(sender_addr));
        sender_addr.sin_family = AF_INET;
        sender_addr.sin_port = htons(cfg.port);
        inet_pton(AF_INET, cfg.ip.c_str(), &sender_addr.sin_addr);

        memset(&send_msg, 0, sizeof(send_msg));
        send_msg.msg_name = &sender_addr;
        send_msg.msg_namelen = sizeof(struct sockaddr_in);
        send_msg.msg_iov = iov;
        send_msg.msg_iovlen = 1;
    }

    TimedStopper stopper;
    stopper.after(std::chrono::milliseconds(cfg.duration));
    RDTSCClock clock(2.4_GHz);

    auto& stats = StatsPrinter::get();
    stats.interval = 100'000;
    stats.start();

    uint64_t latency = 0;
    uint64_t ops = 0;

    StatsPrinter::Scope stats_scope;
    stats.register_var(stats_scope, ops, "ops");
    stats.register_var(stats_scope, latency, "latency", false);

    RDTSCClock ping_clock(2.4_GHz);

    uint64_t warmup = 100;

    if (cfg.reg_bufs) {
        ensure(cfg.pingpong);
    }

    clock.start();
    while (stopper.can_run()) {
        if (cfg.setup_mode == SetupMode::SQPOLL) {
            io_uring_submit(&ring);
        } else {
            io_uring_submit_and_wait(&ring, 1);
        }

        uint64_t ud_action = cfg.pingpong ? 1 : 2;

        int i = 0;
        io_uring_for_each_cqe(&ring, head, cqe) {
            if (cqe->res < 0) {
                Logger::error("CQE Userdata: ", io_uring_cqe_get_data64(cqe));
                Logger::error("CQE Error: ", std::strerror(-cqe->res));
                return 1;
            }

            auto user_data = io_uring_cqe_get_data64(cqe);
            if (user_data == ud_action) {
                ++ops;


                if (cfg.tcp) {
                    // send out new
                    auto sqe = io_uring_get_sqe(&ring);
                    if (cfg.reg_bufs) {
                        io_uring_prep_send_zc_fixed(sqe, fd, buf, cfg.ping_size, MSG_WAITALL, 0, buf_idx);
                    } else {
                        io_uring_prep_send(sqe, fd, buf, cfg.ping_size, MSG_WAITALL);
                    }
                    io_uring_sqe_set_data64(sqe, 2);
                    if (cfg.reg_fds) {
                        sqe->flags |= IOSQE_FIXED_FILE;
                    }
                    if (cfg.poll_first) {
                        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
                    }

                    if (cfg.pingpong) {
                        // post recv
                        sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_recv(sqe, fd, buf, cfg.ping_size, MSG_WAITALL);
                        io_uring_sqe_set_data64(sqe, 1);
                        if (cfg.reg_fds) {
                            sqe->flags |= IOSQE_FIXED_FILE;
                        }
                        if (cfg.poll_first) {
                            sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
                        }
                    }
                } else {
                    auto sqe = io_uring_get_sqe(&ring);
                    if (cfg.reg_bufs) {
                        io_uring_prep_sendmsg_zc(sqe, fd, &send_msg, MSG_WAITALL);
                        // sqe->ioprio |= IORING_RECVSEND_FIXED_BUF;
                        // sqe->buf_index = buf_idx;
                    } else {
                        io_uring_prep_sendmsg(sqe, fd, &send_msg, MSG_WAITALL);
                    }

                    io_uring_sqe_set_data64(sqe, 2);
                    if (cfg.reg_fds) {
                        sqe->flags |= IOSQE_FIXED_FILE;
                    }

                    if (cfg.pingpong) {
                        sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_recvmsg(sqe, fd, &msg, 0);
                        io_uring_sqe_set_data64(sqe, 1);
                        if (cfg.reg_fds) {
                            sqe->flags |= IOSQE_FIXED_FILE;
                        }
                    }
                }

                if (cfg.pingpong && ops > warmup) {
                    ping_clock.stop();
                    latency = ping_clock.as<std::chrono::nanoseconds, uint64_t>();
                    ping_clock.start();
                }
            } else if (user_data == 2) {
                // sends and send_zc
            } else {
                Logger::info("user_data ", user_data);
            }
            ++i;
        }
        io_uring_cq_advance(&ring, i);
    }

    clock.stop();
    stats.stop();

    Logger::info("cycles=", clock.cycles());
    double seconds = clock.as<std::chrono::microseconds, double>() / 1e6;
    Logger::info("secs=", seconds);
    Logger::info("ops=", ops);
    Logger::info("ops_per_sec=", static_cast<double>(ops) / seconds);

    if (!cfg.tcp) {
        Logger::info("Sending empty UDP to terminate server");
        iov[0].iov_len = 0;

        auto sqe = io_uring_get_sqe(&ring);
        io_uring_prep_sendmsg(sqe, fd, &send_msg, MSG_WAITALL);
        io_uring_sqe_set_data64(sqe, 2);
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        io_uring_submit_and_wait(&ring, 1);
    }


    io_uring_queue_exit(&ring);
}
