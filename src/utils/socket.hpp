#pragma once

#include "utils/my_asserts.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

inline int listen_on(const char* ip, const uint16_t port, int backlog = 64) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    check_ret(fd);

    int opt = 1;
    check_ret(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    check_ret(bind(fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)));

    check_ret(::listen(fd, backlog));

    return fd;
}

inline int bind_udp(const char* ip, const uint16_t port) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    check_ret(fd);

    int opt = 1;
    check_ret(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    check_ret(inet_pton(AF_INET, ip, &serv_addr.sin_addr));
    serv_addr.sin_port = htons(port);

    check_ret(bind(fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)));

    return fd;
}

inline void throw_sys(const char* what) {
    throw std::runtime_error(std::string(what) + ": " + std::strerror(errno));
}

inline int connect_to(const char* ip, uint16_t port, int retries = 1,
                      __useconds_t sleep_us = 1'000'000) {
    if (!ip)
        throw std::invalid_argument("ip is null");
    if (retries < 1)
        retries = 1;

    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &sa.sin_addr) != 1)
        throw std::runtime_error("inet_pton failed");

    int last_errno = 0;

    for (int attempt = 1; attempt <= retries; ++attempt) {
        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            last_errno = errno;
            goto fail;
        }

        if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) == 0)
            return fd; // success

        last_errno = errno;
        ::close(fd);

    fail:
        if (attempt == retries)
            break;
        if (last_errno != EINTR && sleep_us > 0)
            ::usleep(sleep_us);
    }

    errno = last_errno ? last_errno : ECONNREFUSED;
    throw_sys("connect");
    return -1;
}

inline void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    check_ret(flags);
    flags |= O_NONBLOCK;
    check_ret(fcntl(fd, F_SETFL, flags));
}

inline void set_nodelay(int fd) {
    int32_t val = 1;
    check_ret(setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)));
}

inline void set_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFD);
    check_ret(flags);
    check_ret(fcntl(fd, F_SETFD, flags | FD_CLOEXEC));
}

inline void set_quickack(int fd) {
    int val = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &val, sizeof(val));
}
