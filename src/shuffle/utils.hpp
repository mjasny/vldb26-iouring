#pragma once

#include "utils/my_asserts.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <ifaddrs.h>
#include <iostream>
#include <net/if.h>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

int runCommand(const std::string& cmd, std::string* output = nullptr);

bool get_ip_port(int fd, bool peer,
                 int& family, std::string& ip, uint16_t& port) {
    sockaddr_storage ss{};
    socklen_t slen = sizeof(ss);
    int rc = peer
                 ? getpeername(fd, reinterpret_cast<sockaddr*>(&ss), &slen)
                 : getsockname(fd, reinterpret_cast<sockaddr*>(&ss), &slen);
    if (rc != 0)
        return false;

    char buf[INET6_ADDRSTRLEN] = {0};
    if (ss.ss_family == AF_INET) {
        auto* sa = reinterpret_cast<sockaddr_in*>(&ss);
        if (!inet_ntop(AF_INET, &sa->sin_addr, buf, sizeof(buf)))
            return false;
        ip = buf;
        port = ntohs(sa->sin_port);
        family = AF_INET;
        return true;
    } else if (ss.ss_family == AF_INET6) {
        auto* sa6 = reinterpret_cast<sockaddr_in6*>(&ss);
        if (!inet_ntop(AF_INET6, &sa6->sin6_addr, buf, sizeof(buf)))
            return false;
        ip = buf;
        port = ntohs(sa6->sin6_port);
        family = AF_INET6;
        return true;
    }
    errno = EAFNOSUPPORT;
    return false;
}


static bool sockaddr_equal(const sockaddr* a, const sockaddr* b) {
    if (!a || !b)
        return false;
    if (a->sa_family != b->sa_family)
        return false;

    if (a->sa_family == AF_INET) {
        auto* ia = reinterpret_cast<const sockaddr_in*>(a);
        auto* ib = reinterpret_cast<const sockaddr_in*>(b);
        return ia->sin_addr.s_addr == ib->sin_addr.s_addr;
    } else if (a->sa_family == AF_INET6) {
        auto* ia = reinterpret_cast<const sockaddr_in6*>(a);
        auto* ib = reinterpret_cast<const sockaddr_in6*>(b);
        // Compare address bytes
        if (std::memcmp(&ia->sin6_addr, &ib->sin6_addr, sizeof(in6_addr)) != 0)
            return false;
        // For link-local, scope must also match
        if (IN6_IS_ADDR_LINKLOCAL(&ia->sin6_addr))
            return ia->sin6_scope_id == ib->sin6_scope_id;
        return true;
    }
    return false;
}

std::string get_iface_name_from_fd(int fd) {
    // 1) If the socket was bound to a device, SO_BINDTODEVICE returns its name
    {
        char buf[IFNAMSIZ] = {0};
        socklen_t len = sizeof(buf);
        if (getsockopt(fd, SOL_SOCKET, SO_BINDTODEVICE, buf, &len) == 0) {
            if (len > 0 && buf[0] != '\0') {
                return std::string(buf);
            }
        } else if (errno != ENOPROTOOPT && errno != EOPNOTSUPP) {
            throw std::runtime_error(std::string("SO_BINDTODEVICE failed: ") + std::strerror(errno));
        }
    }

    // 2) Fall back to: local address -> interface via getifaddrs()
    sockaddr_storage local_ss{};
    socklen_t slen = sizeof(local_ss);
    if (getsockname(fd, reinterpret_cast<sockaddr*>(&local_ss), &slen) != 0) {
        throw std::runtime_error(std::string("getsockname failed: ") + std::strerror(errno));
    }

    ifaddrs* ifs = nullptr;
    if (getifaddrs(&ifs) != 0) {
        throw std::runtime_error(std::string("getifaddrs failed: ") + std::strerror(errno));
    }

    std::string found;
    for (auto* p = ifs; p; p = p->ifa_next) {
        if (!p->ifa_addr)
            continue;

        // Match family first
        if (p->ifa_addr->sa_family != local_ss.ss_family)
            continue;

        // Compare addresses (and scope id if link-local v6)
        if (sockaddr_equal(p->ifa_addr, reinterpret_cast<sockaddr*>(&local_ss))) {
            // Optional: ensure the interface is up / running
            // if (!(p->ifa_flags & IFF_UP)) continue;
            found = p->ifa_name;
            break;
        }
    }
    freeifaddrs(ifs);

    if (found.empty()) {
        // Couldn’t map the local address to an interface (uncommon, but possible with VRFs/policy routing).
        // If you need the *egress* interface chosen by routing to the peer, you’d query rtnetlink (RTM_GETROUTE) with the peer addr.
        throw std::runtime_error("could not determine interface for socket's local address");
    }

    return found;
}


static inline bool is_wildcard_addr(const std::string& ip, int af) {
    if (af == AF_INET)
        return (ip == "0.0.0.0");
    if (af == AF_INET6)
        return (ip == "::");
    return false;
}


void assign_flow_to_rx_queue(int fd, int nic_queue) {
    int fam_local = 0;
    std::string lip;
    uint16_t lport = 0;

    // Figure out the socket type to choose TCP vs UDP in ethtool
    int socktype = 0;
    socklen_t optlen = sizeof(socktype);
    check_ret(getsockopt(fd, SOL_SOCKET, SO_TYPE, &socktype, &optlen));
    const bool is_udp = (socktype == SOCK_DGRAM);

    // Local (listening/bound) side
    if (!get_ip_port(fd, /*peer=*/false, fam_local, lip, lport)) {
        std::cerr << "getsockname failed: " << std::strerror(errno) << "\n";
        return;
    }

    // Try to get peer; for listening TCP or unconnected UDP this returns ENOTCONN.
    int fam_peer = fam_local; // default to same AF
    std::string rip;
    uint16_t rport = 0;
    bool have_peer = true;
    if (!get_ip_port(fd, /*peer=*/true, fam_peer, rip, rport)) {
        if (errno == ENOTCONN) {
            have_peer = false; // expected for listening/unconnected sockets
        } else {
            std::cerr << "getpeername failed: " << std::strerror(errno) << "\n";
            return;
        }
    }

    if (have_peer && fam_local != fam_peer) {
        std::cerr << "Address family mismatch (weird): local AF=" << fam_local
                  << " peer AF=" << fam_peer << "\n";
        return;
    }

    const auto iface = get_iface_name_from_fd(fd);
    const bool v6 = (fam_local == AF_INET6);

    // Ensure ntuple is enabled (needed for rx flow steering)
    {
        std::stringstream ss;
        ss << "sudo ethtool -K " << iface << " ntuple on";
        std::cout << ss.str() << "\n";
        ensure(runCommand(ss.str()) == 0);
    }

    // Build the ethtool rule:
    // For RX steering we match packets as they ARRIVE to us:
    //   src = remote, dst = local.
    // If we don't have a peer (listening/unconnected), we omit src-* and match on dst only.
    // If bound to ANY (0.0.0.0/::), omit dst-ip and match on dst-port only.
    std::stringstream ss;
    ss << "sudo ethtool -N " << iface
       << " flow-type " << (is_udp ? (v6 ? "udp6" : "udp4") : (v6 ? "tcp6" : "tcp4"));

    if (have_peer) {
        ss << " src-ip " << rip
           << " src-port " << rport;
    }

    // Only include dst-ip if not bound to wildcard
    if (!is_wildcard_addr(lip, fam_local)) {
        ss << " dst-ip " << lip;
    }
    ss << " dst-port " << lport
       << " action " << nic_queue;

    std::cout << ss.str() << "\n";
    ensure(runCommand(ss.str()) == 0);
}


int runCommand(const std::string& cmd, std::string* output) {
    FILE* pipe = popen(cmd.c_str(), "r");
    check_ptr(pipe);

    char buffer[256];
    while (fgets(buffer, sizeof(buffer), pipe)) {
        if (output) {
            *output += buffer;
        }
        std::cout << buffer;
    }

    int status = pclose(pipe);
    check_ret(status);

    if (WIFEXITED(status)) {
        return WEXITSTATUS(status); // actual exit code
    } else {
        return -1; // abnormal termination
    }
}
