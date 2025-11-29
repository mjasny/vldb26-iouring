#include "cpu_map.hpp"

#include "utils/my_asserts.hpp"

#include <iostream>
#include <map>
#include <numa.h>
#include <sstream>
#include <string>
#include <unistd.h> // sysconf
#include <vector>


CPUMap::CPUMap() {
    ensure(!(numa_available() < 0), "no numa");

    int maxnode = numa_num_configured_nodes() - 1;
    struct bitmask* cpus = numa_allocate_cpumask();

    for (int i = 0; i <= maxnode; i++) {
        ensure(numa_node_to_cpus(i, cpus) == 0, [&] {
            std::stringstream ss;
            ss << "node " << i << " failed to convert: " << std::strerror(errno);
            return ss.str();
        });

        auto& v = cores[i];

        for (uint32_t k = 0; k < cpus->size; k++) {
            if (numa_bitmask_isbitset(cpus, k)) {
                v.emplace_back(k);
                ++total_cores;
            }
        }
    }

    numa_free_cpumask(cpus);
}

void CPUMap::print() {
    std::stringstream ss;
    ss << "CPUMap:\n";
    for (auto& [node, cores] : cores) {
        ss << "node " << node << " cpus:";
        for (auto& c : cores) {
            ss << " " << c;
        }
        ss << "\n";
    }
    std::cout << ss.rdbuf();
}

int CPUMap::from_socket(int socket, int num) {
    auto& v = cores.at(socket);
    ensure(v.size() > 0, "socket has no cores");
    return v.at(num % v.size());
}

int CPUMap::from_socket_first(int socket, int num) {
    auto avail_cores = cores.at(socket);
    ensure(avail_cores.size() > 0, "socket has no cores");
    for (auto& [s, v] : cores) {
        if (s == socket) {
            continue;
        }
        std::copy(v.begin(), v.end(), std::back_inserter(avail_cores));
    }
    return avail_cores.at(num % avail_cores.size());
}

void CPUMap::pin(int num) {
    ensure(num < total_cores, "core-id out of bounds");
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(num, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        std::perror("pthread_setaffinity_np");
    }
}

void CPUMap::pin_to_socket(int socket, int num) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(from_socket(socket, num), &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        std::perror("pthread_setaffinity_np");
    }
}


void CPUMap::pin_to_socket_first(int socket, int num) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(from_socket_first(socket, num), &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        std::perror("pthread_setaffinity_np");
    }
}


void CPUMap::pin_to_socket_free(int socket) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (auto c : cores.at(socket)) {
        CPU_SET(c, &cpuset);
    }
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        std::perror("pthread_setaffinity_np");
    }
}

void CPUMap::unpin() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    for (long i = 0; i < n; ++i) {
        CPU_SET(i, &cpuset);
    }
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        std::perror("pthread_setaffinity_np");
    }
}
