#pragma once

#include "utils/singleton.hpp"

#include <map>
#include <vector>


struct CPUMap : public Singleton<CPUMap> {

    // numa-node -> core-id
    std::map<int, std::vector<int>> cores;
    size_t total_cores;

    CPUMap();

    void print();

    int from_socket(int socket, int num);

    int from_socket_first(int socket, int num);


    void unpin();

    void pin(int num);

    void pin_to_socket(int socket, int num);

    void pin_to_socket_first(int socket, int num);

    void pin_to_socket_free(int socket);
};
