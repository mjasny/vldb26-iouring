#pragma once

#include "utils/cli_parser.hpp"
#include "utils/types.hpp"

struct Config {
    std::string ip = "127.0.0.1";
    uint16_t port = 1234;
    SetupMode setup_mode = SetupMode::DEFAULT;
    int core_id = 3;
    bool napi = false;
    bool reg_ring = false;
    bool reg_bufs = false;
    bool reg_fds = false;
    uint32_t duration = 30'000;
    uint64_t resp_delay = 0; // microseconds
    uint64_t ping_size = 1;  // bytes
    uint32_t num_threads = 1;
    bool tcp = true;
    bool poll_first = false;
    bool pingpong = true;
    bool perfevent = false;
    bool is_client = false;
    uint64_t max_clients = 1;
    bool pin_queues = false;
    int rx_queue = -1;
    std::string local_ip;


    bool mshot_recv = false;


    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--ip", ip, cli::Parser::optional);
        parser.parse("--port", port, cli::Parser::optional);
        parser.parse("--setup_mode", setup_mode, cli::Parser::optional);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--napi", napi, cli::Parser::optional);
        parser.parse("--reg_ring", reg_ring, cli::Parser::optional);
        parser.parse("--reg_bufs", reg_bufs, cli::Parser::optional);
        parser.parse("--reg_fds", reg_fds, cli::Parser::optional);
        parser.parse("--duration", duration, cli::Parser::optional);
        parser.parse("--resp_delay", resp_delay, cli::Parser::optional);
        parser.parse("--ping_size", ping_size, cli::Parser::optional);
        parser.parse("--num_threads", num_threads, cli::Parser::optional);
        parser.parse("--tcp", tcp, cli::Parser::optional);
        parser.parse("--poll_first", poll_first, cli::Parser::optional);
        parser.parse("--pingpong", pingpong, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--is_client", is_client, cli::Parser::optional);
        parser.parse("--max_clients", max_clients, cli::Parser::optional);
        parser.parse("--pin_queues", pin_queues, cli::Parser::optional);
        parser.parse("--rx_queue", rx_queue, cli::Parser::optional);
        parser.parse("--local_ip", local_ip, cli::Parser::optional);
        parser.check_unparsed();
        parser.print();

        if (rx_queue != -1) {
            ensure(pin_queues);
        }
        if (pin_queues) {
            ensure(rx_queue != -1);
        }
    }
};
