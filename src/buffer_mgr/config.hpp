#pragma once

#include "types.hpp"
#include "utils/literals.hpp"
#include "utils/my_logger.hpp"
#include "utils/singleton.hpp"
#include "utils/types.hpp"

constexpr u64 pageSize = 4096;

struct Config : Singleton<Config> {
    SetupMode setup_mode = SetupMode::DEFER_TASKRUN;
    bool reg_ring = false;
    bool reg_fds = false;
    bool reg_bufs = false;
    bool iopoll = false;
    bool nvme_cmds = false;

    int core_id = 64;
    uint32_t stats_interval = 1'000'000;
    uint32_t duration = 30'000;

    std::string ssd;
    uint64_t virt_size = 16_GiB;
    uint64_t phys_size = 4_GiB;
    uint64_t evict_batch = 64;
    int concurrency = 1;
    float free_target = 0.1;
    float page_table_factor = 1.5; // for ycsb choose 2.5

    std::string workload;
    bool submit_always = false;
    bool sync_variant = false;
    bool posix_variant = false;

    uint64_t ycsb_tuple_count = 100;
    int ycsb_read_ratio = 50;

    int tpcc_warehouses = 1;

    bool libaio = false;


    void parse(int argc, char** argv);
};
