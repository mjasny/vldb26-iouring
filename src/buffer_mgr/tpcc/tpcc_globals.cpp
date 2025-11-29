// Definitions for thread-local globals declared in tpcc_workload.hpp
#include <cstdint>

extern __thread uint16_t workerThreadId;
extern __thread int32_t tpcchistorycounter;

__thread uint16_t workerThreadId = 0;
__thread int32_t tpcchistorycounter = 0;

