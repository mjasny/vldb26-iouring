
#include "perf_event.h"

#include <stdbool.h>
#include <stdio.h>

int main() {
    PerfEvent pevent = {0};

    register_counter(&pevent, "cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, ALL);
    register_counter(&pevent, "instructions", PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS, ALL);
    register_counter(&pevent, "task-clock", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_TASK_CLOCK, ALL);
    register_counter(&pevent, "l1-misses", PERF_TYPE_HW_CACHE, PERF_COUNT_HW_CACHE_L1D | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16), ALL);
    register_counter(&pevent, "llc-misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES, ALL);
    register_counter(&pevent, "branch-misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES, ALL);

    start_counters(&pevent);

    // Simulated workload
    for (volatile int i = 0; i < 100000000; ++i)
        ;

    stop_counters(&pevent);

    print_report(&pevent, 1, false);
    printf("-------------------\n");
    print_report(&pevent, 100000000, false);

    cleanup(&pevent);

    return 0;
}
