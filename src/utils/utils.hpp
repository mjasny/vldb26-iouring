#pragma once

#include "utils/my_asserts.hpp"

#include <bit>
#include <chrono>
#include <csignal>
#include <cstdint>

template <typename T>
uint64_t calc_bps(uint64_t bytes, T duration, uint64_t factor = 1) {
    double amount = static_cast<double>(bytes * factor);
    double duration_d = static_cast<double>(duration);
    return static_cast<uint64_t>(amount / duration_d);
}


template <typename Duration>
void busy_sleep(Duration duration) {
    auto start_time = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start_time < duration) {
        // Busy-waiting loop
    }
}


template <typename... Bools>
int count_true(Bools... args) {
    return ((args ? 1 : 0) + ...);
}


inline void set_realtime_priority(int prio = 99) {
    struct sched_param param = {};
    param.sched_priority = prio; // Highest real-time priority (1-99)

    check_zero(sched_setscheduler(0, SCHED_FIFO, &param));
}


template <typename T>
bool constexpr is_power_of_two(T x) {
    return (x != 0) && ((x & (x - 1)) == 0);
}


constexpr std::uint64_t next_pow2(std::uint64_t n) {
    if (n <= 1)
        return 1;
    if (n > (1ull << 63))
        return 0;
    return 1ull << std::bit_width(n - 1); // ceil_pow2
}


constexpr int ilog2(uint32_t i) {
    int log = -1;
    while (i) {
        i >>= 1;
        log++;
    }
    return log;
}


template <typename T>
struct Diff {
    T last = 0;
    T operator()(T current) {
        auto diff = current - last;
        last = current;
        return diff;
    }
};


template <typename T>
inline void do_not_optimize(T const& value) {
    asm volatile(""
                 :
                 : "i,r,m"(value)
                 : "memory");
}


inline void gdb_hook() {
    std::raise(SIGINT);
}
