#pragma once


#include "utils/my_logger.hpp"

#include <chrono>
#include <cstdint>


class RateLimiter {
public:
    RateLimiter(double rate, uint64_t threads, uint64_t thread_id, bool spiky) {
        double rate_per_thread = rate / threads;
        inter_arrival_time = 1e6 / rate_per_thread; // microseconds
        double inter_arrival_time_offset = (inter_arrival_time / threads) * thread_id;
        if (spiky) {
            inter_arrival_time_offset = 0.0;
        }
        Logger::info("offset=", inter_arrival_time_offset);
        next_time = std::chrono::steady_clock::now() + std::chrono::microseconds(static_cast<uint64_t>(inter_arrival_time_offset + inter_arrival_time));
    }

    uint64_t wait() {
        next_time += std::chrono::microseconds(static_cast<uint64_t>(inter_arrival_time));
        auto now = std::chrono::steady_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(now - next_time).count();
        wait_until(next_time);
        return diff;
    }


    void run(std::function<void()> action, std::function<void(uint64_t)> sampling) {
        next_time += std::chrono::microseconds(static_cast<uint64_t>(inter_arrival_time));
        auto now = std::chrono::steady_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(now - next_time).count();
        wait_until(next_time);
        auto begin = std::chrono::steady_clock::now();
        action();
        auto end = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count();
        if (diff > 0) {
            end += diff;
        }
        sampling(end);
    }

private:
    double inter_arrival_time;
    std::chrono::steady_clock::time_point next_time;

    static void wait_until(std::chrono::steady_clock::time_point next) {
        auto current = std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::duration<double>>(next - current).count() > 0.0) {
            current = std::chrono::steady_clock::now();
            _mm_pause(); // spin-wait
        }
    }
};
