#pragma once


#include <chrono>
#include <cstdint>
#include <x86intrin.h> // For __rdtsc() and __rdtscp()


struct RDTSCClock {

    RDTSCClock(uint64_t cpu_frequency_hz);

    uint64_t start();

    uint64_t stop();

    static uint64_t read();

    uint64_t cycles() const;

    template <typename Duration>
    auto as() const {
        auto ns = static_cast<uint64_t>(cycles() * 1e9 / cpu_frequency_hz);
        return std::chrono::duration_cast<Duration>(std::chrono::nanoseconds(ns));
    }

    template <typename Duration, typename T>
    auto as() const {
        auto val = as<Duration>();
        return static_cast<T>(val.count());
    }

    template <typename Duration, typename T>
    auto convert(auto cycles) const {
        auto ns = static_cast<uint64_t>(cycles * 1e9 / cpu_frequency_hz);
        auto val = std::chrono::duration_cast<Duration>(std::chrono::nanoseconds(ns));
        return static_cast<T>(val.count());
    }

private:
    uint64_t cpu_frequency_hz;
    uint64_t start_cycles = 0;
    uint64_t end_cycles = 0;
};
