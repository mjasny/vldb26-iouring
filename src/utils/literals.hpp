#pragma once

#include <cstddef>
#include <cstdint>

constexpr uint64_t operator""_MHz(long double freq) {
    return static_cast<uint64_t>(freq * 1e6);
}

constexpr uint64_t operator""_GHz(long double freq) {
    return static_cast<uint64_t>(freq * 1e9);
}


constexpr uint64_t operator""_KiB(unsigned long long const x) {
    return 1024L * x;
}

constexpr uint64_t operator""_MiB(unsigned long long const x) {
    return 1024L * 1024L * x;
}

constexpr uint64_t operator""_GiB(unsigned long long const x) {
    return 1024L * 1024L * 1024L * x;
}

constexpr uint64_t operator""_TiB(unsigned long long const x) {
    return 1024L * 1024L * 1024L * 1024L * x;
}
