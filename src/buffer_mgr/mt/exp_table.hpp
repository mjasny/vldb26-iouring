#pragma once

#include <array>
#include <cmath>
#include <cstddef>

// Compile-time generator for exp lookup table
template <std::size_t N>
struct ExpTable {
    static constexpr std::array<double, N + 1> generate() {
        std::array<double, N + 1> table = {};
        for (std::size_t i = 0; i <= N; ++i) {
            table[i] = std::exp(static_cast<double>(i));
        }
        return table;
    }
    static constexpr std::array<double, N + 1> values = generate();
};

// definition outside for static constexpr
template <std::size_t N>
constexpr std::array<double, N + 1> ExpTable<N>::values;
