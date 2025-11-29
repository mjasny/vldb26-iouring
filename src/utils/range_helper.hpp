#pragma once


#include <cstdint>
#include <utility>

struct RangeHelper {
    static auto nth_chunk(uint64_t start, uint64_t end, uint64_t chunks, uint64_t n) {
        // ensure(start <= end);

        uint64_t total_size = end - start;
        uint64_t left = total_size % chunks;
        uint64_t p_size = (total_size - left) / chunks;

        uint64_t offset = start + std::min(n, left);

        uint64_t s = offset + p_size * n;
        uint64_t e = offset + p_size * (n + 1) + (n < left);


        return std::make_pair(s, e);
    }
};