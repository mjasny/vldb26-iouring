#pragma once

#include "types.hpp"
#include "utils/my_asserts.hpp"

#include <immintrin.h>
#include <thread>
#include <vector>

template <class Fn>
void parallel_for(uint64_t begin, uint64_t end, uint64_t nthreads, Fn fn) {
    std::vector<std::thread> threads;
    uint64_t n = end - begin;
    if (n < nthreads)
        nthreads = n;
    uint64_t perThread = n / nthreads;
    for (unsigned i = 0; i < nthreads; i++) {
        threads.emplace_back([&, i]() {
            uint64_t b = (perThread * i) + begin;
            uint64_t e = (i == (nthreads - 1)) ? end : (b + perThread);
            fn(i, b, e);
        });
    }
    for (auto& t : threads)
        t.join();
}


template <class T>
static T loadUnaligned(void* p) {
    T x;
    memcpy(&x, p, sizeof(T));
    return x;
}

static unsigned min(unsigned a, unsigned b) {
    return a < b ? a : b;
}


// Get order-preserving head of key (assuming little endian)
static u32 head(u8* key, unsigned keyLen) {
    switch (keyLen) {
        case 0:
            return 0;
        case 1:
            return static_cast<u32>(key[0]) << 24;
        case 2:
            return static_cast<u32>(__builtin_bswap16(loadUnaligned<u16>(key))) << 16;
        case 3:
            return (static_cast<u32>(__builtin_bswap16(loadUnaligned<u16>(key))) << 16) | (static_cast<u32>(key[2]) << 8);
        default:
            return __builtin_bswap32(loadUnaligned<u32>(key));
    }
}


// use when lock is not free
static void yield(u64 counter) {
    _mm_pause();
}
