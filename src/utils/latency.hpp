// Shared latency utilities extracted from msg-lat.cpp and wake-lat.cpp
#pragma once

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <assert.h>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>

namespace latency {

// Histogram layout (same as original PLAT_* scheme)
static constexpr unsigned PLAT_BITS = 6;
static constexpr unsigned PLAT_VAL = (1u << PLAT_BITS);
static constexpr unsigned PLAT_GROUP_NR = 29;
static constexpr unsigned PLAT_NR = (PLAT_GROUP_NR * PLAT_VAL);

static constexpr float plist[] = {1.0,  5.0,   10.0,  20.0,  30.0,  40.0,
                                  50.0, 60.0,  70.0,  80.0,  90.0,  95.0,
                                  99.0, 99.5, 99.9, 99.95, 99.99};
static constexpr unsigned plist_len = sizeof(plist) / sizeof(plist[0]);

static inline unsigned long plat_idx_to_val(unsigned idx) {
    unsigned int error_bits;
    unsigned long k, base;

    assert(idx < PLAT_NR);
    if (idx < (PLAT_VAL << 1))
        return idx;

    error_bits = (idx >> PLAT_BITS) - 1;
    base = ((unsigned long)1) << (error_bits + PLAT_BITS);
    k = idx % PLAT_VAL;
    return base + ((k + 0.5) * (1 << error_bits));
}

static inline unsigned int plat_val_to_idx(unsigned long val) {
    unsigned int msb, error_bits, base, offset, idx;
    if (val == 0)
        msb = 0;
    else
        msb = (sizeof(val) * 8) - __builtin_clzll(val) - 1;
    if (msb <= PLAT_BITS)
        return val;
    error_bits = msb - PLAT_BITS;
    base = (error_bits + 1) << PLAT_BITS;
    offset = (PLAT_VAL - 1) & (val >> error_bits);
    idx = (base + offset) < (PLAT_NR - 1) ? (base + offset) : (PLAT_NR - 1);
    return idx;
}

struct Histogram {
    unsigned long* plat;
    Histogram() : plat(nullptr) {}
    void init() {
        if (!plat)
            plat = (unsigned long*)calloc(PLAT_NR, sizeof(unsigned long));
    }
    void add(uint64_t v) {
        unsigned int pidx = plat_val_to_idx(v);
        plat[pidx]++;
    }
    void show(unsigned long nr, unsigned precision, const char* name,
              unsigned long msg) {
        if (!plat)
            return;
        unsigned long sum = 0;
        unsigned long maxv = 0, minv = ~0UL;
        unsigned long* ovals = (unsigned long*)malloc(plist_len * sizeof(*ovals));
        if (!ovals)
            return;
        bool is_last = false;
        unsigned j = 0;
        for (unsigned i = 0; i < PLAT_NR && !is_last; i++) {
            sum += plat[i];
            while (sum >= ((long double)plist[j] / 100.0 * nr)) {
                ovals[j] = plat_idx_to_val(i);
                if (ovals[j] < minv)
                    minv = ovals[j];
                if (ovals[j] > maxv)
                    maxv = ovals[j];
                is_last = (j == plist_len - 1);
                if (is_last)
                    break;
                j++;
            }
        }

        printf("Latencies for: %s (msg=%lu)\n", name, msg);
        int scale_down = 0;
        unsigned int divisor = 1;
        printf("    percentiles (nsec):\n     |");

        int time_width = std::max(5, (int)(std::log10((maxv ? maxv : 1) / divisor) + 1));
        char fmt[32];
        std::snprintf(fmt, sizeof(fmt), " %%%u.%ufth=[%%%dllu]%%c", precision + 3,
                      precision, time_width);
        int per_line = (80 - 7) / (precision + 10 + time_width);

        for (j = 0; j < plist_len; j++) {
            if (j != 0 && (j % per_line) == 0)
                printf("     |");
            for (int i = 0; i < scale_down; i++)
                ovals[j] = (ovals[j] + 999) / 1000;
            printf(fmt, plist[j], ovals[j], (j == plist_len - 1) ? '\n' : ',');
            if ((j % per_line) == per_line - 1)
                printf("\n");
        }
        free(ovals);
    }
};

// Monotonic base and helpers
static inline uint64_t ns_diff(timespec* t1, timespec* t2) {
    timespec tmp = *t2;
    tmp.tv_sec -= t1->tv_sec;
    tmp.tv_nsec -= t1->tv_nsec;
    return tmp.tv_sec * 1000000000ULL + tmp.tv_nsec;
}

static inline void set_start(timespec& start_ts) {
    clock_gettime(CLOCK_MONOTONIC, &start_ts);
}

static inline uint64_t get_ns_fast(const timespec& start_ts, timespec* now) {
    clock_gettime(CLOCK_MONOTONIC, now);
    return ns_diff(const_cast<timespec*>(&start_ts), now);
}

static inline uint64_t get_ns(const timespec& start_ts, timespec* now) {
    clock_gettime(CLOCK_MONOTONIC, now);
    return ns_diff(const_cast<timespec*>(&start_ts), now);
}

} // namespace latency

