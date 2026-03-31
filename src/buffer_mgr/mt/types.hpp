#pragma once

#include <cstddef>
#include <cstdint>

using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
using s8 = int8_t;
using s16 = int16_t;
using s32 = int32_t;
using s64 = int64_t;

typedef u64 PID; // Page ID (on SSD)
typedef u64 BID; // Buffer ID (in memory)


struct alignas(4096) Page {

    uint64_t checksum() {
        uint64_t checksum = 0;
        for (size_t i = 0; i < sizeof(Page); ++i) {
            checksum += reinterpret_cast<uint8_t*>(this)[i];
        }
        return checksum;
    }
};

enum class action_t {
    OK,
    RESTART,
};
