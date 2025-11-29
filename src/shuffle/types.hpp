#pragma once

#include <cstddef>
#include <cstdint>


template <size_t N>
struct Tuple {
    uint64_t key;
    uint8_t value[N - sizeof(Tuple::key)];
};
