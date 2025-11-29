#pragma once

#include "utils/my_asserts.hpp"

#include <cstddef>
#include <cstdint>

template <typename T, size_t N>
struct Stack {
    static constexpr size_t capacity = N;
    uint64_t idx = 0;
    T array[capacity];

    void push(T val) {
        ensure(!full());
        array[idx++] = val;
    }

    T pop() {
        ensure(!empty());
        return array[--idx];
    }

    bool empty() {
        return idx == 0;
    }

    bool full() {
        return idx == capacity;
    }

    uint64_t size() {
        return idx;
    }
};
