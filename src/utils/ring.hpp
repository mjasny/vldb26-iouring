#pragma once

#include "utils/utils.hpp"

#include <cstddef>
#include <cstdint>

template <typename T, size_t N>
struct StackRing {
    static constexpr size_t capacity = N;
    static_assert(is_power_of_two(capacity));
    uint64_t read = 0;
    uint64_t write = 0;
    T array[capacity];

    void push(T val) {
        ensure(!full());
        array[mask(write++)] = val;
    }

    T pop() {
        ensure(!empty());
        return array[mask(read++)];
    }

    bool empty() {
        return read == write;
    }

    bool full() {
        return size() == capacity;
    }

    uint64_t size() {
        return write - read;
    }

private:
    auto mask(uint64_t val) {
        return val & (capacity - 1);
    }
};
