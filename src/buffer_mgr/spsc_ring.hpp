#pragma once

#include <cstddef>

template <typename T, size_t CAP_POW2>
struct SpscRing {
    static_assert((CAP_POW2 & (CAP_POW2 - 1)) == 0,
                  "capacity must be power of two");

    static constexpr size_t mask = CAP_POW2 - 1;

    T buf_[CAP_POW2];
    size_t head_ = 0, tail_ = 0;

    inline bool push(T& v) noexcept {
        if (full()) [[unlikely]]
            return false; // full
        buf_[tail_ & mask] = v;
        ++tail_;
        return true;
    }

    inline bool pop(T& out) noexcept {
        if (empty()) [[unlikely]]
            return false; // empty
        out = buf_[head_ & mask];
        ++head_;
        return true;
    }

    inline bool empty() const noexcept {
        return head_ == tail_;
    }

    inline size_t size() const noexcept {
        return tail_ - head_;
    }

    inline bool full() const noexcept {
        return size() == CAP_POW2;
    }
};
