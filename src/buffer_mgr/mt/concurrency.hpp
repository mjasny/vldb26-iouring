#pragma once

#include "types.hpp"

#include <atomic>
#include <type_traits>

template <typename T>
struct AtomicCounter {
    static_assert(std::is_integral_v<T>);

    std::atomic<T> value {};

    AtomicCounter() = default;
    constexpr AtomicCounter(T init) : value(init) {
    }

    T load() const noexcept {
        return value.load(std::memory_order_relaxed);
    }

    void store(T v) noexcept {
        value.store(v, std::memory_order_relaxed);
    }

    T fetch_add(T delta) noexcept {
        return value.fetch_add(delta, std::memory_order_relaxed);
    }

    T fetch_sub(T delta) noexcept {
        return value.fetch_sub(delta, std::memory_order_relaxed);
    }

    operator T() const noexcept {
        return load();
    }

    AtomicCounter& operator=(T v) noexcept {
        store(v);
        return *this;
    }

    T operator++() noexcept {
        return value.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    T operator++(int) noexcept {
        return fetch_add(1);
    }

    T operator--() noexcept {
        return value.fetch_sub(1, std::memory_order_relaxed) - 1;
    }

    T operator--(int) noexcept {
        return fetch_sub(1);
    }

    AtomicCounter& operator+=(T delta) noexcept {
        value.fetch_add(delta, std::memory_order_relaxed);
        return *this;
    }

    AtomicCounter& operator-=(T delta) noexcept {
        value.fetch_sub(delta, std::memory_order_relaxed);
        return *this;
    }
};

struct PageState {
    std::atomic<u64> state_and_version {0};

    static constexpr u64 Unlocked = 0;
    static constexpr u64 MaxShared = 252;
    static constexpr u64 Locked = 253;
    static constexpr u64 Marked = 254;
    static constexpr u64 Evicted = 255;
    static constexpr u64 IoInProgressBit = 1ull << 0;
    static constexpr u64 EvictingBit = 1ull << 1;
    static constexpr u64 DirtyBit = 1ull << 2;
    static constexpr u64 FlagsMask = 0xFFull;
    static constexpr u64 VersionMask = ((1ull << 48) - 1) << 8;

    PageState() = default;

    void init() noexcept {
        state_and_version.store(same_version(0, Evicted), std::memory_order_release);
    }

    static constexpr u64 same_version(u64 old_state_and_version, u64 new_state) noexcept {
        return (old_state_and_version & (VersionMask | FlagsMask)) | (new_state << 56);
    }

    static constexpr u64 next_version(u64 old_state_and_version, u64 new_state) noexcept {
        const u64 version = (old_state_and_version & VersionMask);
        const u64 next = (version + (1ull << 8)) & VersionMask;
        return (old_state_and_version & FlagsMask) | next | (new_state << 56);
    }

    static constexpr u64 get_state(u64 v) noexcept {
        return v >> 56;
    }

    static constexpr u64 get_version(u64 v) noexcept {
        return (v & VersionMask) >> 8;
    }

    u64 load() const noexcept {
        return state_and_version.load(std::memory_order_acquire);
    }

    static constexpr u64 unlocked_state() noexcept {
        return same_version(0, Unlocked);
    }

    static constexpr u64 marked_state() noexcept {
        return same_version(0, Marked);
    }

    static constexpr u64 locked_state() noexcept {
        return same_version(0, Locked);
    }

    static constexpr u64 next_version_no_flags(u64 old_state_and_version, u64 new_state) noexcept {
        const u64 version = (old_state_and_version & VersionMask);
        const u64 next = (version + (1ull << 8)) & VersionMask;
        return next | (new_state << 56);
    }

    void store_state(u64 value) noexcept {
        state_and_version.store(value, std::memory_order_release);
    }

    void init_unlocked(bool marked = false) noexcept {
        store_state(marked ? marked_state() : unlocked_state());
    }

    void init_locked() noexcept {
        store_state(locked_state());
    }

    void rebind_locked() noexcept {
        store_state(next_version_no_flags(load(), Locked));
    }

    void rebind_unlocked(bool marked = false) noexcept {
        store_state(next_version_no_flags(load(), marked ? Marked : Unlocked));
    }

    u64 get_state() const noexcept {
        return get_state(load());
    }

    bool flag(u64 bit) const noexcept {
        return (load() & bit) != 0;
    }

    bool io_in_progress() const noexcept {
        return flag(IoInProgressBit);
    }

    bool evicting() const noexcept {
        return flag(EvictingBit);
    }

    bool dirty() const noexcept {
        return flag(DirtyBit);
    }

    void update_flag(u64 bit, bool value) noexcept {
        if (value) {
            state_and_version.fetch_or(bit, std::memory_order_acq_rel);
        } else {
            state_and_version.fetch_and(~bit, std::memory_order_acq_rel);
        }
    }

    void set_io_in_progress(bool value) noexcept {
        update_flag(IoInProgressBit, value);
    }

    void set_evicting(bool value) noexcept {
        update_flag(EvictingBit, value);
    }

    void set_dirty(bool value) noexcept {
        update_flag(DirtyBit, value);
    }

    bool try_lock_x(u64 expected) noexcept {
        const u64 state = get_state(expected);
        if (state != Unlocked && state != Marked) {
            return false;
        }
        return state_and_version.compare_exchange_strong(expected, same_version(expected, Locked),
                                                         std::memory_order_acq_rel, std::memory_order_acquire);
    }

    // Like try_lock_x but also sets DirtyBit atomically in the same CAS.
    // Saves a second CAS in fixX paths that always mark the page dirty.
    bool try_lock_x_dirty(u64 expected) noexcept {
        const u64 state = get_state(expected);
        if (state != Unlocked && state != Marked) {
            return false;
        }
        return state_and_version.compare_exchange_strong(expected, same_version(expected, Locked) | DirtyBit,
                                                         std::memory_order_acq_rel, std::memory_order_acquire);
    }

    bool try_lock_s(u64 expected) noexcept {
        const u64 state = get_state(expected);
        if (state < MaxShared) {
            return state_and_version.compare_exchange_strong(expected, same_version(expected, state + 1),
                                                            std::memory_order_acq_rel, std::memory_order_acquire);
        }
        if (state == Marked) {
            return state_and_version.compare_exchange_strong(expected, same_version(expected, 1),
                                                            std::memory_order_acq_rel, std::memory_order_acquire);
        }
        return false;
    }

    bool try_mark(u64 expected) noexcept {
        if (get_state(expected) != Unlocked) {
            return false;
        }
        return state_and_version.compare_exchange_strong(expected, same_version(expected, Marked),
                                                        std::memory_order_acq_rel, std::memory_order_acquire);
    }

    bool try_clear_mark(u64 expected) noexcept {
        if (get_state(expected) != Marked) {
            return false;
        }
        return state_and_version.compare_exchange_strong(expected, same_version(expected, Unlocked),
                                                         std::memory_order_acq_rel, std::memory_order_acquire);
    }

    bool try_upgrade_s_to_x(u64 expected) noexcept {
        if (get_state(expected) != 1) {
            return false;
        }
        return state_and_version.compare_exchange_strong(expected, same_version(expected, Locked),
                                                         std::memory_order_acq_rel, std::memory_order_acquire);
    }

    void unlock_x() noexcept {
        state_and_version.store(next_version(load(), Unlocked), std::memory_order_release);
    }

    void unlock_x_marked() noexcept {
        state_and_version.store(next_version(load(), Marked), std::memory_order_release);
    }

    // Single load+store combining set_dirty(true) + unlock_x_marked().
    // Safe only while holding the X lock — no CAS needed since we're the sole writer.
    void unlock_x_marked_dirty() noexcept {
        const u64 current = load();
        state_and_version.store(next_version(current | DirtyBit, Marked), std::memory_order_release);
    }

    void unlock_x_evicted() noexcept {
        state_and_version.store(next_version(load(), Evicted), std::memory_order_release);
    }

    void unlock_s() noexcept {
        while (true) {
            u64 expected = load();
            const u64 state = get_state(expected);
            if (state_and_version.compare_exchange_strong(expected, same_version(expected, state - 1),
                                                          std::memory_order_acq_rel, std::memory_order_acquire)) {
                return;
            }
        }
    }

    void unlock_s_marked() noexcept {
        while (true) {
            u64 expected = load();
            const u64 state = get_state(expected);
            const u64 next = (state == 1) ? same_version(expected, Marked) : same_version(expected, state - 1);
            if (state_and_version.compare_exchange_strong(expected, next,
                                                          std::memory_order_acq_rel, std::memory_order_acquire)) {
                return;
            }
        }
    }

    void downgrade_lock() noexcept {
        state_and_version.store(next_version(load(), 1), std::memory_order_release);
    }
};
