#pragma once
#include "utils/hugepages.hpp"
#include "utils/my_asserts.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

// fixed-size robin-hood with backshift deletion (u64 -> u64)

template <typename Value>
struct RHBSU64Map {
    struct Entry {
        uint64_t key;
        Value val;
    };
    static_assert(sizeof(Value) <= sizeof(uint64_t));

    explicit RHBSU64Map(size_t capacity_pow2)
        : n(capacity_pow2), mask(capacity_pow2 - 1) {
        if (n == 0 || (n & (n - 1)))
            throw std::invalid_argument("capacity must be power of two");

        // keys = new uint64_t[n];
        // vals = new uint64_t[n];
        // ctrl = new uint8_t[n]; // probe distance: 0=empty, 1..254 distance
        // std::memset(ctrl, 0, n);

        entries = HugePages::malloc_array<Entry>(n);
        ctrl = HugePages::malloc_array<uint8_t>(n);

        // We use key=max u64 as forbidden EMPTY sentinel; change if you need full key-space
        for (uint64_t i = 0; i < n; ++i) {
            entries[i].key = EMPTY_KEY;
        }
    }

    ~RHBSU64Map() {
        HugePages::free_array<Entry>(entries, n);
        HugePages::free_array<uint8_t>(ctrl, n);
        // delete[] keys;
        // delete[] vals;
        // delete[] ctrl;
    }

    RHBSU64Map(const RHBSU64Map&) = delete;
    RHBSU64Map& operator=(const RHBSU64Map&) = delete;

    // Insert or update. Returns true if inserted new, false if updated existing.
    bool insert(uint64_t k, Value v) {
        ensure(k != EMPTY_KEY, "key equals EMPTY_KEY sentinel");
        size_t i = index(hash(k));
        uint8_t dist = 1;

        for (;;) {
            uint8_t c = ctrl[i];
            if (c == 0) { // empty: place here
                entries[i].key = k;
                entries[i].val = v;
                ctrl[i] = dist;
                ++sz;
                return true;
            }
            if (entries[i].key == k) {
                entries[i].val = v;
                return false;
            } // update

            if (c < dist) { // robin-hood swap
                std::swap(k, entries[i].key);
                std::swap(v, entries[i].val);
                std::swap(dist, ctrl[i]);
            }
            i = (i + 1) & mask;

            if (++dist == 255)
                ensure(false, "probe distance overflow; table too full");
        }
    }

    // Returns pointer to value or nullptr if not found.
    Value* find(uint64_t k) {
        if (k == EMPTY_KEY)
            return nullptr;
        size_t i = index(hash(k));
        uint8_t dist = 1;

        for (;;) {
            uint8_t c = ctrl[i];
            if (c == 0)
                return nullptr; // empty -> not present
            if (c < dist)
                return nullptr; // early exit (robin-hood property)
            if (entries[i].key == k)
                return &entries[i].val;
            i = (i + 1) & mask;
            if (++dist == 255)
                return nullptr;
        }
    }

    bool erase(uint64_t k) {
        if (k == EMPTY_KEY)
            return false;
        size_t i = index(hash(k));
        uint8_t dist = 1;

        for (;;) {
            uint8_t c = ctrl[i];
            if (c == 0)
                return false;
            if (c < dist)
                return false;
            if (entries[i].key == k) {
                backshift_delete(i);
                --sz;
                return true;
            }
            i = (i + 1) & mask;
            if (++dist == 255)
                return false;
        }
    }

    size_t size() const {
        return sz;
    }
    size_t capacity() const {
        return n;
    }
    double load_factor() const {
        return double(sz) / double(n);
    }


    template <class Callback>
    bool clock_sweep_next(Callback&& cb) {
        if (sz == 0)
            return false;

        size_t scanned = 0;
        while (scanned < n) {
            size_t idx = sweep_;
            sweep_ = (sweep_ + 1) & mask; // advance hand regardless (fairness)

            if (ctrl[idx] != 0) { // occupied (no tombstones in backshift scheme)
                // Deduce callback return type
                using Ret = std::invoke_result_t<Callback, uint64_t, Value&>;
                if constexpr (std::is_same_v<Ret, bool>) {
                    if (cb(entries[idx].key, entries[idx].val))
                        return true; // accepted by callback
                    // else continue sweeping in this call
                } else {
                    cb(entries[idx].key, entries[idx].val); // one-and-done
                    return true;
                }
            }
            ++scanned;
        }
        return false; // no occupied slot accepted in a full rotation
    }

    template <class Callback>
    void dump(Callback&& cb) {
        for (int idx = 0; idx < n; ++idx) {
            if (ctrl[idx] != 0) { // occupied (no tombstones in backshift scheme)
                auto key = entries[idx].key;
                cb(key, entries[idx].val, index(hash(key)), idx);
            }
        }
    }


private:
    static constexpr uint64_t EMPTY_KEY = std::numeric_limits<uint64_t>::max();

    // splitmix64: fast, good diffusion
    static uint64_t hash(uint64_t x) {
        x += 0x9e3779b97f4a7c15ULL;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    }


    // murmur hash
    static uint64_t hash2(uint64_t k) {
        const uint64_t m = 0xc6a4a7935bd1e995;
        const int r = 47;
        uint64_t h = 0x8445d61a4e774912 ^ (8 * m);
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        return h;
    }

    // scalestore hash
    static uint64_t hash3(uint64_t input) {
        uint64_t local_rand = input;
        uint64_t local_rand_hash = 8;
        local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
        local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
        local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
        local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
        local_rand_hash = 40343 * local_rand_hash;
        return local_rand_hash; // if 64 do not rotate
    }


    inline size_t index(uint64_t h) const {
        return size_t(h) & mask;
    }

    // Backshift deletion: pull following entries left while their distance > 1
    void backshift_delete(size_t hole) {
        size_t j = hole;
        size_t k = (j + 1) & mask;
        for (;;) {
            uint8_t ck = ctrl[k];
            if (ck <= 1) { // next slot empty or home -> stop
                ctrl[j] = 0;
                entries[j].key = EMPTY_KEY; // optional (kept for clarity)
                return;
            }
            // Move k back by one
            entries[j] = entries[k];
            ctrl[j] = ck - 1;
            j = k;
            k = (k + 1) & mask;
        }
    }

    Entry* entries = nullptr;
    uint8_t* ctrl = nullptr;
    size_t n = 0, mask = 0, sz = 0;

public:
    size_t sweep_ = 0;
};
