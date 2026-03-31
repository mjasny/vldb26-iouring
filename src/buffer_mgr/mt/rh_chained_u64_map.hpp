#pragma once
#include "utils/hugepages.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

// very-simple chained hashtable (u64 -> u64), drop-in compatible with RHBSU64Map API
template <typename Value>
struct RHCHU64Map {
    static_assert(sizeof(Value) <= sizeof(uint64_t));

    explicit RHCHU64Map(size_t capacity_pow2)
        : n(capacity_pow2), mask(capacity_pow2 - 1) {
        if (n == 0 || (n & (n - 1)))
            throw std::invalid_argument("capacity must be power of two");

        // bucket array lives on huge pages for cache- and TLB-friendly scanning
        buckets = HugePages::malloc_array<Node*>(n);
        std::fill(buckets, buckets + n, nullptr);
    }

    ~RHCHU64Map() {
        // Nodes are allocated with new; clean them up.
        if (!buckets)
            return;
        for (size_t i = 0; i < n; ++i) {
            Node* cur = buckets[i];
            while (cur) {
                Node* nxt = cur->next;
                delete cur;
                cur = nxt;
            }
            buckets[i] = nullptr;
        }
    }

    RHCHU64Map(const RHCHU64Map&) = delete;
    RHCHU64Map& operator=(const RHCHU64Map&) = delete;

    // Insert or update. Returns true if inserted new, false if updated existing.
    bool insert(uint64_t k, Value v) {
        if (k == EMPTY_KEY)
            throw std::invalid_argument("key equals EMPTY_KEY sentinel");

        size_t i = index(hash(k));
        for (Node* p = buckets[i]; p; p = p->next) {
            if (p->k == k) {
                p->v = v; // update existing
                return false;
            }
        }
        // push-front insert (simple & fast)
        buckets[i] = new Node{k, v, buckets[i]};
        ++sz;
        return true;
    }

    // Returns pointer to value or nullptr if not found.
    Value* find(uint64_t k) {
        if (k == EMPTY_KEY)
            return nullptr;
        size_t i = index(hash(k));
        for (Node* p = buckets[i]; p; p = p->next) {
            if (p->k == k)
                return &p->v;
        }
        return nullptr;
    }

    bool erase(uint64_t k) {
        if (k == EMPTY_KEY)
            return false;

        size_t i = index(hash(k));
        Node* prev = nullptr;
        Node* cur = buckets[i];

        while (cur) {
            if (cur->k == k) {
                if (prev)
                    prev->next = cur->next;
                else
                    buckets[i] = cur->next;
                delete cur;
                --sz;
                return true;
            }
            prev = cur;
            cur = cur->next;
        }
        return false;
    }

    size_t size() const {
        return sz;
    }
    size_t capacity() const {
        return n; // number of buckets
    }
    double load_factor() const {
        return double(sz) / double(n);
    }

    // Clock-sweep across buckets. We try buckets in round-robin order starting at `sweep_`.
    // If Callback returns bool, we continue sweeping within the call until one returns true or we
    // wrap around. If Callback is void (or non-bool), we process the first element we see and return.
    template <class Callback>
    bool clock_sweep_next(Callback&& cb) {
        if (sz == 0)
            return false;

        size_t scanned = 0;
        while (scanned < n) {
            size_t idx = sweep_;
            sweep_ = (sweep_ + 1) & mask; // advance hand regardless (fairness)

            Node* node = buckets[idx];
            while (node) {
                using Ret = std::invoke_result_t<Callback, uint64_t, uint64_t&>;
                Node* next = node->next; // compute next up-front to be robust if cb erases `node`

                if constexpr (std::is_same_v<Ret, bool>) {
                    if (cb(node->k, node->v))
                        return true; // accepted by callback; one-and-done for this call
                    // else try next element in this bucket
                    node = next;
                } else {
                    cb(node->k, node->v); // one element processed; return
                    return true;
                }
            }
            ++scanned;
        }
        return false; // full rotation with no element accepted
    }

private:
    struct Node {
        uint64_t k;
        Value v;
        Node* next;
    };

    static constexpr uint64_t EMPTY_KEY = std::numeric_limits<uint64_t>::max();

    // splitmix64: fast, good diffusion
    static uint64_t hash(uint64_t x) {
        x += 0x9e3779b97f4a7c15ULL;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    }

    // murmur-ish variant (unused but kept to match original interface/availability)
    static uint64_t hash2(uint64_t k) {
        const uint64_t m = 0xc6a4a7935bd1e995ULL;
        const int r = 47;
        uint64_t h = 0x8445d61a4e774912ULL ^ (8ULL * m);
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

    inline size_t index(uint64_t h) const {
        return static_cast<size_t>(h) & mask;
    }

    Node** buckets = nullptr;
    size_t n = 0, mask = 0, sz = 0;

    // "clock hand" across buckets (kept to match original semantics)
    size_t sweep_ = 0;
};
