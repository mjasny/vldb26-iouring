#pragma once

#include "utils/hugepages.hpp"
#include "utils/utils.hpp"

#include <cstdint>
#include <limits>

#ifndef HT_PREFETCH
#define HT_PREFETCH(addr, rw, locality) __builtin_prefetch((addr), (rw), (locality))
#endif

template <typename Value>
class ChainedHT {
    static_assert(sizeof(Value) <= sizeof(uint64_t));

    struct Node {
        Node* next;
        uint64_t k;
        Value v;
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
    Node* nodes = nullptr;
    Node* free_list = nullptr;
    size_t n = 0;
    size_t mask = 0;
    size_t sz = 0;

    Node* alloc_node() {
        ensure(free_list);
        auto node = free_list;
        free_list = free_list->next;
        return node;
    }

public:
    explicit ChainedHT(size_t capacity)
        : n(capacity), mask(capacity - 1) {

        ensure(is_power_of_two(capacity), "capacity must be power of two");

        buckets = HugePages::malloc_array<Node*>(n);
        nodes = HugePages::malloc_array<Node>(n);

        // build free-list
        for (size_t i = 0; i + 1 < n; ++i) {
            nodes[i].next = &nodes[i + 1];
        }
        nodes[n - 1].next = free_list;
        free_list = &nodes[0];
    }

    ~ChainedHT() {
        HugePages::free_array(buckets, n);
        HugePages::free_array(nodes, n);
    }

    ChainedHT(const ChainedHT&) = delete;
    ChainedHT& operator=(const ChainedHT&) = delete;

    // Insert or update. Returns true if inserted new, false if updated existing.
    bool insert(uint64_t k, Value v) {
        ensure(k != EMPTY_KEY, "key equals EMPTY_KEY sentinel");

        size_t i = index(hash(k));
        Node** link = &buckets[i];

        for (Node* p = *link; p; p = p->next) {
            if (p->k == k) { // update existing
                p->v = v;
                return false;
            }
            link = &p->next; // advance walker: &current->next
        }

        // link now points to the tail slot (&last->next or &buckets[i] if empty)
        Node* node = alloc_node();
        new (node) Node{nullptr, k, v}; // append at tail
        *link = node;
        ++sz;
        return true;
    }


    static constexpr size_t BATCH_SIZE = 1024 * 2;


    // helps
    static constexpr int PREF_LINK_LOCALITY = 0;
    static constexpr int PREF_BUCKET_AHEAD = 14;
    static constexpr int PREF_BUCKET_LOCALITY = 0;
    static constexpr int PREF_QUEUE_AHEAD = 22;
    static constexpr int PREF_QUEUE_LOCALITY = 0;

    // does not help
    static constexpr int HEAD_PF_AHEAD = 6;
    static constexpr int PREF_HEAD_LOCALITY = 0;

    // Contiguous AoS that holds ALL hot per-item state (no random indirection).
    struct Work {
        Node** cur; // pointer-to-pointer walker (starts at &buckets[i])
        uint64_t k; // key
        Value v;    // value (<=8 bytes per your static_assert)
    };
    size_t batch_len_ = 0;
    alignas(64) std::array<Work, BATCH_SIZE> work_;

    inline void insert_batch(uint64_t key, Value val) {
        ensure(key != EMPTY_KEY, "key equals EMPTY_KEY sentinel");
        // Only record k/v here; slot/index resolution happens in the seed step
        Work& w = work_[batch_len_++];
        w.k = key;
        w.v = val;

        if (batch_len_ == BATCH_SIZE) {
            process_batch_full(); // fixed-size fast path
            batch_len_ = 0;
        }
    }

    inline void flush_batch() {
        if (batch_len_) {
            process_batch_var(batch_len_); // variable-size tail
            batch_len_ = 0;
        }
    }


    inline uint32_t seed_build_(size_t len, size_t& new_count) {
        auto* __restrict wv = work_.data();
        wv = (Work* __restrict)__builtin_assume_aligned(wv, 64);
        Node** __restrict B = (Node**)__builtin_assume_aligned(buckets, 64);

        // compute & store slot pointers once (hash+mask only here)
        for (uint32_t t = 0; t < len; ++t) {
            const uint64_t k = wv[t].k;
            const size_t i = index(hash(k));
            wv[t].cur = &B[i];
        }

        // linear seed with bucket-ahead prefetch and fast paths; compact walkers
        uint32_t wn = 0;
        for (uint32_t t = 0; t < len; ++t) {
            // prefetch upcoming slot pointer linearly (address of buckets[i])
            if (t + PREF_BUCKET_AHEAD < len)
                HT_PREFETCH(wv[t + PREF_BUCKET_AHEAD].cur, /*read*/ 0, PREF_BUCKET_LOCALITY);

            // // prefetch a future head node itself (small lookahead)
            // if (t + HEAD_PF_AHEAD < len) {
            //     // load the pointer early, then prefetch that node
            //     Node* hpf = *wv[t + HEAD_PF_AHEAD].slot;
            //     if (hpf)
            //         HT_PREFETCH(hpf, 0, PREF_HEAD_LOCALITY);
            // }

            Work w = wv[t];   // pull k/v/slot into registers
            Node* h = *w.cur; // current head

            if (!h) {
                Node* node = alloc_node();
                // HT_PREFETCH(node, /*write*/1, PREF_LOCALITY);
                new (node) Node{h, w.k, w.v};
                *w.cur = node;
                ++new_count;
                continue;
            }
            if (h->k == w.k) {
                h->v = w.v;
                continue;
            }

            // needs traversal => emit compacted Work for the walk
            w.cur = &h->next;
            wv[wn++] = w;
        }
        return wn;
    }

    inline void walk_compact_(uint32_t& wn, size_t& new_count) {
        auto* __restrict wv = work_.data();
        wv = (Work* __restrict)__builtin_assume_aligned(wv, 64);

        while (wn) {
            uint32_t write = 0;

            for (uint32_t j = 0; j < wn; ++j) {
                Work w = wv[j];      // pull fields into registers
                Node** link = w.cur; // current link (pointer-to-pointer)
                Node* p = *link;     // current node

                // Queue-ahead prefetch (now contiguous)
                if (j + PREF_QUEUE_AHEAD < wn) {
                    if (Node* cpf = *wv[j + PREF_QUEUE_AHEAD].cur)
                        HT_PREFETCH(cpf, /*read*/ 0, PREF_QUEUE_LOCALITY);
                }

                if (p) {
                    HT_PREFETCH(p->next, /*read*/ 0, PREF_LINK_LOCALITY); // link-ahead

                    if (p->k != w.k) {
                        w.cur = &p->next; // advance one hop
                        wv[write++] = w;  // still active => keep at front
                    } else {
                        p->v = w.v; // hit => done
                    }
                } else {
                    // tail reached: append new node here
                    Node* node = alloc_node();
                    new (node) Node{nullptr, w.k, w.v};
                    *link = node;
                    ++new_count;
                }
            }

            wn = write; // compact survivors; next round gets smaller
        }
    }


    __attribute__((always_inline)) inline void process_batch_full() {
        size_t new_count = 0;
        uint32_t wn = seed_build_(BATCH_SIZE, new_count);
        walk_compact_(wn, new_count);
        sz += new_count; // single size update
    }

    inline void process_batch_var(size_t len) {
        size_t new_count = 0;
        uint32_t wn = seed_build_(len, new_count); // same seed; runtime len
        walk_compact_(wn, new_count);
        sz += new_count;
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

                // prepend to free_list
                cur->next = free_list;
                free_list = cur;

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
};
