#pragma once
#include "tpcc/random_generator.hpp"
#include "utils/zipf.hpp"
#include "tpcc/types.hpp"
#include "utils/literals.hpp"
#include "utils/my_logger.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/utils.hpp"

#include <memory>
#include <random>


constexpr size_t YCSB_LEN = 128;


template <u64 size>
struct BytesPayload {
    u8 value[size];

    BytesPayload() = default;

    bool operator==(BytesPayload& other) {
        return (std::memcmp(value, other.value, sizeof(value)) == 0);
    }
    bool operator!=(BytesPayload& other) {
        return !(operator==(other));
    }
};

struct ycsb_t {
    static constexpr int id = 0;
    struct Key {
        static constexpr int id = 0;
        uint64_t key;
    };
    BytesPayload<YCSB_LEN> value;

    template <class T>
    static unsigned foldKey(uint8_t* out, const T& record) {
        unsigned pos = 0;
        pos += fold(out + pos, record.key);
        return pos;
    }
    template <class T>
    static unsigned unfoldKey(const uint8_t* in, T& record) {
        unsigned pos = 0;
        pos += unfold(in + pos, record.key);
        return pos;
    }
    static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::key); };
};


template <template <typename> class AdapterType>
struct YCSBWorkload {

    AdapterType<ycsb_t>& table;

    uint64_t tuple_count;
    int read_ratio;
    double zipf_theta;

public:
    YCSBWorkload(AdapterType<ycsb_t>& t,
                 uint64_t tuple_count,
                 int read_ratio = 100,
                 double zipf_theta = 0.0)
        : table(t),
          tuple_count(tuple_count),
          read_ratio(read_ratio),
          zipf_theta(zipf_theta) {
    }

    void loadRange(uint64_t begin, uint64_t end) {
        for (uint64_t i = begin; i < end; ++i) {
            ycsb_t record;
            RandomGenerator::getRandString(reinterpret_cast<u8*>(record.value.value), YCSB_LEN);
            table.insert({i}, record);
        }
    }


    void loadTable() {
        loadRange(0, tuple_count);
        Logger::info("loaded ", tuple_count, " tuples");
    }


    void read(uint64_t key) {
        table.lookup1({key}, [&](const ycsb_t& rec) {
            do_not_optimize(rec);
        });
    }


    uint64_t outlier = 0;
    uint64_t total = 0;
    std::array<uint64_t, 256> hist;

    void write(uint64_t key) {

        // RDTSCClock clock(2.4_GHz);
        // clock.start();

        table.update1({key}, [&](ycsb_t& rec) {
            RandomGenerator::getRandString(reinterpret_cast<u8*>(rec.value.value), YCSB_LEN);
            do_not_optimize(rec);
        });

        // clock.stop();

        // uint64_t slot = clock.cycles() / 1000;
        // if (slot < hist.size()) {
        //     hist.at(slot) += 1;
        //     ++total;
        // } else {
        //     ++outlier;
        // }

        // static int cntr = 0;
        // if (cntr++ == 10000) {
        //     for (size_t i = 0; i < hist.size(); ++i) {
        //         if (hist.at(i) > 0)
        //             Logger::info("<", i, "K cycles count=", hist.at(i), " %=", hist.at(i) / static_cast<double>(total));
        //         hist.at(i) = 0;
        //     }
        //     Logger::info("outlier=", outlier);
        //     outlier = 0;
        //     total = 0;

        //    cntr = 0;
        //}
    }

    int tx() {
        uint64_t key = RandomGenerator::getRand(uint64_t{0}, tuple_count);
        u64 rnd = RandomGenerator::getRand(0, 100);

        if (rnd <= read_ratio) {
            read(key);
            return 0;
        }
        write(key);
        return 1;
    }

    int tx_skew() {
        struct ThreadZipfState {
            uint64_t tuple_count = 0;
            double theta = 0.0;
            std::mt19937 rng;
            std::unique_ptr<zipf_distribution<uint64_t, double>> dist;

            ThreadZipfState(uint64_t count, double q)
                : tuple_count(count),
                  theta(q),
                  rng(static_cast<std::mt19937::result_type>(RandomGenerator::getRandU64())),
                  dist(std::make_unique<zipf_distribution<uint64_t, double>>(count, q)) {
            }
        };

        static thread_local std::unique_ptr<ThreadZipfState> zipf_state;
        if (!zipf_state || zipf_state->tuple_count != tuple_count || zipf_state->theta != zipf_theta) {
            zipf_state = std::make_unique<ThreadZipfState>(tuple_count, zipf_theta);
        }

        const uint64_t key = (*zipf_state->dist)(zipf_state->rng) - 1;
        const u64 rnd = RandomGenerator::getRand(0, 100);

        if (rnd <= read_ratio) {
            read(key);
            return 0;
        }
        write(key);
        return 1;
    }
};
