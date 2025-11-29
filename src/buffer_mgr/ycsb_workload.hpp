#pragma once
#include "tpcc/random_generator.hpp"
#include "tpcc/types.hpp"
#include "utils/literals.hpp"
#include "utils/my_logger.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/utils.hpp"


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

public:
    YCSBWorkload(AdapterType<ycsb_t>& t,
                 uint64_t tuple_count,
                 int read_ratio = 100)
        : table(t),
          tuple_count(tuple_count),
          read_ratio(read_ratio) {
    }


    void loadTable() {
        for (uint64_t i = 0; i < tuple_count; ++i) {
            ycsb_t record;
            RandomGenerator::getRandString(reinterpret_cast<u8*>(record.value.value), YCSB_LEN);
            table.insert({i}, record);
        }

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
        table.update1({key}, [&](ycsb_t& rec) {
            RandomGenerator::getRandString(reinterpret_cast<u8*>(rec.value.value), YCSB_LEN);
            do_not_optimize(rec);
        });
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
};
