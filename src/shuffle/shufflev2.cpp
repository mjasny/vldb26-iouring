#include "shuffle/mini_alloc.hpp"
#include "shuffle/utils.hpp"
#include "shuffle/zc_recv_helper.hpp"
#include "types.hpp"
#include "utils/cli_parser.hpp"
#include "utils/cpu_map.hpp"
#include "utils/hashtable.hpp"
#include "utils/hugepages.hpp"
#include "utils/jmp.hpp"
#include "utils/literals.hpp"
#include "utils/my_logger.hpp"
#include "utils/perfevent.hpp"
#include "utils/random.hpp"
#include "utils/range_helper.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/small_pages.hpp"
#include "utils/socket.hpp"
#include "utils/stack.hpp"
#include "utils/stats_printer.hpp"
#include "utils/tagged_pointer.hpp"
#include "utils/threadpool.hpp"
#include "utils/types.hpp"
#include "utils/utils.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <liburing.h>
#include <memory>
#include <pthread.h>
#include <ratio>
#include <span>
#include <thread>
#include <utility>
#include <vector>

namespace jmp {
std::istream& operator>>(std::istream& is, const static_branch<bool>& v) {
    bool parsed;
    is >> std::boolalpha >> parsed;
    v = parsed;
    return is;
}
} // namespace jmp

static constexpr size_t MAX_PARTITIONS = 8;
static constexpr size_t MAX_CONNS = 8;

struct Config : Singleton<Config> {
    int core_id = 7;
    size_t tuple_size = 128;
    size_t scan_size = 100_GiB;
    bool perfevent = false;

    uint64_t partitions;
    // static constexpr jmp::static_branch<bool> use_budget = false;
    bool use_budget = false;
    int num_workers = 1;
    bool use_hashtable = false;
    bool same_irq = true;
    uint8_t nr_conns = 1; // connections per partition
    double hashtable_factor = 1.5;

    std::vector<std::string> ips;
    uint16_t port = 1234;
    uint32_t my_id;
    SetupMode setup_mode = SetupMode::DEFER_TASKRUN;
    bool reg_ring = false;
    bool reg_bufs = false;
    bool reg_fds = false;
    bool send_zc = false;

    bool recv_zc = false;
    std::string ifname;
    bool use_epoll = false;

    bool pin_queues = false;
    bool napi = false;
    uint64_t stats_interval = 1'000'000; // microseconds

    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--tuple_size", tuple_size, cli::Parser::optional);
        parser.parse("--scan_size", scan_size, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--num_workers", num_workers, cli::Parser::optional);
        parser.parse("--nr_conns", nr_conns, cli::Parser::optional);
        parser.parse("--hashtable_factor", hashtable_factor, cli::Parser::optional);

        parser.parse("--ips", ips);
        parser.parse("--port", port, cli::Parser::optional);
        parser.parse("--my_id", my_id);
        parser.parse("--setup_mode", setup_mode, cli::Parser::optional);
        parser.parse("--reg_ring", reg_ring, cli::Parser::optional);
        parser.parse("--reg_bufs", reg_bufs, cli::Parser::optional);
        parser.parse("--reg_fds", reg_fds, cli::Parser::optional);
        parser.parse("--send_zc", send_zc, cli::Parser::optional);
        parser.parse("--pin_queues", pin_queues, cli::Parser::optional);
        parser.parse("--napi", napi, cli::Parser::optional);

        parser.parse("--recv_zc", recv_zc, cli::Parser::optional);
        parser.parse("--ifname", ifname, cli::Parser::optional);
        parser.parse("--use_epoll", use_epoll, cli::Parser::optional);

        parser.parse("--use_budget", use_budget, cli::Parser::optional);
        parser.parse("--use_hashtable", use_hashtable, cli::Parser::optional);
        parser.parse("--same_irq", same_irq, cli::Parser::optional);

        parser.parse("--stats_interval", stats_interval, cli::Parser::optional);

        parser.check_unparsed();
        parser.print();

        if (reg_bufs) {
            ensure(send_zc);
        }

        ensure(my_id < ips.size());
        ensure(partitions <= MAX_PARTITIONS);
        ensure(nr_conns <= MAX_CONNS);
        ensure(tuple_size >= sizeof(Tuple<8>));

        if (recv_zc) {
            ensure(pin_queues);
            ensure(ifname.size() > 0);
        }

        partitions = ips.size();
    }
};

struct WorkerPinInfo {
    int core_id;
    int tx_queue;
    int rx_queue;
};

std::array<WorkerPinInfo, 32> pin_info_seq = {{
    {.core_id = 8, .tx_queue = 8, .rx_queue = 12},
    {.core_id = 9, .tx_queue = 9, .rx_queue = 13},
    {.core_id = 10, .tx_queue = 10, .rx_queue = 14},
    {.core_id = 11, .tx_queue = 11, .rx_queue = 15},

    {.core_id = 16, .tx_queue = 16, .rx_queue = 20},
    {.core_id = 17, .tx_queue = 17, .rx_queue = 21},
    {.core_id = 18, .tx_queue = 18, .rx_queue = 22},
    {.core_id = 19, .tx_queue = 19, .rx_queue = 23},

    {.core_id = 24, .tx_queue = 24, .rx_queue = 28},
    {.core_id = 25, .tx_queue = 25, .rx_queue = 29},
    {.core_id = 26, .tx_queue = 26, .rx_queue = 30},
    {.core_id = 27, .tx_queue = 27, .rx_queue = 31},

    {.core_id = 32, .tx_queue = 32, .rx_queue = 36},
    {.core_id = 33, .tx_queue = 33, .rx_queue = 37},
    {.core_id = 34, .tx_queue = 34, .rx_queue = 38},
    {.core_id = 35, .tx_queue = 35, .rx_queue = 39},

    {.core_id = 40, .tx_queue = 40, .rx_queue = 44},
    {.core_id = 41, .tx_queue = 41, .rx_queue = 45},
    {.core_id = 42, .tx_queue = 42, .rx_queue = 46},
    {.core_id = 43, .tx_queue = 43, .rx_queue = 47},

    {.core_id = 48, .tx_queue = 48, .rx_queue = 52},
    {.core_id = 49, .tx_queue = 49, .rx_queue = 53},
    {.core_id = 50, .tx_queue = 50, .rx_queue = 54},
    {.core_id = 51, .tx_queue = 51, .rx_queue = 55},

    {.core_id = 56, .tx_queue = 56, .rx_queue = 60},
    {.core_id = 57, .tx_queue = 57, .rx_queue = 61},
    {.core_id = 58, .tx_queue = 58, .rx_queue = 62},
    //{.core_id = 59, .tx_queue = 59, .rx_queue = 63},

    {.core_id = 0, .tx_queue = 0, .rx_queue = 4},
    {.core_id = 1, .tx_queue = 1, .rx_queue = 5},
    {.core_id = 2, .tx_queue = 2, .rx_queue = 6},
    {.core_id = 3, .tx_queue = 3, .rx_queue = 7},

    {.core_id = 59, .tx_queue = 59, .rx_queue = 62},
}};

// IRQs are remapped to all 12 chiplets
std::array<WorkerPinInfo, 32> pin_info_rr = {{
    {.core_id = 2, .tx_queue = 2, .rx_queue = 3},
    {.core_id = 4, .tx_queue = 4, .rx_queue = 5},
    {.core_id = 8, .tx_queue = 6, .rx_queue = 7},
    {.core_id = 10, .tx_queue = 8, .rx_queue = 9},
    {.core_id = 12, .tx_queue = 10, .rx_queue = 11},
    {.core_id = 16, .tx_queue = 12, .rx_queue = 13},
    {.core_id = 18, .tx_queue = 14, .rx_queue = 15},
    {.core_id = 20, .tx_queue = 16, .rx_queue = 17},
    {.core_id = 24, .tx_queue = 18, .rx_queue = 19},
    {.core_id = 26, .tx_queue = 20, .rx_queue = 21},
    {.core_id = 28, .tx_queue = 22, .rx_queue = 23},
    {.core_id = 32, .tx_queue = 24, .rx_queue = 25},
    {.core_id = 34, .tx_queue = 26, .rx_queue = 27},
    {.core_id = 36, .tx_queue = 28, .rx_queue = 29},
    {.core_id = 40, .tx_queue = 30, .rx_queue = 31},
    {.core_id = 42, .tx_queue = 32, .rx_queue = 33},
    {.core_id = 44, .tx_queue = 34, .rx_queue = 35},
    {.core_id = 48, .tx_queue = 36, .rx_queue = 37},
    {.core_id = 50, .tx_queue = 38, .rx_queue = 39},
    {.core_id = 52, .tx_queue = 40, .rx_queue = 41},
    {.core_id = 56, .tx_queue = 42, .rx_queue = 43},
    {.core_id = 58, .tx_queue = 44, .rx_queue = 45},
    {.core_id = 60, .tx_queue = 46, .rx_queue = 47},
    {.core_id = 64, .tx_queue = 48, .rx_queue = 49},
    {.core_id = 66, .tx_queue = 50, .rx_queue = 51},
    {.core_id = 72, .tx_queue = 52, .rx_queue = 53},
    {.core_id = 74, .tx_queue = 54, .rx_queue = 55},
    {.core_id = 80, .tx_queue = 56, .rx_queue = 57},
    {.core_id = 82, .tx_queue = 58, .rx_queue = 59},
    {.core_id = 88, .tx_queue = 60, .rx_queue = 61},
    {.core_id = 90, .tx_queue = 62, .rx_queue = 62},
    {.core_id = 0, .tx_queue = 0, .rx_queue = 1},
}};

auto& pin_info = pin_info_rr;

// template <typename T, size_t size = 1_KiB>
template <typename T, size_t size = 1_MiB>
struct OutputBuffer {
    static constexpr size_t SIZE = size;
    static constexpr size_t max = size / sizeof(T);
    int buf_idx = 0;
    uint64_t idx = 0;
    T data[max];

    inline T* get_slot() {
        ensure(!full());
        return &data[idx++];
    }

    inline bool full() { return idx == max; }

    inline void clear() { idx = 0; }

    inline T* begin() noexcept { return data; }
    inline T* end() noexcept { return data + max; }

    inline const T* begin() const noexcept { return data; }
    inline const T* end() const noexcept { return data + max; }
};

template <size_t tuple_size>
struct MorselIterator {
    using tuple_t = Tuple<tuple_size>;

    static constexpr size_t MORSEL_SIZE = 128_MiB;
    static constexpr size_t tuples_per_morsel = MORSEL_SIZE / sizeof(tuple_t);

    tuple_t* tuples;
    const size_t n_tuples;

    std::atomic<uint64_t> offset = 0;

    MorselIterator(tuple_t* tuples, size_t n_tuples)
        : tuples(tuples), n_tuples(n_tuples) {}

    std::span<tuple_t> next() {
        auto start = offset.fetch_add(
            tuples_per_morsel,
            std::memory_order_relaxed); // value preceding add operation
        // start %= n_tuples;
        if (start >= n_tuples) {
            return {};
        }

        auto end = std::min(start + tuples_per_morsel, n_tuples);
        return std::span(tuples + start, tuples + end);
    }

    double progress() const {
        return double(offset.load(std::memory_order::relaxed)) / double(n_tuples);
    }
};

template <std::size_t tuple_size>
class TupleIterator {
    static_assert(tuple_size >= 8, "tuple_size must be at least 8 bytes");
    static constexpr std::size_t key_size = 8;

public:
    // key_offset: absolute offset (from stream start) of the first key
    explicit TupleIterator(std::size_t key_offset = 0)
        : next_key_pos_(key_offset) {}

    // Feed one chunk. The callback is invoked for each extracted key.
    // on_key must be callable as: void(std::uint64_t)
    template <class OnKey>
    void process(const void* chunk, std::size_t len, OnKey&& on_key) {
        const std::uint8_t* p = static_cast<const std::uint8_t*>(chunk);

        // 1) complete a partially assembled key (if any)
        if (partial_filled_ != 0) {
            std::size_t need = key_size - partial_filled_;
            std::size_t take = (len < need ? len : need);
            std::memcpy(partial_.data() + partial_filled_, p, take);
            partial_filled_ += take;
            p += take;
            len -= take;
            stream_pos_ += take;

            if (partial_filled_ == key_size) {
                emit_from(partial_.data(), std::forward<OnKey>(on_key));
                partial_filled_ = 0;
                next_key_pos_ += tuple_size;
            }
        }

        // 2) main loop
        while (len > 0) {
            // No key starts in the remainder of this chunk
            if (stream_pos_ + len <= next_key_pos_) {
                stream_pos_ += len;
                return;
            }

            // Skip up to the next key start
            if (stream_pos_ < next_key_pos_) {
                std::size_t skip = next_key_pos_ - stream_pos_;
                std::size_t take = (len < skip ? len : skip);
                p += take;
                len -= take;
                stream_pos_ += take;
                if (len == 0)
                    break;
            }

            // At key start
            if (len >= key_size) {
                std::uint64_t key;
                std::memcpy(&key, p, key_size); // portable & optimized
                on_key(key);

                p += key_size;
                len -= key_size;
                stream_pos_ += key_size;
                next_key_pos_ += tuple_size;

                // Skip gap between key end and next key start (if present)
                constexpr std::size_t gap = tuple_size - key_size;
                std::size_t take = (len < gap ? len : gap);
                p += take;
                len -= take;
                stream_pos_ += take;
            } else {
                // Key straddles boundary: buffer what we have
                std::memcpy(partial_.data(), p, len);
                partial_filled_ = len;
                stream_pos_ += len;
                return;
            }
        }
    }

private:
    template <class OnKey>
    static void emit_from(const void* ptr, OnKey&& on_key) {
        std::uint64_t key;
        std::memcpy(&key, ptr, key_size);
        on_key(key);
    }

    // State
    std::size_t stream_pos_ = 0;                   // absolute position in stream
    std::size_t next_key_pos_;                     // absolute offset of next key
    std::array<std::uint8_t, key_size> partial_{}; // for boundary-spanning keys
    std::size_t partial_filled_ = 0;
};

template <size_t tuple_size>
struct IWorker {
    int wid;

    Config cfg;

    uint64_t bytes_sent = 0;
    uint64_t bytes_recv = 0;
    uint64_t io_cycles = 0;

    IWorker(int wid) : wid(wid) { cfg = Config::get(); }

    virtual ~IWorker() = default;

    virtual void init() = 0;
    virtual void deinit() = 0;

    virtual void run(MorselIterator<tuple_size>& morsel_it) = 0;

    static constexpr bool MEASURE_IO_CYCLES = true;
    RDTSCClock io_clock = RDTSCClock(2.4_GHz);

    void io_begin() {
        if constexpr (MEASURE_IO_CYCLES) {
            io_clock.start();
        }
    }

    void io_end() {
        if constexpr (MEASURE_IO_CYCLES) {
            io_clock.stop();
            io_cycles += io_clock.cycles();
        }
    }
};

template <size_t tuple_size>
struct alignas(64) Worker : IWorker<tuple_size> {
    using Base = IWorker<tuple_size>;
    using tuple_t = Tuple<tuple_size>;

    // this is due to the templated Base
    using Base::bytes_recv;
    using Base::bytes_sent;
    using Base::cfg;
    using Base::io_begin;
    using Base::io_end;
    using Base::wid;

    struct io_uring ring;
    int server_fd;
    int outstanding = 0;

    static constexpr size_t num_buffers = MAX_PARTITIONS * (1 + 2 * MAX_CONNS);
    using Buffer = OutputBuffer<tuple_t, 1_MiB>;
    std::unique_ptr<Buffer[]> buffers;
    Stack<Buffer*, num_buffers> unused_buffers;

    enum {
        SEND_TAG,
        RECV_TAG,
        IGNR_TAG,
    };
    struct UserData {
        union {
            struct {
                uint8_t tag;
                uint8_t conn_id;
                uint32_t target_id;
            };
            uint64_t val;
        };

        operator uint64_t() const { return val; }

        static UserData from_u64(uint64_t val) { return UserData{.val = val}; }
    };
    static_assert(sizeof(UserData) == 8);

    struct Target {
        int budget = 2;

        Buffer* fill_buffer = nullptr;

        struct Connection {
            int fd = -1;
            bool done;
            Buffer* send_buffer = nullptr;
            Buffer* recv_buffer = nullptr;
            size_t last_bytes = 0;
            TupleIterator<sizeof(tuple_t)> ex;
        };
        std::array<Connection, MAX_CONNS> conns;
    };
    std::array<Target, MAX_PARTITIONS> part_to_target;

    std::vector<int> fds_to_close;

    using Hashtable = ChainedHT<tuple_t*>;
    std::unique_ptr<Hashtable> probe_table;

    ZCRecvHelper zcrcv;

    Worker(int id) : Base(id) {}

    void deinit() override {
        for (auto& fd : fds_to_close) {
            close(fd);
        }
        Logger::info("closed ", fds_to_close.size(), " fds");
    }

    void init() override {
        std::string name = "Worker-" + std::to_string(wid);
        check_zero(pthread_setname_np(pthread_self(), name.c_str()));

        struct io_uring_params params;
        memset(&params, 0, sizeof(params));
        params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CLAMP;
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = 131072;
        if (cfg.setup_mode == SetupMode::DEFER_TASKRUN) {
            params.flags |= IORING_SETUP_DEFER_TASKRUN;
        }
        if (cfg.setup_mode == SetupMode::SQPOLL) {
            params.flags |= IORING_SETUP_SQPOLL;
            params.sq_thread_idle = 1000;
            ensure(false);
            // params.sq_thread_cpu = core_id + 1;
            params.flags |= IORING_SETUP_SQ_AFF;
        }
        if (cfg.setup_mode == SetupMode::COOP_TASKRUN) {
            params.flags |= IORING_SETUP_COOP_TASKRUN;
        }
        if (cfg.recv_zc) {
            params.flags |= IORING_SETUP_CQE32;
        }
        if (io_uring_queue_init_params(4096, &ring, &params) < 0) {
            fprintf(stderr, "%s\n", strerror(errno));
            exit(1);
        }

        if (cfg.reg_ring) {
            if (!(ring.features & IORING_FEAT_REG_REG_RING)) {
                Logger::error("IORING_FEAT_REG_REG_RING not supported");
                exit(1);
            }
            ensure(io_uring_register_ring_fd(&ring) == 1);
            Logger::info("registered ring fd");
        }

        buffers = std::make_unique<Buffer[]>(num_buffers);
        for (size_t i = 0; i < num_buffers; ++i) {
            unused_buffers.push(&buffers[i]);
        }

        if (cfg.reg_bufs) {
            auto iovs = std::vector<struct iovec>(num_buffers);
            for (int i = 0; i < num_buffers; ++i) {
                auto& buffer = buffers[i];
                iovs[i].iov_base = buffer.data;
                iovs[i].iov_len = Buffer::SIZE;
                buffer.buf_idx = i;
            }
            check_iou(io_uring_register_buffers(&ring, iovs.data(), iovs.size()));
            Logger::info("registered buffer");

            // br = std::make_unique<BufRing>(&ring, /*nr_bufs=*/64, MORSEL_SIZE,
            // /*incremental=*/true); Logger::info("bufring ", br->avail());
        }

        const int reg_fd_slots = 1 + cfg.ips.size() * cfg.nr_conns;
        if (cfg.reg_fds) {
            check_iou(io_uring_register_files_sparse(&ring, 1 + cfg.ips.size() *
                                                                    cfg.nr_conns));
        }

        auto my_ip = cfg.ips.at(cfg.my_id);
        server_fd = listen_on(my_ip.c_str(), cfg.port + wid, 1024);
        fds_to_close.push_back(server_fd);

        if (cfg.napi) {
            struct io_uring_napi napi = {};
            napi.prefer_busy_poll = 1;
            napi.busy_poll_to = 50;
            check_iou(io_uring_register_napi(&ring, &napi));
            Logger::info("enabled napi");
        }

        static std::mutex mutex;

        if (cfg.recv_zc) {
            int rx_queue = pin_info.at(wid).rx_queue;
            Logger::info("wid=", wid, " rx_queue=", rx_queue);

            static std::atomic<uint64_t> ready = 0;
            {
                const std::lock_guard<std::mutex> lock(mutex);
                zcrcv.setup(&ring, cfg.ifname.c_str(), rx_queue);
                auto x = ++ready;
                Logger::info("recv_zc init done ready=", x);
                Logger::flush();
            }

            while (ready != cfg.num_workers)
                ;

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        auto config_fd = [&](int fd) {
            if (cfg.use_budget) {
                return;
            }
            //  int64_t lowat = 64 * 1024; // try 32–256 KiB
            int64_t lowat = 8 * 1024 * 1024;
            check_ret(setsockopt(fd, IPPROTO_TCP, TCP_NOTSENT_LOWAT, &lowat,
                                 sizeof(lowat)));

            return;

            int busy = 50;
            check_ret(setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL, &busy, sizeof(busy)));
            int prefer = 1;
            check_ret(setsockopt(fd, SOL_SOCKET, SO_PREFER_BUSY_POLL, &prefer,
                                 sizeof(prefer)));

            int snd = 512 * 1024 * 1024;
            check_ret(setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd)));
            int rcv = 4 * 1024 * 1024; // room for 1 MiB + headroom
            check_ret(setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv)));
        };

        for (int i = cfg.my_id + 1; i < cfg.ips.size(); ++i) {
            for (uint8_t conn = 0; conn < cfg.nr_conns; ++conn) {
                Logger::info("connecting to: ", i, " ", conn);
                auto ip = cfg.ips.at(i);

                int retries = 100; // 10 secs
                int fd = connect_to(ip.c_str(), cfg.port + wid, retries, 100'000);
                set_nodelay(fd);

                ensure(send(fd, &cfg.my_id, sizeof(uint32_t), MSG_WAITALL) ==
                       sizeof(int));
                part_to_target.at(i).conns.at(conn).fd = fd;

                // currently only sending
                if (cfg.pin_queues) {
                    int tx_queue = pin_info.at(wid).tx_queue;
                    const std::lock_guard<std::mutex> lock(mutex);
                    assign_flow_to_rx_queue(fd, tx_queue);
                    // std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        }

        std::array<uint32_t, MAX_PARTITIONS> conn_idx;
        conn_idx.fill(0);
        for (int i = 0; i < cfg.my_id; ++i) {
            for (uint8_t conn = 0; conn < cfg.nr_conns; ++conn) {
                Logger::info("waiting for: ", i, " ", conn);
                int fd = accept(server_fd, nullptr, nullptr);
                check_ret(fd);
                set_cloexec(fd);
                set_nodelay(fd);

                uint32_t remote_id = -1;
                ensure(recv(fd, &remote_id, sizeof(uint32_t), MSG_WAITALL) ==
                       sizeof(int));
                Logger::info("remote_id=", remote_id);
                ensure(remote_id < cfg.ips.size());
                ensure(remote_id != cfg.my_id);
                auto c_idx = conn_idx[remote_id]++;
                ensure(c_idx < cfg.nr_conns);
                part_to_target.at(remote_id).conns.at(c_idx).fd = fd;

                // currently only receiving
                if (cfg.pin_queues) {
                    int rx_queue = pin_info.at(wid).rx_queue;
                    const std::lock_guard<std::mutex> lock(mutex);
                    assign_flow_to_rx_queue(fd, rx_queue);
                    // std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        }

        if (cfg.reg_fds) {
            check_iou(
                io_uring_register_files_update(&ring, /*off*/ 0, &server_fd, 1));
            server_fd = 0;
        }

        int offset = 1;
        auto reg_fd = [&](int& fd) {
            fds_to_close.push_back(fd);
            config_fd(fd);
            if (cfg.reg_fds) {
                check_iou(io_uring_register_files_update(&ring, offset, &fd, 1));
                fd = offset++;
                ensure(offset <= reg_fd_slots);
            }
        };
        for (size_t i = 0; i < cfg.ips.size(); ++i) {
            if (i == cfg.my_id) {
                continue;
            }
            for (uint8_t conn = 0; conn < cfg.nr_conns; ++conn) {
                auto& fd = part_to_target.at(i).conns.at(conn).fd;
                ensure(fd != 0);
                Logger::info("fd=", fd);
                ensure(fd != -1);
                reg_fd(fd);
            }
        }

        for (size_t i = 0; i < cfg.ips.size(); ++i) {
            if (i == cfg.my_id) {
                continue;
            }
            for (uint8_t conn = 0; conn < cfg.nr_conns; ++conn) {
                prep_recv(i, conn);
            }
        }
        io_uring_submit(&ring);

        if (cfg.use_hashtable) {
            const auto n_tuples = cfg.scan_size / tuple_size / cfg.num_workers;
            const auto capacity = next_pow2(n_tuples * cfg.hashtable_factor);
            probe_table = std::make_unique<Hashtable>(capacity);
        }

        Logger::info("init done ", wid);
    }

    void prep_recv(uint32_t target_id, uint8_t conn_id) {
        auto& conn = part_to_target[target_id].conns[conn_id];

        // Logger::info("prep_recv Target: ", target_id);

        auto sqe = io_uring_get_sqe(&ring);
        check_ptr(sqe);

        if (cfg.recv_zc) {
            zcrcv.prep_recv_zc(sqe, conn.fd, 0);
            // zcrcv.prep_recv_zc(sqe, conn.fd, Buffer::SIZE);
        } else {
            auto& buffer = conn.recv_buffer;
            ensure(!buffer);
            buffer = unused_buffers.pop();

            io_uring_prep_recv(sqe, conn.fd, buffer->data, Buffer::SIZE, MSG_WAITALL);
        }

        // if (cfg.recv_buf) {
        //     br->set_bg(sqe);
        //     // sqe->ioprio |= IORING_RECVSEND_BUNDLE;
        // }
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }

        UserData ud{.tag = RECV_TAG, .conn_id = conn_id, .target_id = target_id};
        io_uring_sqe_set_data64(sqe, ud);

        // Logger::info("recv target.fd=", target.fd);

        ++outstanding;
    }

    void prep_send(uint32_t target_id, uint8_t conn_id) {
        auto& conn = part_to_target[target_id].conns[conn_id];

        // Logger::info("prep_send Target: ", target_id);

        auto sqe = io_uring_get_sqe(&ring);
        check_ptr(sqe);

        if (cfg.send_zc) {
            if (cfg.reg_bufs) {
                io_uring_prep_send_zc_fixed(sqe, conn.fd, conn.send_buffer->data,
                                            Buffer::SIZE, MSG_WAITALL, 0,
                                            /*buf_index=*/conn.send_buffer->buf_idx);
            } else {
                io_uring_prep_send_zc(sqe, conn.fd, conn.send_buffer->data,
                                      Buffer::SIZE, MSG_WAITALL, 0);
            }

        } else {
            io_uring_prep_send(sqe, conn.fd, conn.send_buffer->data, Buffer::SIZE,
                               MSG_WAITALL);
        }
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }

        // sqe->flags |= IOSQE_ASYNC;
        UserData ud{.tag = SEND_TAG, .conn_id = conn_id, .target_id = target_id};
        io_uring_sqe_set_data64(sqe, ud);

        // Logger::info("sending");
        ++outstanding;
    }

    void prep_shutdown(uint32_t target_id, uint8_t conn_id) {
        auto& conn = part_to_target[target_id].conns[conn_id];

        auto sqe = io_uring_get_sqe(&ring);
        check_ptr(sqe);

        io_uring_prep_shutdown(sqe, conn.fd, SHUT_WR);
        if (cfg.reg_fds) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }

        UserData ud{.tag = IGNR_TAG, .conn_id = conn_id, .target_id = target_id};
        io_uring_sqe_set_data64(sqe, ud);

        ++outstanding;
    }

    void drain_cqe() {
        bool do_submit = false;
        int i = 0;
        uint32_t head;
        struct io_uring_cqe* cqe;
        io_uring_for_each_cqe(&ring, head, cqe) {
            ++i;

            auto ud = UserData::from_u64(io_uring_cqe_get_data64(cqe));

            if (cqe->res < 0) {
                if (cqe->res == -ENOBUFS) {
                    Logger::info("out of bufs");
                } else {
                    Logger::error("CQE Tag: ", ud.tag == SEND_TAG ? "send" : "recv",
                                  " Target: ", ud.target_id);
                    Logger::error("CQE Error: ", std::strerror(-cqe->res));

                    for (uint8_t conn_id = 0; conn_id < cfg.nr_conns; ++conn_id) {
                        auto& target = part_to_target[ud.target_id];
                        auto& conn = target.conns[conn_id];
                        Logger::error("fd=", conn.fd);
                    }

                    if (cfg.recv_zc) {
                        if (cqe->res == -EPIPE || cqe->res == -ECONNRESET) {
                            continue;
                        }
                    }
                    check_iou(cqe->res);
                }
            }

            auto& target = part_to_target[ud.target_id];
            auto& conn = target.conns[ud.conn_id];
            switch (ud.tag) {
                case SEND_TAG: {
                    if (cqe->flags & IORING_CQE_F_NOTIF) {
                        outstanding++;
                        // notification that zc buffer can be re-used
                        break;
                    }
                    //--inflight;
                    if (!cfg.recv_zc) {
                        ensure(cqe->res == Buffer::SIZE);
                    }
                    bytes_sent += Buffer::SIZE;

                    ensure(conn.send_buffer);
                    unused_buffers.push(conn.send_buffer);
                    conn.send_buffer = nullptr;
                    // bytes_sent += cqe->res;
                    break;
                }
                case RECV_TAG: {
                    // ensure(cqe->res == Buffer::SIZE);

                    bytes_recv += cqe->res;

                    if (cfg.recv_zc) {
                        // Logger::info("flags=", cqe->flags, " res=", cqe->res);

                        conn.last_bytes += cqe->res;
                        if (cqe->res == 0) {
                            ensure(cqe->res == 0 && !(cqe->flags & IORING_CQE_F_MORE));

                            // connection close
                            if (conn.last_bytes == 0) {
                                Logger::info("got shutdown wid=", wid, " part=", ud.target_id,
                                             " conn=", +ud.conn_id);
                                conn.done = true;
                                break;
                            }
                            conn.last_bytes = 0;

                            prep_recv(ud.target_id, ud.conn_id);
                            do_submit = true;
                            // Logger::info("zc_recv rearm wid=", wid);
                        } else {
                            ensure((cqe->flags & IORING_CQE_F_MORE));

                            outstanding++;
                            zcrcv.process_recvzc(cqe, [&](void* data, int len) {
                                if (cfg.use_hashtable) {
                                    io_end();
                                    conn.ex.process(data, len, [&](uint64_t key) {
                                        probe_table->insert_batch(key, (tuple_t*)data);
                                        recv_inserts++;
                                    });
                                    io_begin();
                                }
                            });

                            if (cfg.use_budget) {
                                static uint64_t num_chunks = 0;
                                uint64_t chunk_idx = conn.last_bytes % Buffer::SIZE;
                                if (chunk_idx > num_chunks) {
                                    target.budget += 1;
                                    num_chunks = chunk_idx;
                                }
                            }
                        }
                    } else {
                        ensure(conn.recv_buffer);
                        if (cfg.use_hashtable) {
                            io_end();
                            for (auto& tuple : *conn.recv_buffer) {
                                probe_table->insert_batch(tuple.key, &tuple);
                                recv_inserts++;
                            }
                            io_begin();
                        }
                        unused_buffers.push(conn.recv_buffer);
                        conn.recv_buffer = nullptr;

                        // ensure(!conn.done);
                        if (cqe->res == 0) {
                            conn.done = true;
                            break;
                        }

                        if (cfg.use_budget) {
                            target.budget += 1;
                        }
                        prep_recv(ud.target_id, ud.conn_id);
                        do_submit = true;
                    }

                    break;
                }
                case IGNR_TAG:
                    break;
            }
        }
        io_uring_cq_advance(&ring, i);
        outstanding -= i;

        if (do_submit) {
            io_uring_submit(&ring);
        }
    }

    uint64_t scan_inserts = 0;
    uint64_t recv_inserts = 0;

    void run(MorselIterator<tuple_size>& morsel_it) override {

        RDTSCClock clock(2.4_GHz);
        std::unique_ptr<PerfEvent> e;
        if (cfg.perfevent) {
            e = std::make_unique<PerfEvent>();
            e->startCounters();
        }
        clock.start();

        uint64_t n_tuples = 0;
        uint64_t copies = 0;

        uint64_t sents = 0;

        while (true) {
            auto morsel = morsel_it.next();
            if (morsel.empty()) {
                break;
            }
            for (auto& tuple : morsel) {
                uint64_t part_id = tuple.key % cfg.partitions;
                if (part_id != cfg.my_id) {
                    auto& target = part_to_target[part_id];

                    auto& buffer = target.fill_buffer;
                    if (!buffer) [[unlikely]] {
                        buffer = unused_buffers.pop();
                        check_ptr(buffer);
                        buffer->clear();
                    }

                    auto slot = buffer->get_slot();
                    std::memcpy(slot, &tuple, sizeof(tuple_t));
                    if (buffer->full()) [[unlikely]] {

                        io_begin();
                        // find empty connection
                        uint8_t conn_id;
                        while (true) {
                            bool found = false;
                            for (conn_id = 0; conn_id < cfg.nr_conns; ++conn_id) {
                                auto& conn = target.conns[conn_id];
                                if (!conn.send_buffer) {
                                    found = true;
                                    break;
                                }
                            }
                            if (found) {
                                break;
                            }
                            io_uring_get_events(&ring);
                            drain_cqe();
                        }
                        if (cfg.use_budget) {
                            while (target.budget == 0) {
                                io_uring_get_events(&ring);
                                drain_cqe();
                            }
                        }

                        auto& conn = target.conns[conn_id];
                        ensure(!conn.send_buffer);
                        std::swap(buffer, conn.send_buffer);

                        prep_send(part_id, conn_id);
                        sents++;

                        if (cfg.use_budget) {
                            target.budget--;
                            // prep_recv already scheduled in drain_cqe
                        }
                        // io_uring_submit(&ring);
                        io_uring_submit_and_get_events(&ring);
                        drain_cqe();
                        io_end();
                    }

                    ++copies;
                } else {
                    // insert to HT?
                    if (cfg.use_hashtable) {
                        probe_table->insert_batch(tuple.key, &tuple);
                        scan_inserts++;
                    }
                }
                ++n_tuples;
            }

            // morsel done

            io_begin();
            io_uring_get_events(&ring);
            drain_cqe();
            io_end();
        }

        // Logger::info("Scan finalizing");

        RDTSCClock done_clock(2.4_GHz);

        std::array<std::array<bool, MAX_CONNS>, MAX_PARTITIONS> conns_shutdown{};
        while (true) {
            bool done = true;
            for (int part_id = 0; part_id < cfg.partitions; ++part_id) {
                if (part_id == cfg.my_id) {
                    continue;
                }

                // send out all remaining
                auto& target = part_to_target[part_id];
                for (uint8_t conn_id = 0; conn_id < cfg.nr_conns; ++conn_id) {
                    auto& conn = target.conns[conn_id];

                    if (target.fill_buffer && !conn.send_buffer) {
                        std::swap(target.fill_buffer, conn.send_buffer);
                        ensure(!target.fill_buffer);
                        prep_send(part_id, conn_id);
                    }

                    // wait until last send completes
                    if (conn.send_buffer) {
                        done = false;
                    } else {
                        bool& shut = conns_shutdown.at(part_id).at(conn_id);
                        if (!shut) {
                            Logger::info("prep_shutdown wid=", wid, " part=", part_id,
                                         " conn=", +conn_id);
                            if (!cfg.recv_zc) {
                                prep_shutdown(part_id, conn_id);
                            }
                            shut = true;
                        }
                    }
                    if (!conn.done) {
                        done = false;
                    }
                }
            }
            if (done) {
                break;
            }
            io_uring_submit_and_get_events(&ring);
            drain_cqe();

            if (cfg.recv_zc) {
                // Bugfix: somehow zc recv drops shutdowns
                static thread_local uint64_t last_rx = 0;
                static thread_local uint64_t last_tx = 0;
                static thread_local bool clock_running = false;

                if (last_rx == bytes_recv && last_tx == bytes_sent) {
                    if (!clock_running) {
                        done_clock.start();
                        clock_running = true;
                    } else {
                        done_clock.stop();
                        auto passed = done_clock.as<std::chrono::milliseconds, uint64_t>();
                        if (passed > 1000) {
                            Logger::info("Stop after inactivity");
                            break;
                        }
                    }
                } else {
                    clock_running = false;
                }

                last_rx = bytes_recv;
                last_tx = bytes_sent;
            }
        }

        if (!cfg.recv_zc) {
            for (int part_id = 0; part_id < cfg.partitions; ++part_id) {
                if (part_id == cfg.my_id) {
                    continue;
                }
                auto& target = part_to_target[part_id];
                for (uint8_t conn_id = 0; conn_id < cfg.nr_conns; ++conn_id) {
                    auto& conn = target.conns[conn_id];
                    bool& shut = conns_shutdown.at(part_id).at(conn_id);
                    ensure(shut);
                }
            }
        }

        if (cfg.use_hashtable) {
            probe_table->flush_batch();
        }

        Logger::info("outstanding=", outstanding);

        while (outstanding > 0 && !cfg.recv_zc) {
            io_uring_submit_and_get_events(&ring);
            drain_cqe();
        }

        clock.stop();
        auto sec = clock.as<std::chrono::microseconds, uint64_t>() / 1e6;
        if (e) {
            e->stopCounters();
            e->printReport(std::cout, n_tuples);
        }
        Logger::info("Scan took: ", sec, "s");
        Logger::info("n_tuples=", n_tuples);
        // Logger::info("copies=", copies, " ratio=", copies /
        // static_cast<double>(n_tuples)); auto bw = (copies * sizeof(tuple_t)) /
        // sec; Logger::info("copy_bw=", bw, " copy_bw_gib=", bw / (1UL << 30));

        if (cfg.use_hashtable) {
            Logger::info("probe_table=", probe_table->size());
        }
        Logger::info("scans=", scan_inserts, " recvs=", recv_inserts);

        if (cfg.reg_fds) {
            check_iou(io_uring_unregister_files(&ring));
        }
        io_uring_queue_exit(&ring);
    }
};

#include <fcntl.h>
#include <linux/errqueue.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/uio.h>

template <size_t tuple_size>
struct alignas(64) EpollWorker : IWorker<tuple_size> {
    using Base = IWorker<tuple_size>;
    using tuple_t = Tuple<tuple_size>;

    using Base::bytes_recv;
    using Base::bytes_sent;
    using Base::cfg;
    using Base::io_begin;
    using Base::io_end;
    using Base::wid;

    int epoll_fd = -1;
    int server_fd = -1;

    uint64_t sends_called = 0;
    uint64_t sends_eagain = 0;

    static constexpr size_t num_buffers = MAX_PARTITIONS * (1 + 2 * MAX_CONNS);
    using Buffer = OutputBuffer<tuple_t, 1_MiB>;
    std::unique_ptr<Buffer[]> buffers;
    Stack<Buffer*, num_buffers> unused_buffers;

    struct UserDataPacked {
        static uint64_t pack(uint32_t target_id, uint8_t conn_id) {
            return (static_cast<uint64_t>(target_id) << 8) | conn_id;
        }
        static void unpack(uint64_t v, uint32_t& target_id, uint8_t& conn_id) {
            conn_id = static_cast<uint8_t>(v & 0xff);
            target_id = static_cast<uint32_t>(v >> 8);
        }
    };

    struct Target {
        int budget = 2;
        Buffer* fill_buffer = nullptr;

        struct Connection {
            int fd = -1;
            bool done = false;

            // sending state
            Buffer* send_buffer = nullptr;
            size_t send_off = 0;

            // receiving state
            Buffer* recv_buffer = nullptr;
            size_t recv_off = 0;
            size_t last_bytes = 0;

            // zero-copy send support
            bool zc_enabled = false;
            bool out_armed = false;   // whether EPOLLOUT is currently enabled
            bool wr_shutdown = false; // NEW: have we half-closed our write side?

            bool peer_rd_closed = false; // NEW: peer sent FIN (RDHUP/EOF)
            uint32_t cur_mask = 0;       // NEW: what we told epoll last time
        };
        std::array<Connection, MAX_CONNS> conns;
    };
    std::array<Target, MAX_PARTITIONS> part_to_target;

    std::vector<int> fds_to_close;

    using Hashtable = ChainedHT<tuple_t*>;
    std::unique_ptr<Hashtable> probe_table;

    EpollWorker(int id) : Base(id) {}

    void deinit() override {
        close(server_fd);
        if (epoll_fd != -1)
            close(epoll_fd);
        for (auto& fd : fds_to_close)
            close(fd);
        Logger::info("closed ", fds_to_close.size(), " fds");
    }

    // ------- helpers -------

    static void set_nonblock(int fd) {
        int fl = fcntl(fd, F_GETFL, 0);
        ensure(fl != -1);
        ensure(fcntl(fd, F_SETFL, fl | O_NONBLOCK) != -1);
    }

    void ep_ctl(int op, int fd, uint32_t events, uint32_t target_id,
                uint8_t conn_id) {
        struct epoll_event ev{};
        ev.events = events;
        ev.data.u64 = UserDataPacked::pack(target_id, conn_id);
        ensure(epoll_ctl(epoll_fd, op, fd, &ev) == 0);
    }

    void set_mask(uint32_t t, uint8_t c, uint32_t new_mask) {
        auto& conn = part_to_target[t].conns[c];
        if (conn.cur_mask == new_mask)
            return;
        ep_ctl(EPOLL_CTL_MOD, conn.fd, new_mask, t, c);
        conn.cur_mask = new_mask;
        conn.out_armed = (new_mask & EPOLLOUT);
    }

    void want_read(uint32_t t, uint8_t c) {
        set_mask(t, c, EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP);
    }
    void want_write(uint32_t t, uint8_t c) {
        set_mask(t, c, EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLRDHUP);
    }

    // modular sequence compare for 32-bit IDs (handle wrap)
    static inline bool seq_gte(uint32_t a, uint32_t b) {
        return static_cast<int32_t>(a - b) >= 0;
    }

    // Zero-copy send: try to write as much as possible.
    // Returns true if the whole buffer has been queued to the kernel (not
    // necessarily *completed* for ZC).

    bool send_progress(uint32_t target_id, uint8_t conn_id) {
        auto& conn = part_to_target[target_id].conns[conn_id];
        ensure(conn.send_buffer);

        const bool use_zc = (cfg.send_zc && conn.zc_enabled);

        while (conn.send_off < Buffer::SIZE) {
            const char* base =
                reinterpret_cast<char*>(conn.send_buffer->data) + conn.send_off;
            size_t len = Buffer::SIZE - conn.send_off;

            int flags = use_zc ? MSG_ZEROCOPY : 0;
            ssize_t n = ::send(conn.fd, base, len, flags);
            sends_called++;
            if (n > 0) {
                conn.send_off += size_t(n);
                bytes_sent += uint64_t(n);
                continue; // try to push more right now
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                sends_eagain++;
                want_write(target_id, conn_id);
                return false;
            }
            if (n < 0 && (errno == EPIPE || errno == ECONNRESET)) {
                conn.done = true;
                return true; // drop buffer
            }
            check_ret(n);
            ensure(false, "send failed");
            return true;
        }

        // fully queued
        conn.send_off = 0;
        want_read(target_id, conn_id);
        unused_buffers.push(conn.send_buffer);
        conn.send_buffer = nullptr;
        return true;
    }

    // Drain MSG_ZEROCOPY completion notifications and recycle buffers when safe.

    void drain_errqueue(uint32_t target_id, uint8_t conn_id) {
        auto& conn = part_to_target[target_id].conns[conn_id];
        while (true) {
            char cbuf[256], dummy;
            iovec iov{&dummy, sizeof(dummy)};
            msghdr msg{};
            msg.msg_iov = &iov;
            msg.msg_iovlen = 1;
            msg.msg_control = cbuf;
            msg.msg_controllen = sizeof(cbuf);
            ssize_t r = recvmsg(conn.fd, &msg, MSG_ERRQUEUE);
            if (r < 0) {
                if (errno == EAGAIN)
                    break;
                else
                    break;
            }
            // ignore contents; don't touch buffers here
        }
    }

    // Receive as much as possible into a 1 MiB buffer; process when full.
    void recv_progress(uint32_t target_id, uint8_t conn_id) {
        auto& target = part_to_target[target_id];
        auto& conn = target.conns[conn_id];

        if (!conn.recv_buffer) {
            conn.recv_buffer = unused_buffers.pop();
            conn.recv_off = 0;
        }

        while (true) {
            char* base = reinterpret_cast<char*>(conn.recv_buffer->data);
            size_t remain = Buffer::SIZE - conn.recv_off;
            ssize_t n = recv(conn.fd, base + conn.recv_off, remain, 0);
            if (n > 0) {
                conn.recv_off += static_cast<size_t>(n);
                conn.last_bytes += static_cast<size_t>(n);
                bytes_recv += static_cast<uint64_t>(n);

                if (conn.recv_off == Buffer::SIZE) {
                    if (cfg.use_hashtable) {
                        io_end();
                        for (auto& t : *conn.recv_buffer) {
                            probe_table->insert_batch(t.key, &t);
                            recv_inserts++;
                        }
                        io_begin();
                    }
                    unused_buffers.push(conn.recv_buffer);
                    conn.recv_buffer = nullptr;
                    conn.recv_off = 0;
                    if (cfg.use_budget)
                        target.budget += 1;
                    if (!conn.recv_buffer)
                        conn.recv_buffer = unused_buffers.pop();
                }
                continue;
            } else if (n == 0) {
                conn.peer_rd_closed = true; // peer FIN
                if (conn.wr_shutdown)
                    conn.done = true;
                break;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else if (errno == ECONNRESET) {
                conn.done = true;
                break;
            } else {
                check_ret(n);
                ensure(false, "recv failed");
            }
        }
    }

    void process_events() {
        constexpr int MAX_EVENTS = 256;
        struct epoll_event evs[MAX_EVENTS];
        int n = epoll_wait(epoll_fd, evs, MAX_EVENTS, 1);
        if (n <= 0)
            return;
        for (int i = 0; i < n; ++i) {
            uint32_t target_id;
            uint8_t conn_id;
            UserDataPacked::unpack(evs[i].data.u64, target_id, conn_id);
            auto& conn = part_to_target[target_id].conns[conn_id];

            if (evs[i].events & EPOLLERR) {
                drain_errqueue(target_id, conn_id);
                int soerr = 0;
                socklen_t sl = sizeof(soerr);
                if (getsockopt(conn.fd, SOL_SOCKET, SO_ERROR, &soerr, &sl) == 0 &&
                    soerr) {
                    if (soerr == EPIPE || soerr == ECONNRESET)
                        conn.done = true;
                }
            }
            if ((evs[i].events & EPOLLIN) && !conn.done)
                recv_progress(target_id, conn_id);
            if ((evs[i].events & EPOLLOUT) && conn.send_buffer && !conn.done)
                (void)send_progress(target_id, conn_id);

            if (evs[i].events & EPOLLRDHUP) {
                conn.peer_rd_closed = true; // peer half-closed read
                if (conn.wr_shutdown)
                    conn.done = true; // both halves closed now
            }
            if (evs[i].events & EPOLLHUP) {
                conn.done = true; // full hangup/error
            }
        }
    }

    void init() override {
        std::string name = "Worker-" + std::to_string(wid);
        check_zero(pthread_setname_np(pthread_self(), name.c_str()));

        epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        ensure(epoll_fd != -1);

        buffers = std::make_unique<Buffer[]>(num_buffers);
        for (size_t i = 0; i < num_buffers; ++i)
            unused_buffers.push(&buffers[i]);

        auto my_ip = cfg.ips.at(cfg.my_id);
        server_fd = listen_on(my_ip.c_str(), cfg.port + wid, 1024);
        fds_to_close.push_back(server_fd);

        static std::mutex mutex;

        // per-socket config (returns whether SO_ZEROCOPY was enabled)
        auto config_fd = [&](int fd) -> bool {
            if (!cfg.use_budget) {
                int64_t lowat = 8 * 1024 * 1024;
                check_ret(setsockopt(fd, IPPROTO_TCP, TCP_NOTSENT_LOWAT, &lowat,
                                     sizeof(lowat)));
            }

            bool zc_ok = false;
            if (cfg.send_zc) {
                int one = 1;
                if (setsockopt(fd, SOL_SOCKET, SO_ZEROCOPY, &one, sizeof(one)) == 0) {
                    zc_ok = true;
                }
            }

            // int snd = 512 * 1024 * 1024;
            // check_ret(setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd)));
            // int rcv = 4 * 1024 * 1024; // room for 1 MiB + headroom
            // check_ret(setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv)));

            return zc_ok;
        };

        // outgoing conns (to higher index peers)
        for (int i = cfg.my_id + 1; i < cfg.ips.size(); ++i) {
            for (uint8_t conn = 0; conn < cfg.nr_conns; ++conn) {
                auto ip = cfg.ips.at(i);
                int retries = 100;
                int fd = connect_to(ip.c_str(), cfg.port + wid, retries, 100'000);
                set_cloexec(fd);
                set_nodelay(fd);
                ensure(send(fd, &cfg.my_id, sizeof(uint32_t), MSG_WAITALL) ==
                       sizeof(int));
                part_to_target.at(i).conns.at(conn).fd = fd;
                set_nonblock(fd);

                if (cfg.pin_queues) {
                    int tx_queue = pin_info.at(wid).tx_queue;
                    const std::lock_guard<std::mutex> lock(mutex);
                    assign_flow_to_rx_queue(fd, tx_queue);
                }

                bool zc_ok = config_fd(fd);
                part_to_target.at(i).conns.at(conn).zc_enabled = zc_ok;
            }
        }

        // incoming conns (from lower index peers)
        std::array<uint32_t, MAX_PARTITIONS> conn_idx{};
        conn_idx.fill(0);
        for (int i = 0; i < cfg.my_id; ++i) {
            for (uint8_t conn = 0; conn < cfg.nr_conns; ++conn) {
                int fd = accept(server_fd, nullptr, nullptr);
                check_ret(fd);
                set_cloexec(fd);
                set_nodelay(fd);

                uint32_t remote_id = -1;
                ensure(recv(fd, &remote_id, sizeof(uint32_t), MSG_WAITALL) ==
                       sizeof(int));
                auto c_idx = conn_idx[remote_id]++;
                ensure(c_idx < cfg.nr_conns);
                part_to_target.at(remote_id).conns.at(c_idx).fd = fd;
                set_nonblock(fd);

                if (cfg.pin_queues) {
                    int rx_queue = pin_info.at(wid).rx_queue;
                    const std::lock_guard<std::mutex> lock(mutex);
                    assign_flow_to_rx_queue(fd, rx_queue);
                }

                bool zc_ok = config_fd(fd);
                part_to_target.at(remote_id).conns.at(c_idx).zc_enabled = zc_ok;
            }
        }

        // register with epoll
        for (size_t i = 0; i < cfg.ips.size(); ++i) {
            if (i == cfg.my_id)
                continue;
            for (uint8_t conn = 0; conn < cfg.nr_conns; ++conn) {
                auto& c = part_to_target.at(i).conns.at(conn);
                ensure(c.fd != -1);
                ep_ctl(EPOLL_CTL_ADD, c.fd, EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP,
                       i, conn);
                c.cur_mask = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP; // NEW
                c.out_armed = false;

                fds_to_close.push_back(c.fd);
            }
        }

        if (cfg.recv_zc) {
            ensure(false, "recv_zc is not supported in the epoll worker");
        }

        if (cfg.use_hashtable) {
            const auto n_tuples = cfg.scan_size / tuple_size / cfg.num_workers;
            const auto capacity = next_pow2(n_tuples * cfg.hashtable_factor);
            probe_table = std::make_unique<Hashtable>(capacity);
        }

        Logger::info("init done ", wid);
    }

    uint64_t scan_inserts = 0;
    uint64_t recv_inserts = 0;

    void run(MorselIterator<tuple_size>& morsel_it) override {
        RDTSCClock clock(2.4_GHz);
        std::unique_ptr<PerfEvent> e;
        if (cfg.perfevent) {
            e = std::make_unique<PerfEvent>();
            e->startCounters();
        }
        clock.start();

        uint64_t n_tuples = 0;
        uint64_t copies = 0;
        uint64_t sents = 0;

        while (true) {
            auto morsel = morsel_it.next();
            if (morsel.empty())
                break;

            for (auto& tuple : morsel) {
                uint64_t part_id = tuple.key % cfg.partitions;
                if (part_id != cfg.my_id) {
                    auto& target = part_to_target[part_id];

                    auto& buf = target.fill_buffer;
                    if (!buf) {
                        buf = unused_buffers.pop();
                        buf->clear();
                    }

                    auto slot = buf->get_slot();
                    std::memcpy(slot, &tuple, sizeof(tuple_t));

                    if (buf->full()) {
                        // find available connection
                        uint8_t conn_id;
                        while (true) {
                            bool found = false;
                            for (conn_id = 0; conn_id < cfg.nr_conns; ++conn_id) {
                                auto& c = target.conns[conn_id];
                                if (!c.send_buffer && !c.done) {
                                    found = true;
                                    break;
                                }
                            }
                            if (found)
                                break;
                            io_begin();
                            process_events();
                            io_end();
                        }
                        if (cfg.use_budget) {
                            io_begin();
                            while (target.budget == 0) {
                                process_events();
                            }
                            io_end();
                        }

                        auto& c = target.conns[conn_id];
                        ensure(!c.send_buffer);
                        std::swap(buf, c.send_buffer);

                        // try to send immediately
                        io_begin();
                        send_progress(part_id, conn_id);
                        process_events();
                        io_end();
                        sents++;

                        if (cfg.use_budget)
                            target.budget--;
                    }
                    ++copies;
                } else {
                    if (cfg.use_hashtable) {
                        probe_table->insert_batch(tuple.key, &tuple);
                        scan_inserts++;
                    }
                }
                ++n_tuples;
            }

            io_begin();
            process_events();
            io_end();
        }

        // Flush any remaining per-target fill buffers
        for (int part_id = 0; part_id < cfg.partitions; ++part_id) {
            if (part_id == cfg.my_id)
                continue;
            auto& t = part_to_target[part_id];

            if (t.fill_buffer) {
                for (uint8_t conn_id = 0; conn_id < cfg.nr_conns; ++conn_id) {
                    auto& c = t.conns[conn_id];
                    if (!c.send_buffer && !c.done) {
                        std::swap(t.fill_buffer, c.send_buffer);
                        ensure(!t.fill_buffer);
                        io_begin();
                        send_progress(part_id, conn_id);
                        io_end();
                        break;
                    }
                }
            }
        }

        // Drain sends, half-close, wait for peer FIN
        for (int part_id = 0; part_id < cfg.partitions; ++part_id) {
            if (part_id == cfg.my_id)
                continue;
            for (uint8_t conn_id = 0; conn_id < cfg.nr_conns; ++conn_id) {
                auto& c = part_to_target[part_id].conns[conn_id];

                // finish queuing remaining bytes (if any)
                io_begin();
                while (c.send_buffer && !c.done) {
                    send_progress(part_id, conn_id);
                    process_events();
                }

                // 2) immediately half-close our TX (once)
                if (!c.wr_shutdown) {
                    shutdown(c.fd, SHUT_WR);
                    c.wr_shutdown = true;
                    want_read(part_id, conn_id); // ensures mask = IN|ERR|HUP|RDHUP
                    if (c.peer_rd_closed)
                        c.done = true; // handshake already complete
                }

                // If ZC in-flight, wait for ERRQUEUE to free buffer
                while (!c.done) {
                    process_events();
                }

                io_end();
            }
        }

        // Wait for remote side
        RDTSCClock done_clock(2.4_GHz);
        uint64_t last_rx = 0, last_tx = 0;
        bool clock_running = false;
        while (true) {
            bool all_done = true;
            for (int part_id = 0; part_id < cfg.partitions; ++part_id) {
                if (part_id == cfg.my_id)
                    continue;
                for (uint8_t conn_id = 0; conn_id < cfg.nr_conns; ++conn_id) {
                    auto& c = part_to_target[part_id].conns[conn_id];
                    if (!c.done || c.send_buffer) {
                        all_done = false;
                        break;
                    }
                }
                if (!all_done)
                    break;
            }
            if (all_done)
                break;

            io_begin();
            process_events();
            io_end();

            if (last_rx == bytes_recv && last_tx == bytes_sent) {
                if (!clock_running) {
                    done_clock.start();
                    clock_running = true;
                } else {
                    done_clock.stop();
                    auto passed = done_clock.as<std::chrono::milliseconds, uint64_t>();
                    if (passed > 1000)
                        break;
                }
            } else {
                clock_running = false;
            }
            last_rx = bytes_recv;
            last_tx = bytes_sent;
        }

        if (cfg.use_hashtable)
            probe_table->flush_batch();

        clock.stop();
        auto sec = clock.as<std::chrono::microseconds, uint64_t>() / 1e6;
        if (e) {
            e->stopCounters();
            e->printReport(std::cout, n_tuples);
        }
        Logger::info("Scan took: ", sec, "s");
        Logger::info("n_tuples=", n_tuples);
        if (cfg.use_hashtable)
            Logger::info("probe_table=", probe_table->size());
        Logger::info("scans=", scan_inserts, " recvs=", recv_inserts);
    }
};

struct TCPBarrier {
    std::vector<std::string>& ips;
    std::vector<int> conns;

    int server_fd = 0;
    TCPBarrier(std::vector<std::string>& ips, uint16_t port, int my_id)
        : ips(ips) {
        auto my_ip = ips.at(my_id);
        server_fd = listen_on(my_ip.c_str(), port, 1024);

        static std::mutex mutex;

        conns.reserve(ips.size() - 1);
        for (int i = my_id + 1; i < ips.size(); ++i) {
            auto ip = ips.at(i);

            int retries = 100; // 10 secs
            int fd = connect_to(ip.c_str(), port, retries, 100'000);
            set_nodelay(fd);

            auto& cfg = Config::get();
            if (cfg.pin_queues) {
                const std::lock_guard<std::mutex> lock(mutex);
                assign_flow_to_rx_queue(fd, 0);
            }

            conns.push_back(fd);
        }
        for (int i = 0; i < my_id; ++i) {
            Logger::info("waiting for: ", i);
            int fd = accept(server_fd, nullptr, nullptr);
            check_ret(fd);
            set_cloexec(fd);
            set_nodelay(fd);

            auto& cfg = Config::get();
            if (cfg.pin_queues) {
                const std::lock_guard<std::mutex> lock(mutex);
                assign_flow_to_rx_queue(fd, 0);
            }

            conns.push_back(fd);
        }

        ensure(conns.size() == ips.size() - 1);
    }

    ~TCPBarrier() {
        close(server_fd);
        for (auto fd : conns) {
            close(fd);
        }
    }

    void wait() {
        for (auto fd : conns) {
            int val = 0;
            ensure(send(fd, &val, sizeof(int), MSG_WAITALL) == sizeof(int));
        }
        for (auto fd : conns) {
            int val = 0;
            ensure(recv(fd, &val, sizeof(int), MSG_WAITALL) == sizeof(int));
        }
        Logger::info("TCPBarrier done");
    }
};

template <size_t tuple_size>
void do_benchmark() {
    using tuple_t = Tuple<tuple_size>;

    auto& cfg = Config::get();

    Logger::info("Benchmark start");

    auto& stats = StatsPrinter::get();
    if (cfg.stats_interval > 0) {
        stats.interval = cfg.stats_interval;
    }
    stats.start();

    HugePages mem(cfg.scan_size + 2_MiB * 256);
    // SmallPages mem(cfg.scan_size);
    MiniAlloc alloc(mem.addr, mem.size);

    const auto n_tuples = cfg.scan_size / tuple_size;
    auto [tuples, _] = alloc.allocate_array<tuple_t>(n_tuples);

    { // load phase
        Logger::info("Load start");
        RDTSCClock clock(2.4_GHz);
        clock.start();

        int num_threads = 64;
        ThreadPool tp;
        tp.parallel_n(num_threads, [&](std::stop_token, int id) {
            CPUMap::get().pin(8 + id);
            MersenneTwister mt(cfg.my_id * 1000 + id);
            auto [start, end] = RangeHelper::nth_chunk(0, n_tuples, num_threads, id);
            for (uint64_t i = start; i < end; i++) {
                auto& tuple = tuples[i];
                tuple.key = mt.rnd();
            }
        });
        tp.join();

        clock.stop();
        auto sec = clock.as<std::chrono::microseconds, uint64_t>() / 1e6;
        Logger::info("Load took: ", sec, "s");
    }

    using IWorker = IWorker<tuple_size>;

    std::vector<std::unique_ptr<IWorker>> workers;
    workers.reserve(cfg.num_workers);
    for (int i = 0; i < cfg.num_workers; ++i) {
        if (!cfg.use_epoll) {
            auto w = std::make_unique<Worker<tuple_size>>(i);
            workers.push_back(std::move(w));
        } else {
            auto w = std::make_unique<EpollWorker<tuple_size>>(i);
            workers.push_back(std::move(w));
        }
    }

    MorselIterator<tuple_size> morsel_it(tuples, n_tuples);

    StatsPrinter::Scope stats_scope;
    std::array<size_t, 32> last_bytes;
    last_bytes.fill(0);
    stats.register_func(stats_scope, [&](auto& ss) {
        static Diff<uint64_t> diff_recv;
        static Diff<uint64_t> diff_sent;
        static Diff<uint64_t> diff_io_cycles;
        uint64_t sum_recv = 0;
        uint64_t sum_sent = 0;
        uint64_t stalled = 0;
        uint64_t sum_io_cycles = 0;
        for (size_t i = 0; auto& worker : workers) {
            sum_recv += worker->bytes_recv;
            sum_sent += worker->bytes_sent;
            sum_io_cycles += worker->io_cycles;
            if (worker->bytes_recv == last_bytes[i]) {
                stalled++;
            }
            last_bytes[i] = worker->bytes_recv;
            ++i;
        }
        auto bytes_recv = diff_recv(sum_recv);
        auto bytes_sent = diff_sent(sum_sent);
        auto io_cycles = diff_io_cycles(sum_io_cycles);
        ss << " recv=" << bytes_recv;
        ss << " sent=" << bytes_sent;
        ss << " recv_mib=" << bytes_recv / (1UL << 20);
        ss << " sent_mib=" << bytes_sent / (1UL << 20);
        ss << " ratio=" << bytes_recv / static_cast<double>(bytes_sent);
        ss << " total_mib=" << (bytes_sent + bytes_recv) / (1UL << 20);
        ss << " io_cycles=" << io_cycles;
        ss << " stalled=" << stalled;
    });

    TCPBarrier tcp_barrier(cfg.ips, cfg.port - 1, cfg.my_id);
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, cfg.num_workers + 1);

    Logger::info("Scan start tuple_size=", sizeof(tuple_t));

    bool stop = false;
    ThreadPool tp;
    tp.parallel_n(cfg.num_workers, [&](std::stop_token token, int id) {
        CPUMap::get().pin(pin_info.at(id).core_id);

        auto& worker = workers.at(id);
        worker->init();

        pthread_barrier_wait(&barrier);
        pthread_barrier_wait(&barrier);

        worker->run(morsel_it);
    });

    pthread_barrier_wait(&barrier);
    tcp_barrier.wait();
    pthread_barrier_wait(&barrier);

    // executes


    tp.join();

    for (auto& w : workers) {
        Logger::info("sent=", w->bytes_sent, " recv=", w->bytes_recv);
        w->deinit();
    }
}

#include <csignal>

int main(int argc, char** argv) {
    if (!jmp::init()) { // enables run-time code patching
        return errno;
    }
    auto& cfg = Config::get();
    cfg.parse(argc, argv);

    signal(SIGUSR1, [](int signum) {
        Logger::info("got sigusr");
        exit(0);
    });

    if (cfg.same_irq) {
        for (auto& p : pin_info) {
            p.tx_queue = p.rx_queue;
        }
    }

    CPUMap::get().pin(cfg.core_id);

    switch (cfg.tuple_size) {
        case 16:
            do_benchmark<16>();
            break;
        case 32:
            do_benchmark<32>();
            break;
        case 64:
            do_benchmark<64>();
            break;
        case 128:
            do_benchmark<128>();
            break;
        case 256:
            do_benchmark<256>();
            break;
        case 512:
            do_benchmark<512>();
            break;
        case 1024:
            do_benchmark<1024>();
            break;
        case 2048:
            do_benchmark<2048>();
            break;
        case 4096:
            do_benchmark<4096>();
            break;
        case 8192:
            do_benchmark<8192>();
            break;
        case 16384:
            do_benchmark<16384>();
            break;
        default:
            ensure(false, "invalid tuple_size");
            break;
    };

    return 0;
}
