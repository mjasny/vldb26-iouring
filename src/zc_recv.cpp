#include "shuffle/utils.hpp"
#include "utils/cli_parser.hpp"
#include "utils/cpu_map.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/perfevent.hpp"
#include "utils/socket.hpp"
#include "utils/stats_printer.hpp"
#include "utils/types.hpp"
#include "utils/utils.hpp"

#include <cerrno>
#include <cstring>
#include <liburing.h>
#include <linux/dma-buf.h>
#include <linux/memfd.h>
#include <linux/mman.h>
#include <linux/udmabuf.h>
#include <net/if.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/socket.h>


static long page_size = 4096;
#define AREA_SIZE (8192 * page_size * 32)

enum {
    RQ_ALLOC_USER,
    RQ_ALLOC_KERNEL,

    __RQ_ALLOC_MAX,
};

enum {
    AREA_TYPE_NORMAL,
    AREA_TYPE_HUGE_PAGES,
    AREA_TYPE_DMABUF,
    __AREA_TYPE_MAX,
};

struct Config {
    std::string ip;
    uint16_t port = 1234;
    SetupMode setup_mode = SetupMode::DEFER_TASKRUN;
    int core_id = 3;
    uint32_t duration = 30'000;
    uint32_t size = 0;
    bool perfevent = false;
    int nr_conns = 1;

    std::string ifname;
    uint32_t queue_id;

    int rq_alloc_mode = RQ_ALLOC_USER;
    int area_type = AREA_TYPE_NORMAL;


    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--ip", ip);
        parser.parse("--port", port, cli::Parser::optional);
        parser.parse("--setup_mode", setup_mode, cli::Parser::optional);
        parser.parse("--core_id", core_id, cli::Parser::optional);
        parser.parse("--duration", duration, cli::Parser::optional);
        parser.parse("--size", size, cli::Parser::optional);
        parser.parse("--perfevent", perfevent, cli::Parser::optional);
        parser.parse("--nr_conns", nr_conns, cli::Parser::optional);
        parser.parse("--ifname", ifname);
        parser.parse("--queue_id", queue_id);
        parser.check_unparsed();
        parser.print();

        ensure(queue_id > 0, "no admin queue?");
    }
};

static Config cfg;

static void* area_ptr;
static void* ring_ptr;
static size_t ring_size;
static struct io_uring_zcrx_rq rq_ring;


static uint64_t area_token;
static uint32_t zcrx_id;

static int dmabuf_fd;
static int memfd;


#define T_ALIGN_UP(v, align) (((v) + (align) - 1) & ~((align) - 1))

static inline __u64 uring_ptr_to_u64(const void* ptr) {
    return (__u64)(unsigned long)ptr;
}


static inline size_t get_refill_ring_size(unsigned int rq_entries) {
    ring_size = rq_entries * sizeof(struct io_uring_zcrx_rqe);
    /* add space for the header (head/tail/etc.) */
    ring_size += page_size;
    return T_ALIGN_UP(ring_size, page_size);
}


static void zcrx_populate_area_udmabuf(struct io_uring_zcrx_area_reg* area_reg) {

    int devfd = open("/dev/udmabuf", O_RDWR);
    check_ret(devfd);

    memfd = memfd_create("udmabuf-test", MFD_ALLOW_SEALING);
    check_ret(memfd);

    check_ret(fcntl(memfd, F_ADD_SEALS, F_SEAL_SHRINK));

    check_ret(ftruncate(memfd, AREA_SIZE));

    struct udmabuf_create create;
    memset(&create, 0, sizeof(create));
    create.memfd = memfd;
    create.offset = 0;
    create.size = AREA_SIZE;
    dmabuf_fd = ioctl(devfd, UDMABUF_CREATE, &create);
    check_ret(dmabuf_fd);

    area_ptr = mmap(NULL, AREA_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
                    dmabuf_fd, 0);
    ensure(area_ptr != MAP_FAILED);

    memset(area_reg, 0, sizeof(*area_reg));
    area_reg->addr = 0; /* offset into dmabuf */
    area_reg->len = AREA_SIZE;
    area_reg->flags |= IORING_ZCRX_AREA_DMABUF;
    area_reg->dmabuf_fd = dmabuf_fd;

    close(devfd);
}


static void zcrx_populate_area(struct io_uring_zcrx_area_reg* area_reg) {
    unsigned flags = MAP_PRIVATE | MAP_ANONYMOUS;
    unsigned prot = PROT_READ | PROT_WRITE;

    if (cfg.area_type == AREA_TYPE_DMABUF) {
        zcrx_populate_area_udmabuf(area_reg);
        return;
    }
    if (cfg.area_type == AREA_TYPE_NORMAL) {
        area_ptr = mmap(NULL, AREA_SIZE, prot,
                        flags, 0, 0);
    } else if (cfg.area_type == AREA_TYPE_HUGE_PAGES) {
        area_ptr = mmap(NULL, AREA_SIZE, prot,
                        flags | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
    }

    ensure(area_ptr != MAP_FAILED);

    memset(area_reg, 0, sizeof(*area_reg));
    area_reg->addr = uring_ptr_to_u64(area_ptr);
    area_reg->len = AREA_SIZE;
    area_reg->flags = 0;
}

static void setup_zcrx(struct io_uring* ring) {
    struct io_uring_zcrx_area_reg area_reg;
    uint32_t rq_entries = 4096 * 8;
    unsigned rq_flags = 0;

    uint32_t ifindex = if_nametoindex(cfg.ifname.c_str());
    check_ret(ifindex);

    ring_size = get_refill_ring_size(rq_entries);
    ring_ptr = NULL;
    if (cfg.rq_alloc_mode == RQ_ALLOC_USER) {
        ring_ptr = mmap(NULL, ring_size,
                        PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_PRIVATE,
                        0, 0);

        ensure(ring_ptr != MAP_FAILED);
        rq_flags |= IORING_MEM_REGION_TYPE_USER;
    }

    struct io_uring_region_desc region_reg = {
        .user_addr = uring_ptr_to_u64(ring_ptr),
        .size = ring_size,
        .flags = rq_flags,
    };

    zcrx_populate_area(&area_reg);

    struct io_uring_zcrx_ifq_reg reg = {
        .if_idx = ifindex,
        .if_rxq = cfg.queue_id,
        .rq_entries = rq_entries,
        .area_ptr = uring_ptr_to_u64(&area_reg),
        .region_ptr = uring_ptr_to_u64(&region_reg),
    };

    check_iou(io_uring_register_ifq(ring, &reg));

    if (cfg.rq_alloc_mode == RQ_ALLOC_KERNEL) {
        ring_ptr = mmap(NULL, ring_size,
                        PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_POPULATE,
                        ring->ring_fd, region_reg.mmap_offset);
        ensure(ring_ptr != MAP_FAILED);
    }

    rq_ring.khead = (unsigned int*)((char*)ring_ptr + reg.offsets.head);
    rq_ring.ktail = (unsigned int*)((char*)ring_ptr + reg.offsets.tail);
    rq_ring.rqes = (struct io_uring_zcrx_rqe*)((char*)ring_ptr + reg.offsets.rqes);
    rq_ring.rq_tail = 0;
    rq_ring.ring_entries = reg.rq_entries;

    zcrx_id = reg.zcrx_id;
    area_token = area_reg.rq_area_token;
}

static uint64_t bytes_recv = 0;
constexpr bool print_payload = false;

static void process_recvzc(struct io_uring __attribute__((unused)) * ring,
                           struct io_uring_cqe* cqe) {
    unsigned rq_mask = rq_ring.ring_entries - 1;
    struct io_uring_zcrx_cqe* rcqe;
    struct io_uring_zcrx_rqe* rqe;
    uint64_t mask;
    char* data;


    rcqe = (struct io_uring_zcrx_cqe*)(cqe + 1);
    mask = (1ULL << IORING_ZCRX_AREA_SHIFT) - 1;
    data = (char*)area_ptr + (rcqe->off & mask);


    // verify_data(data, cqe->res, received);
    bytes_recv += cqe->res;
    if (print_payload) {
        printf("Data: %.*s\n", cqe->res, data);
        Logger::info("len=", cqe->res);
    }

    /* processed, return back to the kernel */
    rqe = &rq_ring.rqes[rq_ring.rq_tail & rq_mask];
    rqe->off = (rcqe->off & ~IORING_ZCRX_AREA_MASK) | area_token;
    rqe->len = cqe->res;
    io_uring_smp_store_release(rq_ring.ktail, ++rq_ring.rq_tail);
}


static void add_recvzc(struct io_uring* ring, int sockfd, size_t len) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);

    io_uring_prep_rw(IORING_OP_RECV_ZC, sqe, sockfd, NULL, len, 0);
    sqe->ioprio |= IORING_RECV_MULTISHOT; // required
    sqe->zcrx_ifq_idx = zcrx_id;
    sqe->user_data = sockfd;

    // apparently IORING_RECVSEND_POLL_FIRST is supported too
}


// + shuffle/prepare.sh
// Hardware GRO (generic receive offload).
//
// sudo ethtool -K ens3np0 rx-gro-hw on
// sudo ethtool -G ens3np0 tcp-data-split on
// sudo ethtool -X ens3np0 equal 1
//
// sudo ethtool -G ens3np0 rx 4096 tx 4096   <= increase arena size
//
// reset
// sudo ethtool -G ens3np0 tcp-data-split off
// sudo ethtool -K ens3np0 rx-gro-hw off
// sudo ethtool -X ens3np0 equal 63


int main(int argc, char** argv) {
    cfg.parse(argc, argv);


    auto& stats = StatsPrinter::get();
    stats.start();


    StatsPrinter::Scope stats_scope;
    stats.register_var(stats_scope, bytes_recv, "bw");
    stats.register_func(stats_scope, [&](auto& ss) {
        static Diff<uint64_t> diff;
        ss << " bw_mib=" << diff(bytes_recv) / (1UL << 20);
    });


    if (cfg.core_id != -1) {
        CPUMap::get().pin(cfg.core_id);
    }


    struct io_uring ring;
    struct io_uring_params params;

    memset(&params, 0, sizeof(params));
    params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CLAMP;
    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = 131072;
    params.flags |= IORING_SETUP_CQE32;

    if (cfg.setup_mode == SetupMode::DEFER_TASKRUN) {
        params.flags |= IORING_SETUP_DEFER_TASKRUN;
    }
    if (cfg.setup_mode == SetupMode::SQPOLL) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 1000;
        if (cfg.core_id != -1) {
            params.sq_thread_cpu = cfg.core_id + 1;
            params.flags |= IORING_SETUP_SQ_AFF;
        }
    }
    if (cfg.setup_mode == SetupMode::COOP_TASKRUN) {
        params.flags |= IORING_SETUP_COOP_TASKRUN;
    }


    if (io_uring_queue_init_params(4096, &ring, &params) < 0) {
        fprintf(stderr, "%s\n", strerror(errno));
        exit(1);
    }

    setup_zcrx(&ring);


    Logger::info("listening");
    int server_fd = listen_on(cfg.ip.c_str(), cfg.port);
    Logger::info("init done");
    Logger::flush();

    for (int i = 0; i < cfg.nr_conns; ++i) {
        int fd = accept(server_fd, nullptr, nullptr);
        check_ret(fd);
        set_nodelay(fd);

        Logger::info("accept fd=", fd);

        assign_flow_to_rx_queue(fd, cfg.queue_id);
        add_recvzc(&ring, fd, cfg.size);
    }

    std::unique_ptr<PerfEvent> e;
    if (cfg.perfevent) {
        e = std::make_unique<PerfEvent>();
        e->startCounters();
    }


    bool done = false;
    size_t bytes_recv = 0;
    std::array<size_t, 32> bytes_since_last;
    bytes_since_last.fill(0);
    while (!done) {
        if (cfg.setup_mode == SetupMode::SQPOLL) {
            io_uring_submit(&ring);
        } else {
            io_uring_submit_and_wait(&ring, 1);
        }


        unsigned head;
        int i = 0;
        struct io_uring_cqe* cqe;
        io_uring_for_each_cqe(&ring, head, cqe) {
            ++i;
            // Logger::info(cqe->res, " more=", (cqe->flags & IORING_CQE_F_MORE));

            check_iou(cqe->res);

            auto user_data = io_uring_cqe_get_data64(cqe);
            ensure(user_data > 0);


            if (!(cqe->flags & IORING_CQE_F_MORE)) {
                ensure(cqe->res == 0);
            }


            auto& last_bytes = bytes_since_last.at(user_data);
            if (cqe->res == 0) {
                // done = true;
                // continue;

                ensure(cqe->res == 0 && !(cqe->flags & IORING_CQE_F_MORE));

                // connection close
                if (last_bytes == 0) {
                    done = true;
                    continue;
                }
                if (cfg.size > 0) {
                    ensure(last_bytes <= cfg.size, [&] {
                        std::stringstream ss;
                        ss << "bytes_since_last=" << std::to_string(last_bytes);
                        return ss.str();
                    });
                }
                last_bytes = 0;
                add_recvzc(&ring, user_data, cfg.size);
                continue;
            }
            check_iou(cqe->res);


            if (!(cqe->flags & IORING_CQE_F_MORE)) {
                Logger::info("no more cqes");
                done = true;
                continue;
            }

            process_recvzc(&ring, cqe);

            bytes_recv += cqe->res;
            last_bytes += cqe->res;

            // if (cfg.size > 0 && bytes_since_last > cfg.size) {
            //     Logger::info("overread");
            // }
        }
        io_uring_cq_advance(&ring, i);
    }

    if (e) {
        e->stopCounters();
        e->printReport(std::cout, bytes_recv);
    }

    Logger::info("bytes_recv=", bytes_recv);

    io_uring_queue_exit(&ring);

    Logger::info("Exit");
}
