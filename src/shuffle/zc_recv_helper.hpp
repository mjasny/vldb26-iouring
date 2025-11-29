#pragma once

#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"

#include <cstddef>
#include <cstdint>
#include <liburing.h>
#include <linux/dma-buf.h>
#include <linux/memfd.h>
#include <linux/mman.h>
#include <linux/udmabuf.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>


struct ZCRecvHelper {
    enum {
        RQ_ALLOC_USER,
        RQ_ALLOC_KERNEL,
    };

    enum {
        AREA_TYPE_NORMAL,
        AREA_TYPE_HUGE_PAGES,
        AREA_TYPE_DMABUF,
    };

    static constexpr size_t page_size = 4096;
    static constexpr size_t area_size = 8192 * page_size * 32; // max *32
    static constexpr uint32_t rq_entries = 4096 * 8;

    int rq_alloc_mode = RQ_ALLOC_USER;
    int area_type = AREA_TYPE_NORMAL;

    void* area_ptr;
    void* ring_ptr;
    size_t ring_size;
    struct io_uring_zcrx_rq rq_ring;

    uint64_t area_token;
    uint32_t zcrx_id;

    int dmabuf_fd;
    int memfd;


    void setup(struct io_uring* ring, const char* ifname, uint32_t queue_id) {
        struct io_uring_zcrx_area_reg area_reg;
        unsigned rq_flags = 0;

        uint32_t ifindex = if_nametoindex(ifname);
        check_ret(ifindex);

        ring_size = get_refill_ring_size(rq_entries);
        ring_ptr = NULL;
        if (rq_alloc_mode == RQ_ALLOC_USER) {
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
            .if_rxq = queue_id,
            .rq_entries = rq_entries,
            .area_ptr = uring_ptr_to_u64(&area_reg),
            .region_ptr = uring_ptr_to_u64(&region_reg),
        };

        check_iou(io_uring_register_ifq(ring, &reg));

        if (rq_alloc_mode == RQ_ALLOC_KERNEL) {
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


    void prep_recv_zc(struct io_uring_sqe* sqe, int fd, size_t len) {
        io_uring_prep_rw(IORING_OP_RECV_ZC, sqe, fd, NULL, len, 0);
        sqe->ioprio |= IORING_RECV_MULTISHOT; // required
        sqe->zcrx_ifq_idx = zcrx_id;
        sqe->user_data = fd;

        // apparently IORING_RECVSEND_POLL_FIRST is supported too
    }


    template <typename Fn>
    void process_recvzc(struct io_uring_cqe* cqe, Fn&& fn) {
        unsigned rq_mask = rq_ring.ring_entries - 1;
        struct io_uring_zcrx_cqe* rcqe;
        struct io_uring_zcrx_rqe* rqe;
        uint64_t mask;
        char* data;


        rcqe = (struct io_uring_zcrx_cqe*)(cqe + 1);
        mask = (1ULL << IORING_ZCRX_AREA_SHIFT) - 1;
        data = (char*)area_ptr + (rcqe->off & mask);

        fn(data, cqe->res);

        /* processed, return back to the kernel */
        rqe = &rq_ring.rqes[rq_ring.rq_tail & rq_mask];
        rqe->off = (rcqe->off & ~IORING_ZCRX_AREA_MASK) | area_token;
        rqe->len = cqe->res;
        io_uring_smp_store_release(rq_ring.ktail, ++rq_ring.rq_tail);
    }

private:
    inline size_t get_refill_ring_size(unsigned int rq_entries) {
        auto ring_size = rq_entries * sizeof(struct io_uring_zcrx_rqe);
        /* add space for the header (head/tail/etc.) */
        ring_size += page_size;
        return ring_size = (ring_size + (page_size - 1)) & ~(page_size - 1);
    }

    inline __u64 uring_ptr_to_u64(const void* ptr) {
        return (__u64)(unsigned long)ptr;
    }

    void zcrx_populate_area_udmabuf(struct io_uring_zcrx_area_reg* area_reg) {

        int devfd = open("/dev/udmabuf", O_RDWR);
        check_ret(devfd);

        memfd = memfd_create("udmabuf-test", MFD_ALLOW_SEALING);
        check_ret(memfd);

        check_ret(fcntl(memfd, F_ADD_SEALS, F_SEAL_SHRINK));

        check_ret(ftruncate(memfd, area_size));

        struct udmabuf_create create;
        memset(&create, 0, sizeof(create));
        create.memfd = memfd;
        create.offset = 0;
        create.size = area_size;
        dmabuf_fd = ioctl(devfd, UDMABUF_CREATE, &create);
        check_ret(dmabuf_fd);

        area_ptr = mmap(NULL, area_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                        dmabuf_fd, 0);
        ensure(area_ptr != MAP_FAILED);

        memset(area_reg, 0, sizeof(*area_reg));
        area_reg->addr = 0; /* offset into dmabuf */
        area_reg->len = area_size;
        area_reg->flags |= IORING_ZCRX_AREA_DMABUF;
        area_reg->dmabuf_fd = dmabuf_fd;

        close(devfd);
    }


    void zcrx_populate_area(struct io_uring_zcrx_area_reg* area_reg) {
        unsigned flags = MAP_PRIVATE | MAP_ANONYMOUS;
        unsigned prot = PROT_READ | PROT_WRITE;

        if (area_type == AREA_TYPE_DMABUF) {
            zcrx_populate_area_udmabuf(area_reg);
            return;
        }
        if (area_type == AREA_TYPE_NORMAL) {
            area_ptr = mmap(NULL, area_size, prot,
                            flags, 0, 0);
        } else if (area_type == AREA_TYPE_HUGE_PAGES) {
            area_ptr = mmap(NULL, area_size, prot,
                            flags | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
        }

        ensure(area_ptr != MAP_FAILED);

        memset(area_reg, 0, sizeof(*area_reg));
        area_reg->addr = uring_ptr_to_u64(area_ptr);
        area_reg->len = area_size;
        area_reg->flags = 0;
    }
};
