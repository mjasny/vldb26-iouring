#pragma once

#include "my_asserts.hpp"
#include "my_logger.hpp"
#include "utils.hpp"

#include <cstdint>
#include <cstring>
#include <liburing.h>
#include <limits>

struct BufRing {
    struct io_uring* ring;
    struct io_uring_buf_ring* br;
    void* buf;


    uint32_t nr_bufs;
    uint32_t buf_size;
    int bgid;
    int br_mask;

    inline static int next_bgid = 1;


    BufRing(struct io_uring* ring, uint32_t nr_bufs, uint32_t buf_size, bool incremental = false) : ring(ring) {
        ensure(is_power_of_two(nr_bufs));
        ensure(nr_bufs <= 32768);

        size_t fsize = static_cast<size_t>(buf_size) * nr_bufs;
        check_ret(posix_memalign(&buf, 4096, fsize));

        memset(buf, 0, fsize);

        this->nr_bufs = nr_bufs;
        this->buf_size = buf_size;
        br_mask = io_uring_buf_ring_mask(nr_bufs);
        bgid = next_bgid++;

        int ret;
        uint32_t flags = 0;
        if (incremental) {
            flags |= IOU_PBUF_RING_INC;
        }
        br = io_uring_setup_buf_ring(ring, nr_bufs, bgid, flags, &ret);
        if (!br) {
            check_ret(ret);
        }

        uint8_t* ptr = reinterpret_cast<uint8_t*>(buf);
        for (uint32_t i = 0; i < nr_bufs; i++) {
            io_uring_buf_ring_add(br, ptr, buf_size, i, br_mask, i); // first bid is 1
            ptr += buf_size;
        }
        io_uring_buf_ring_advance(br, nr_bufs);
    }

    ~BufRing() {
        io_uring_free_buf_ring(ring, br, nr_bufs, bgid);
        free(buf);
    }

    inline void add_from_cqe(struct io_uring_cqe* cqe) {
        uint16_t bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
        uint8_t* ptr = reinterpret_cast<uint8_t*>(buf);
        ptr += bid * buf_size;
        io_uring_buf_ring_add(br, ptr, buf_size, bid, br_mask, 0);
        io_uring_buf_ring_advance(br, 1);
    }

    inline void* get_buffer(struct io_uring_cqe* cqe) {
        uint16_t bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
        uint8_t* ptr = reinterpret_cast<uint8_t*>(buf);
        ptr += bid * buf_size;
        return ptr;
    }

    inline void add_bundle_from_cqe(struct io_uring_cqe* cqe, uint32_t n_bytes) {
        uint16_t bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;

        int nr_bids = 0;
        while (n_bytes) {
            uint8_t* ptr = reinterpret_cast<uint8_t*>(buf);
            ptr += bid * buf_size;
            io_uring_buf_ring_add(br, ptr, buf_size, bid, br_mask, nr_bids);

            if (n_bytes > buf_size) {
                n_bytes -= buf_size;
            } else {
                n_bytes = 0;
            }

            bid = (bid + 1) & (nr_bufs - 1);
            nr_bids++;
        }

        io_uring_buf_ring_advance(br, nr_bids);
    }

    inline void set_bg(struct io_uring_sqe* sqe) {
        sqe->buf_group = bgid;
        sqe->flags |= IOSQE_BUFFER_SELECT;
    }

    int avail() {
        int ret = io_uring_buf_ring_available(ring, br, bgid);
        check_ret(ret);
        return ret;
    }
};
