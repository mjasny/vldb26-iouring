#pragma once

#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/utils.hpp"

#include <liburing.h>
#include <linux/nvme_ioctl.h>
#include <nvme/types.h>
#include <sys/ioctl.h>

extern uint32_t nsid;
extern uint32_t lba_shift;

int nvme_get_info(int fd) {
    nsid = ioctl(fd, NVME_IOCTL_ID);
    check_ret(nsid);

    constexpr uint32_t NVME_DEFAULT_IOCTL_TIMEOUT = 0;
    constexpr uint32_t NVME_IDENTIFY_CSI_SHIFT = 24;

    struct nvme_id_ns ns;
    struct nvme_passthru_cmd cmd = {
        .opcode = nvme_admin_identify,
        .nsid = nsid,
        .addr = reinterpret_cast<uintptr_t>(&ns),
        .data_len = NVME_IDENTIFY_DATA_SIZE,
        .cdw10 = NVME_IDENTIFY_CNS_NS,
        .cdw11 = NVME_CSI_NVM << NVME_IDENTIFY_CSI_SHIFT,
        .timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT,
    };

    check_ret(ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd));

    uint32_t lba_size = 1 << ns.lbaf[(ns.flbas & 0x0f)].ds;
    lba_shift = ilog2(lba_size);

    return 0;
}


inline void prep_nvme_read(struct io_uring_sqe* sqe, int fd, void* buf, uint32_t len, uint64_t offset) {
    // https://github.com/axboe/liburing/blob/master/test/io_uring_passthrough.c

    sqe->fd = fd;
    sqe->flags = 0;

    // io_uring_prep_read(sqe, fd, buf, len, offset);

    sqe->opcode = IORING_OP_URING_CMD;
    sqe->cmd_op = NVME_URING_CMD_IO;
    struct nvme_uring_cmd* cmd = reinterpret_cast<struct nvme_uring_cmd*>(sqe->cmd);
    memset(cmd, 0, sizeof(struct nvme_uring_cmd));

    uint64_t slba = offset >> lba_shift;
    uint32_t nlb = (len >> lba_shift) - 1;

    cmd->opcode = nvme_cmd_read;
    cmd->cdw10 = slba & 0xffffffff;
    cmd->cdw11 = slba >> 32;
    cmd->cdw12 = nlb;
    cmd->addr = reinterpret_cast<uintptr_t>(buf);
    cmd->data_len = len;
    cmd->nsid = nsid;
    // cmd->cdw13 = 1 << 6; // DSM Sequential Request
    // Data Set Management (DSM) Hints: Written in near future, seq. read, seq. write...
}

inline void prep_nvme_write(struct io_uring_sqe* sqe, int fd, void* buf, uint32_t len, uint64_t offset) {
    sqe->fd = fd;
    sqe->flags = 0;


    // sqe->off = offset;
    // io_uring_prep_write(sqe, fd, buf, len, offset);

    sqe->opcode = IORING_OP_URING_CMD;
    sqe->cmd_op = NVME_URING_CMD_IO;
    struct nvme_uring_cmd* cmd = reinterpret_cast<struct nvme_uring_cmd*>(sqe->cmd);
    memset(cmd, 0, sizeof(struct nvme_uring_cmd));

    uint64_t slba = offset >> lba_shift;
    uint32_t nlb = (len >> lba_shift) - 1;
    // Logger::info("nsid=", nsid, " lba_shift=", lba_shift, " slba=", slba);

    cmd->opcode = nvme_cmd_write;
    cmd->cdw10 = slba & 0xffffffff;
    cmd->cdw11 = slba >> 32;
    cmd->cdw12 = nlb;
    cmd->addr = reinterpret_cast<uintptr_t>(buf);
    cmd->data_len = len;
    cmd->nsid = nsid;
    // cmd->cdw13 = 1 << 6; // DSM Sequential Request
}
