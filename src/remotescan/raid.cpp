#include "remotescan/raid.hpp"

#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"

#include <fcntl.h>
#include <linux/fs.h>
#include <mutex>
#include <sys/ioctl.h>
#include <unistd.h>

namespace remotescan {

namespace {
std::once_flag g_log_ssd_info_once;
}

SsdFile::SsdFile(SsdFile&& other) noexcept
    : raw_fd(other.raw_fd), ring_fd(other.ring_fd), logical_bytes(other.logical_bytes), usable_bytes(other.usable_bytes) {
    other.raw_fd = -1;
    other.ring_fd = -1;
    other.logical_bytes = 0;
    other.usable_bytes = 0;
}

SsdFile& SsdFile::operator=(SsdFile&& other) noexcept {
    if (this != &other) {
        if (raw_fd >= 0) {
            close(raw_fd);
        }
        raw_fd = other.raw_fd;
        ring_fd = other.ring_fd;
        logical_bytes = other.logical_bytes;
        usable_bytes = other.usable_bytes;
        other.raw_fd = -1;
        other.ring_fd = -1;
        other.logical_bytes = 0;
        other.usable_bytes = 0;
    }
    return *this;
}

SsdFile::~SsdFile() {
    if (raw_fd >= 0) {
        close(raw_fd);
    }
}

RaidLayout::RaidLayout(const Config& cfg) : cfg(cfg) {
    std::vector<std::tuple<std::string, int, int, int, uint64_t, uint64_t>> ssd_infos;
    ssd_infos.reserve(cfg.ssds.size());
    ssds.reserve(cfg.ssds.size());
    for (const auto& path : cfg.ssds) {
        SsdFile ssd;
        ssd.raw_fd = open(path.c_str(), O_RDONLY | O_DIRECT | O_CLOEXEC);
        check_ret(ssd.raw_fd);

        int logical_block_size = 0;
        int physical_block_size = 0;
        int optimal_io_size = 0;
        uint64_t logical_bytes = 0;
        check_ret(ioctl(ssd.raw_fd, BLKSSZGET, &logical_block_size));
        check_ret(ioctl(ssd.raw_fd, BLKPBSZGET, &physical_block_size));
        check_ret(ioctl(ssd.raw_fd, BLKIOOPT, &optimal_io_size));
        check_ret(ioctl(ssd.raw_fd, BLKGETSIZE64, &logical_bytes));
        ssd.logical_bytes = logical_bytes;
        ssd.usable_bytes = logical_bytes - (logical_bytes % cfg.stripe_size);
        ensure(ssd.usable_bytes >= cfg.stripe_size);
        ssd_infos.emplace_back(path, logical_block_size, physical_block_size, optimal_io_size, ssd.logical_bytes, ssd.usable_bytes);

        ssds.push_back(std::move(ssd));
    }

    std::call_once(g_log_ssd_info_once, [&ssd_infos]() {
        for (const auto& [path, logical_block_size, physical_block_size, optimal_io_size, logical_bytes, usable_bytes] : ssd_infos) {
            Logger::info("ssd=", path,
                         " logical_block_size=", logical_block_size,
                         " physical_block_size=", physical_block_size,
                         " optimal_io_size=", optimal_io_size,
                         " logical_bytes=", logical_bytes,
                         " usable_bytes=", usable_bytes);
        }
    });
}

void RaidLayout::register_files(io_uring& ring, uint32_t fixed_fd_base) {
    for (uint32_t i = 0; i < ssds.size(); ++i) {
        int fd = ssds[i].raw_fd;
        check_iou(io_uring_register_files_update(&ring, fixed_fd_base + i, &fd, 1));
        ssds[i].ring_fd = static_cast<int>(fixed_fd_base + i);
    }
}

StripeAddress RaidLayout::map(uint64_t logical_offset, uint32_t len) const {
    ensure(!ssds.empty());
    ensure(logical_offset % kBlockSize == 0);
    ensure(len % kBlockSize == 0);

    const uint64_t stripe_nr = logical_offset / cfg.stripe_size;
    const uint64_t stripe_off = logical_offset % cfg.stripe_size;
    ensure(stripe_off + len <= cfg.stripe_size);

    const uint32_t ssd_idx = stripe_nr % ssds.size();
    const uint64_t stripe_round = stripe_nr / ssds.size();
    const uint64_t wrapped_base = (stripe_round * cfg.stripe_size) % ssds[ssd_idx].usable_bytes;
    const uint64_t drive_offset = wrapped_base + stripe_off;
    return {.ssd_idx = ssd_idx, .drive_offset = drive_offset};
}

int RaidLayout::fd_for_request(uint64_t logical_offset, uint32_t len) const {
    auto addr = map(logical_offset, len);
    return cfg.reg_fds ? ssds[addr.ssd_idx].ring_fd : ssds[addr.ssd_idx].raw_fd;
}

uint64_t RaidLayout::offset_for_request(uint64_t logical_offset, uint32_t len) const {
    return map(logical_offset, len).drive_offset;
}

} // namespace remotescan
