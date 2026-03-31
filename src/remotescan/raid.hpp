#pragma once

#include "remotescan/config.hpp"

#include <cstdint>
#include <liburing.h>
#include <vector>

namespace remotescan {

struct SsdFile {
    int raw_fd = -1;
    int ring_fd = -1;
    uint64_t logical_bytes = 0;
    uint64_t usable_bytes = 0;

    SsdFile() = default;
    SsdFile(const SsdFile&) = delete;
    SsdFile& operator=(const SsdFile&) = delete;
    SsdFile(SsdFile&& other) noexcept;
    SsdFile& operator=(SsdFile&& other) noexcept;
    ~SsdFile();
};

struct StripeAddress {
    uint32_t ssd_idx = 0;
    uint64_t drive_offset = 0;
};

struct RaidLayout {
    const Config& cfg;
    std::vector<SsdFile> ssds;

    explicit RaidLayout(const Config& cfg);

    void register_files(io_uring& ring, uint32_t fixed_fd_base);
    StripeAddress map(uint64_t logical_offset, uint32_t len) const;
    int fd_for_request(uint64_t logical_offset, uint32_t len) const;
    uint64_t offset_for_request(uint64_t logical_offset, uint32_t len) const;
};

} // namespace remotescan
