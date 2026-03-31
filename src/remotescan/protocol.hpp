#pragma once

#include <cstdint>

namespace remotescan {

struct ScanRequest {
    uint64_t offset = 0;
    uint32_t length = 0;
    uint32_t reserved = 0;
};

static_assert(sizeof(ScanRequest) == 16);

} // namespace remotescan
