#pragma once

#include "remotescan/config.hpp"
#include "remotescan/pinning.hpp"

#include <liburing.h>

namespace remotescan {

struct RegisteredRing {
    io_uring ring {};
    ~RegisteredRing();
};

struct WorkerBase {
    const Config& cfg;
    RegisteredRing reg_ring;
    size_t worker_idx;

    WorkerBase(const Config& cfg, size_t worker_idx);
    virtual ~WorkerBase() = default;

    const WorkerPinInfo& worker_pin() const;
    void init_ring();
};

} // namespace remotescan
