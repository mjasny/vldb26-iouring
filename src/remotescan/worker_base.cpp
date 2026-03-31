#include "remotescan/worker_base.hpp"

#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"

namespace remotescan {

RegisteredRing::~RegisteredRing() {
    io_uring_queue_exit(&ring);
}

WorkerBase::WorkerBase(const Config& cfg, size_t worker_idx) : cfg(cfg), worker_idx(worker_idx) {
}

const WorkerPinInfo& WorkerBase::worker_pin() const {
    return pin_info.at(worker_idx % pin_info.size());
}

void WorkerBase::init_ring() {
    io_uring_params params {};
    params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CLAMP;
    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = cfg.ring_entries * 2;

    if (cfg.setup_mode == SetupMode::DEFER_TASKRUN) {
        params.flags |= IORING_SETUP_DEFER_TASKRUN;
    } else if (cfg.setup_mode == SetupMode::COOP_TASKRUN) {
        params.flags |= IORING_SETUP_COOP_TASKRUN;
    } else if (cfg.setup_mode == SetupMode::SQPOLL) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 1000;
        params.flags |= IORING_SETUP_SQ_AFF;
        params.sq_thread_cpu = static_cast<uint32_t>(worker_pin().core_id);
    }

    check_iou(io_uring_queue_init_params(cfg.ring_entries, &reg_ring.ring, &params));

    if (cfg.reg_ring) {
        ensure(reg_ring.ring.features & IORING_FEAT_REG_REG_RING);
        ensure(io_uring_register_ring_fd(&reg_ring.ring) == 1);
        Logger::info("registered ring fd");
    }

    if (cfg.napi) {
        io_uring_napi napi {};
        napi.prefer_busy_poll = 1;
        napi.busy_poll_to = 50;
        check_iou(io_uring_register_napi(&reg_ring.ring, &napi));
        Logger::info("enabled napi");
    }
}

} // namespace remotescan
