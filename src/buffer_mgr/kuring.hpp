#pragma once

#include "exp_table.hpp"
#include "spsc_ring.hpp"
#include "tpcc/random_generator.hpp"
#include "utils/cpu_map.hpp"
#include "utils/jmp.hpp"
#include "utils/literals.hpp"
#include "utils/my_asserts.hpp"
#include "utils/my_logger.hpp"
#include "utils/rdtsc_clock.hpp"
#include "utils/stack.hpp"
#include "utils/utils.hpp"

#include <algorithm>
#include <boost/context/continuation.hpp>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <libaio.h>
#include <liburing.h>
#include <memory>
#include <utility>
#include <vector>

namespace ctx = boost::context;

extern u64 write_cycles;
extern u64 io_cycles;
extern RDTSCClock write_clock;


struct BaseReactor {
    enum class State : uint8_t { Ready,
                                 Running,
                                 Parked,
                                 Finished };

    struct Fiber {
        ctx::continuation peer;
        State state = State::Ready;
    };

    static constexpr size_t MAX_FIBERS = 256;


    template <class Fn>
    inline void spawn(Fiber* f, Fn&& fn) {
        f->peer = ctx::callcc(
            [this, f, fncap = std::forward<Fn>(fn)](ctx::continuation&& caller) mutable {
                // cold start: capture reactor continuation, bounce back
                f->peer = std::move(caller);
                f->peer = std::move(f->peer).resume(); // back to reactor
                // first real entry:
                fncap();
                f->state = State::Finished;
                return std::move(f->peer);
            });

        ensure(ready_.push(f));
    }


    // Fiber-side API
    inline void yield() {
        assert(fiber_current_ && "yield() outside fiber");
        Fiber* f = fiber_current_;
        f->state = State::Ready;
        ensure(ready_.push(f));                // enqueue self now
        f->peer = std::move(f->peer).resume(); // hop to reactor
    }

    inline void park() {
        assert(fiber_current_ && "park() outside fiber");
        Fiber* f = fiber_current_;
        f->state = State::Parked;
        f->peer = std::move(f->peer).resume();
    }

    inline bool wake(Fiber* f) noexcept {
        if (!f || f->state == State::Finished) [[unlikely]]
            return false;

        if (f->state == State::Parked) {
            f->state = State::Ready;
            ensure(ready_.push(f));
            return true;
        }
        // Ready or Running: nothing to do
        return false;
    }

    inline Fiber* current() const noexcept {
        return fiber_current_;
    }


protected:
    Fiber* fiber_current_ = nullptr; // valid only while inside a fiber
    SpscRing<Fiber*, MAX_FIBERS> ready_;
};


struct UringReactor : BaseReactor {

    uint64_t fiber_run = 0;
    uint64_t get_events = 0;


    // Minimal scheduler: resume until ring is empty. No requeue here.
    void run(bool& stop) {
        while (!stop) {
            Fiber* f;
            size_t n = ready_.size();
            for (int i = 0; i < n; ++i) {
                ensure(ready_.pop(f));
                f->peer = std::move(f->peer).resume_with([this, f](ctx::continuation&& back) noexcept {
                    fiber_current_ = f;
                    f->state = State::Running;
                    return std::move(back);
                });
                fiber_current_ = nullptr;
                fiber_run++;
            }
            drain_cqe();
        }
    }


    struct Op {
        Fiber* ctx = nullptr; // set by do_io()
        int res = 0;          // cqe->res
        unsigned flags = 0;   // cqe->flags

        // for batching
        int cqe_left = 1;
        // for fibers waiting on this completion
        Op* next = nullptr;
    };

    struct io_uring& ring_;
    int outstanding_io = 0;
    int total_io_fibers = 0;
    int to_submit = 0;
    int fibers_since_first_io = 0;
    uint64_t num_submits = 0;

    UringReactor(struct io_uring& ring) : ring_(ring) {}


    template <class Prep>
    inline int io(Op& op, Prep&& prep) {
        // RDTSCClock clock(2.4_GHz);
        // clock.start();

        op.ctx = current();
        op.res = 0;
        op.flags = 0;

        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        check_ptr(sqe);
        prep(sqe);
        io_uring_sqe_set_data(sqe, &op);

        // io_uring_submit(&ring_);
        outstanding_io += op.cqe_left;
        to_submit++;
        check_submit();

        // clock.stop();
        // io_cycles += clock.cycles();

        park();

        return op.res;
    }


    template <class Prep>
    int io_batch(int n, Op& op, Prep&& prep) {
        ensure(n > 0);

        // RDTSCClock clock(2.4_GHz);
        // clock.start();

        op.ctx = current();
        op.res = 0;
        op.flags = 0;
        op.cqe_left = n;

        // RDTSCClock clock(2.4_GHz);
        // clock.start();

        for (int i = 0; i < n; ++i) {
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
            check_ptr(sqe);
            prep(i, sqe);
            io_uring_sqe_set_data(sqe, &op);
        }

        // io_uring_submit_and_wait(&ring_, n);
        outstanding_io += op.cqe_left;
        io_uring_submit(&ring_);
        if constexpr (WRITE_RESETS) {
            ++num_submits;
            to_submit = 0; // Reset
            fibers_since_first_io = 0;
        }
        write_clock.stop();
        write_cycles += write_clock.cycles();

        // clock.stop();
        // io_cycles += clock.cycles();

        park();

        return op.res;
    }

    static constexpr auto& table = ExpTable<MAX_FIBERS>::values;

    static constexpr bool WRITE_RESETS = true;

    static constexpr jmp::static_branch<bool> submit_always = false;


    void check_submit() {
        if (to_submit == 0) {
            return;
        }

        // if constexpr (ALWAYS_SUBMIT) {
        if (submit_always) {
            io_uring_submit(&ring_);
            to_submit = 0;
            ++num_submits;
            return;
        }

        if (to_submit > 0) {
            fibers_since_first_io++;
        }

        bool do_submit = false;
        if (fibers_since_first_io == total_io_fibers) {
            // Logger::info("first submit to_submit=", to_submit);
            do_submit = true;
        }

        if (!do_submit) {
            // int submit_prob = 1'000'000 / ((to_submit + 1) * 4);
            // int submit_prob = 1'000'000 * (table.at(fibers_since_first_io - to_submit) / table.at(2));
            // int submit_prob = 1'000'000 * (table.at(fibers_since_first_io - to_submit) / table.at(total_io_fibers / 2));
            int submit_prob = 1'000'000 * (table.at(fibers_since_first_io - to_submit) / table.at(total_io_fibers / 4));
            u64 rnd = RandomGenerator::getRand(0, 1'000'000);
            if (rnd <= submit_prob) {
                // Logger::info("submit batch=", to_submit);
                do_submit = true;
            }
        }

        if (do_submit) {
            io_uring_submit(&ring_);
            ++num_submits;
            to_submit = 0;
            fibers_since_first_io = 0;
        }
    }


    void drain_cqe() {
        if (outstanding_io == 0) {
            return;
        }


        // for (int i = 0; i < (outstanding_io + 19) / 20; ++i) {
        //     io_uring_get_events(&ring_);
        // }
        io_uring_get_events(&ring_);

        int i = 0;
        uint32_t head;
        struct io_uring_cqe* cqe;
        io_uring_for_each_cqe(&ring_, head, cqe) {
            ++i;
            check_iou(cqe->res);

            auto* op = static_cast<Op*>(io_uring_cqe_get_data(cqe));
            ensure(op != nullptr);
            if (op && op->ctx) {
                op->res = cqe->res;
                op->flags = cqe->flags;
                if (--op->cqe_left == 0) {
                    wake(op->ctx); // enqueue the fiber
                }
            }
        }
        io_uring_cq_advance(&ring_, i);
        outstanding_io -= i;

        get_events++;
        if (i == 0) {
            // empty_after_drain++;
        }
    }
};


struct LibaioReactor : BaseReactor {
    uint64_t fiber_run = 0;
    uint64_t get_events = 0;

    // Minimal scheduler: resume until ring is empty. No requeue here.
    void run(bool& stop) {
        while (!stop) {
            Fiber* f;
            size_t n = ready_.size();
            for (int i = 0; i < n; ++i) {
                ensure(ready_.pop(f));
                f->peer = std::move(f->peer).resume_with([this, f](ctx::continuation&& back) noexcept {
                    fiber_current_ = f;
                    f->state = State::Running;
                    return std::move(back);
                });
                fiber_current_ = nullptr;
                fiber_run++;
            }
            drain_cqe();
        }
    }


    struct Op {
        Fiber* ctx = nullptr; // set by do_io()
        int res = 0;          // cqe->res
        unsigned flags = 0;   // cqe->flags

        // for batching
        int cqe_left = 1;
        // for fibers waiting on this completion
        Op* next = nullptr;
    };

    static constexpr int maxIOs = 1024;

    io_context_t ctx;
    iocb cb[maxIOs];
    // iocb* cbPtr[maxIOs];
    io_event events[maxIOs];
    std::vector<iocb*> batch;
    Stack<iocb*, maxIOs> free_cbs;

    int outstanding_io = 0;
    int total_io_fibers = 0;
    int to_submit = 0;
    int fibers_since_first_io = 0;
    uint64_t num_submits = 0;

    LibaioReactor(struct io_uring&) {
        memset(&ctx, 0, sizeof(io_context_t));
        check_ret(io_setup(maxIOs, &ctx));

        for (int i = 0; i < maxIOs; ++i) {
            free_cbs.push(&cb[i]);
        }
    }


    template <class Prep>
    inline int io(Op& op, Prep&& prep) {
        // RDTSCClock clock(2.4_GHz);
        // clock.start();

        op.ctx = current();
        op.res = 0;
        op.flags = 0;

        iocb* cb = free_cbs.pop();
        prep(cb);
        // io_prep_pwrite(cb + i, blockfd, &virtMem[pid], pageSize, pageSize * pid);
        cb->data = &op;
        batch.push_back(cb);

        outstanding_io += op.cqe_left;
        to_submit++;
        check_submit();

        // clock.stop();
        // io_cycles += clock.cycles();

        park();

        return op.res;
    }


    template <class Prep>
    int io_batch(int n, Op& op, Prep&& prep) {
        ensure(n > 0);

        // RDTSCClock clock(2.4_GHz);
        // clock.start();

        op.ctx = current();
        op.res = 0;
        op.flags = 0;
        op.cqe_left = n;

        // RDTSCClock clock(2.4_GHz);
        // clock.start();

        for (int i = 0; i < n; ++i) {
            iocb* cb = free_cbs.pop();
            prep(i, cb);
            cb->data = &op;
            batch.push_back(cb);
        }

        // io_uring_submit_and_wait(&ring_, n);
        outstanding_io += op.cqe_left;
        int ret = io_submit(ctx, batch.size(), batch.data());
        ensure(ret == batch.size());
        batch.clear();
        if constexpr (WRITE_RESETS) {
            ++num_submits;
            to_submit = 0; // Reset
            fibers_since_first_io = 0;
        }
        write_clock.stop();
        write_cycles += write_clock.cycles();

        // clock.stop();
        // io_cycles += clock.cycles();

        park();

        return op.res;
    }

    static constexpr auto& table = ExpTable<MAX_FIBERS>::values;

    static constexpr bool WRITE_RESETS = true;

    static constexpr jmp::static_branch<bool> submit_always = false;


    void check_submit() {
        if (to_submit == 0) {
            return;
        }

        // if constexpr (ALWAYS_SUBMIT) {
        if (submit_always) {
            int ret = io_submit(ctx, batch.size(), batch.data());
            ensure(ret == batch.size());
            batch.clear();
            to_submit = 0;
            ++num_submits;
            return;
        }

        if (to_submit > 0) {
            fibers_since_first_io++;
        }

        bool do_submit = false;
        if (fibers_since_first_io == total_io_fibers) {
            // Logger::info("first submit to_submit=", to_submit);
            do_submit = true;
        }

        if (!do_submit) {
            // int submit_prob = 1'000'000 / ((to_submit + 1) * 4);
            // int submit_prob = 1'000'000 * (table.at(fibers_since_first_io - to_submit) / table.at(2));
            // int submit_prob = 1'000'000 * (table.at(fibers_since_first_io - to_submit) / table.at(total_io_fibers / 2));
            int submit_prob = 1'000'000 * (table.at(fibers_since_first_io - to_submit) / table.at(total_io_fibers / 4));
            u64 rnd = RandomGenerator::getRand(0, 1'000'000);
            if (rnd <= submit_prob) {
                // Logger::info("submit batch=", to_submit);
                do_submit = true;
            }
        }

        if (do_submit) {
            int ret = io_submit(ctx, batch.size(), batch.data());
            ensure(ret == batch.size());
            batch.clear();
            ++num_submits;
            to_submit = 0;
            fibers_since_first_io = 0;
        }
    }


    void drain_cqe() {
        if (outstanding_io == 0) {
            return;
        }

        int ret = io_getevents(ctx, 0, maxIOs, events, nullptr);
        for (int i = 0; i < ret; ++i) {
            auto& event = events[i];
            auto* op = static_cast<Op*>(event.data);
            check_ret(event.res);
            if (op && op->ctx) {
                op->res = event.res;
                op->flags = 0;
                if (--op->cqe_left == 0) {
                    wake(op->ctx); // enqueue the fiber
                }
            }
            free_cbs.push(event.obj);
        }
        outstanding_io -= ret;

        get_events++;
    }
};


using Reactor = UringReactor;
// using Reactor = LibaioReactor;


namespace mini {

static constexpr bool LIBAIO = std::is_same_v<Reactor, LibaioReactor>;


inline thread_local Reactor* R = nullptr;
inline void set_reactor(Reactor& r) {
    R = &r;
}
inline Reactor::Fiber* current() {
    return R->current();
}
inline void yield() {
    R->yield();
}
inline void park() {
    R->park();
}
inline bool wake(Reactor::Fiber* f) {
    return R->wake(f);
}
template <class Fn>
inline void spawn(Fn&& fn) {
    R->spawn(std::forward<Fn>(fn));
}

using Op = Reactor::Op;

template <class Prep>
inline int io(Reactor::Op& op, Prep&& prep) {
    return R->io(op, std::forward<Prep>(prep));
}


template <class Prep>
inline int io_batch(int n, Reactor::Op& op, Prep&& prep) {
    return R->io_batch(n, op, std::forward<Prep>(prep));
}


inline void check_submit() {
    return R->check_submit();
}


struct Fiber {
    template <class Fn, class... Args>
    Fiber(Fn&& fn, Args&&... args) {
        ensure(R, "mini::set_reactor(r) must be called before constructing mini::Fiber");
        fiber = std::make_unique<Reactor::Fiber>();
        Reactor::Fiber* self = fiber.get();

        // bind args (perfect-forwarded) and pass to reactor as a zero-arg callable
        auto bound = [self,
                      fncap = std::forward<Fn>(fn),
                      tup = std::make_tuple(std::forward<Args>(args)...)]() mutable {
            invoke_dispatch(fncap, self, tup, std::make_index_sequence<sizeof...(Args)>{});
        };

        R->spawn(self, std::move(bound));
    }

private:
    std::unique_ptr<Reactor::Fiber> fiber;

    // helper: invoke fn either as fn(Fiber*, args...) or fn(args...)
    template <class Fn, class Tup, std::size_t... I>
    static void invoke_dispatch(Fn& fn, Reactor::Fiber* self, Tup& tup, std::index_sequence<I...>) {
        if constexpr (std::is_invocable_v<Fn&, Reactor::Fiber*, decltype(std::get<I>(tup))...>) {
            fn(self, std::get<I>(tup)...);
        } else {
            fn(std::get<I>(tup)...);
        }
    }
};

// Allocation-free await cell on the fiber stack.
template <class T, class Submit>
T await(Submit&& submit) {
    struct Cell {
        Reactor::Fiber* fiber;
        T result;
    } cell{R->current(), T{}};
    submit(&cell); // stash &cell in your I/O user_data
    R->park();
    return std::move(cell.result);
}
template <class Submit>
void await_void(Submit&& submit) {
    struct Cell {
        Reactor::Fiber* fiber;
    } cell{R->current()};
    submit(&cell);
    R->park();
}


//    // somewhere global/local:
// std::vector<std::function<void()>> cq; // completion queue
//
//// in a fiber:
// int value = mini::await<int>([&](auto* cell){
//     // schedule completion "later"
//     cq.emplace_back([cell]{
//         cell->result = 123;
//         mini::wake(cell->fiber);
//     });
// });
//
//// between r.run() calls, or in an idle hook:
// while (!cq.empty()) { auto fn = std::move(cq.back()); cq.pop_back(); fn(); }

} // namespace mini
