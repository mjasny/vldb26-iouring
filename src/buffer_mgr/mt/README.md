# `buffer_mgr/mt`

This directory contains the standalone multithreaded buffer manager.

It is intentionally separate from [`src/buffer_mgr`](/home/matthias/repositories/ringding/src/buffer_mgr), which remains the single-threaded baseline.

The current implementation is no longer in bring-up mode. The important runtime pieces are in place, and the remaining work is targeted profiling/tuning rather than unfinished core design.

## Main Components

- [`BufferManager`](/home/matthias/repositories/ringding/src/buffer_mgr/mt/bm.hpp)
  - owns the global storage mappings, resident state arrays, free-page management, and worker coordination
- [`AtomicPageTable`](/home/matthias/repositories/ringding/src/buffer_mgr/mt/bm.hpp)
  - atomic open-addressed `pid -> bid` table
  - stores each slot as one packed atomic word
- [`PageState`](/home/matthias/repositories/ringding/src/buffer_mgr/mt/concurrency.hpp)
  - packed version/state word used for optimistic validation and shared/exclusive locking
  - also carries resident flags such as `dirty`, `evicting`, and `io_in_progress`
- [`BufferFrame`](/home/matthias/repositories/ringding/src/buffer_mgr/mt/bm.hpp)
  - per-frame runtime state
  - holds the current `pid`
  - holds the lock-free waiter stack for in-flight page reads
- [`BTree`](/home/matthias/repositories/ringding/src/buffer_mgr/mt/btree.hpp)
  - mt B-tree traversal and modification logic using optimistic lock coupling
- [`kuring.hpp`](/home/matthias/repositories/ringding/src/buffer_mgr/mt/kuring.hpp)
  - worker-local reactor and fiber runtime for `io_uring` and `libaio`

## Current Architecture

- hot metadata is split into:
  - page table: `pid -> bid`
  - resident state array: `PageState[bid]`
  - frame runtime state: `BufferFrame[bid]`
- the page table is not the concurrency object by itself
- `PageState` holds:
  - optimistic version/state
  - shared/exclusive lock state
  - marked state
  - packed `dirty`, `evicting`, and `io_in_progress` flags
- page faults and waits are handled locally inside `fixS` / `fixX` / `fixO`
  - full restarts are now reserved for validation-style conflicts
- in-flight read waiters use a lock-free waiter stack on `BufferFrame`
- storage supports multiple SSDs
  - `--ssds` accepts a vector
  - pages are distributed round-robin by `pid`

## Runtime Model

Each worker owns:

- one OS thread
- one reactor
- one fiber scheduler
- one I/O context
- one local free-page pool
- one inbound wake queue
- one eviction fiber

Fibers do not migrate between workers. Cross-worker wakeups are routed through explicit inbound wake queues.

Free frames are managed by:

- a small worker-local cache
- a sharded shared free-frame pool
- per-worker free-frame waiter queues with local-first wakeup

This replaced the earlier single global free-list and materially improved multiworker miss-heavy scaling. The current design also keeps free-frame waiters per worker so free returns can satisfy local parked fibers first.

### YCSB Variants

- The MT benchmark supports:
  - uniform YCSB via the default path
  - Zipf-skewed YCSB via `--ycsb_zipf_theta <theta>`
- `theta <= 0` uses the uniform path.
- `theta = 0.99` is already strongly skewed and should not be interpreted as “almost uniform”.

## Concurrency Model

### Guards

The mt B-tree uses:

- `GuardO` for optimistic traversal
- `GuardS` for shared access
- `GuardX` for exclusive access

Writers descend optimistically and only lock where needed.

- `GuardO -> GuardS` and `GuardO -> GuardX` reacquire by `pid`
- the concrete frame identity is taken from the reacquired page pointer, not from the optimistic guard
- `GuardS -> GuardX` is intentionally not supported
- validation failures use the mt restart path

### Scan Traversal

The current mt scan behavior now matches [`vmcache.cpp`](/home/matthias/repositories/ringding/vmcache.cpp) for ascending scans:

- `scanAsc()`
  - when a leaf is exhausted, mt continues directly through `nextLeafNode`
  - the shared fix on the next leaf uses the normal MT retry loop, so transient page-fault or lock conflicts do not truncate the scan
- `scanDesc()`
  - when a leaf is exhausted, mt still restarts from the root using the lower fence

So:

- ascending scan now follows the vmcache-style leaf chain
- descending scan is still root-based because there is no backward leaf link

### Page Faults

On a miss:

1. the worker looks up `pid` in the atomic page table
2. if absent, it allocates a free frame and tries to publish `pid -> bid`
3. exactly one worker wins the insert and issues the read
4. other workers register as waiters on the frame and park
5. completion publishes the readable resident state and wakes the detached waiter stack

The waiter path is lock-free at the `BufferFrame` level.

### Eviction

Eviction works by:

1. sweeping the global page-table clock
2. clearing second-chance marks
3. claiming candidates through `PageState`
4. writing back dirty pages if needed
5. erasing `pid -> bid`
6. recycling the frame

The current implementation uses a single global sweep under `evict_lock`.

Current limitation:

- eviction candidate selection is still serialized by the single global `evict_lock`
- a possible future optimization is true concurrent per-partition/local sweeps with finer-grained claiming or locking

## Scaling Status

Basic multiworker scaling has already been tested.

Recent miss-heavy YCSB profiling showed that the old single global free-list was a real bottleneck at high worker counts. Replacing it with sharded shared free pools improved the 64-worker 100% read case from roughly `~2.0M` to `~2.5M` tx/s. The remaining `16 -> 64` drop is smaller now and looks more like a normal miss-path/runtime ceiling than a pathological BM coordination failure.

The most effective later improvement was free-frame locality:

- keep a worker-local free cache
- wake that worker's free-frame waiters first when it returns frames
- refill proactively before the local cache is fully empty

That reduced remote free-frame wakeups substantially and is part of the current default design.

The remaining runtime work is:

- measure and compare `2/4/8` worker throughput on real workloads such as `ycsb`, `rndread`, and `tpcc`
- use the wait/restart sub-counters to localize the first scaling bottleneck
- tune only the measured bottleneck instead of doing more architectural churn

Possible future tuning targets if profiling points at them:

- sweep overhead
- page-table lookup hot path
- waiter-stack overhead under fault contention
- refill/steal policy across the sharded shared free pools
- local free-page ownership bias, where workers tend to keep frames they freed and other workers still have to steal/refill
- remaining free-frame waiter churn under heavy miss pressure
- inbound remote-wakeup drain, which still scans all source-worker channels once a destination has pending wakeups
- runtime submit/completion overhead in the miss path

## Tests

Important mt test binaries:

- [`buffer_mgr_mt_smoke_test`](/home/matthias/repositories/ringding/build/buffer_mgr_mt_smoke_test)
- [`buffer_mgr_mt_runtime_test`](/home/matthias/repositories/ringding/build/buffer_mgr_mt_runtime_test)
- [`buffer_mgr_mt_system_test`](/home/matthias/repositories/ringding/build/buffer_mgr_mt_system_test)
- [`buffer_mgr_mt_eviction_test`](/home/matthias/repositories/ringding/build/buffer_mgr_mt_eviction_test)
- [`buffer_mgr_mt_multiworker_eviction_test`](/home/matthias/repositories/ringding/build/buffer_mgr_mt_multiworker_eviction_test)
- [`buffer_mgr_mt_multiworker_progress_test`](/home/matthias/repositories/ringding/build/buffer_mgr_mt_multiworker_progress_test)
- [`buffer_mgr_mt_all_tests`](/home/matthias/repositories/ringding/build/buffer_mgr_mt_all_tests)

Run all mt tests:

```bash
./build/buffer_mgr_mt_all_tests --backend=all
```

Run a focused multiworker progress check:

```bash
./build/buffer_mgr_mt_multiworker_progress_test --backend=posix --workers=2
./build/buffer_mgr_mt_multiworker_progress_test --backend=uring --workers=4
```

## Notes

- The single-threaded code in [`src/buffer_mgr`](/home/matthias/repositories/ringding/src/buffer_mgr) remains the baseline for behavior and one-worker performance comparisons.
- MT B-tree debugging found real stale optimistic handovers caused by resident frame reuse (`PID -> BID -> frame` rebinding).
- The current MT code uses continuous per-slot resident versions for optimistic validation and exact `GuardO -> GuardS/X` handover.
- Every BID rebind bumps the same resident version word that `GuardO` captures, so a recycled frame slot forces validation restart without a separate `bind_gen`.
- This keeps the resident-frame identity check in one place: the per-slot resident version tracked by `PageState`.
- `AtomicPageTable` no longer tracks a separate `size_` counter in MT.
  - The runtime uses `physUsedCount` as the occupancy/budgeting signal.
  - Load/occupancy reporting uses `BufferManager::buffer_load()`, not page-table entry counting.
  - Sweep callers must pass an explicit size hint instead of relying on page-table entry counting.
- Optimistic readers still transition `Unlocked -> Marked`.
  - This is intentional in the current design: version bumps detect stale validation, but they do not stop a BID from being reclaimed and rebound while an optimistic reader is still traversing `pages[bid]`.
  - The preferred long-term direction is read-only optimistic validation plus deferred frame reuse, so readers no longer write shared page-state on the hot path.
- A more complete remove/merge implementation, including merged-page retirement and grace-period reclamation, was prototyped and then backed out.
  - It reduced normal throughput by roughly 2x in the current MT architecture.
  - If remove/merge reclamation is revisited, it should use a lower-overhead design instead of reintroducing the previous epoch-based approach.
