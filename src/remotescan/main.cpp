#include "remotescan/client.hpp"
#include "remotescan/config.hpp"
#include "remotescan/epoll_libaio.hpp"
#include "remotescan/server.hpp"
#include "remotescan/stats.hpp"

#include "utils/literals.hpp"
#include "utils/my_logger.hpp"

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace {

void report_summary(const remotescan::Config& cfg, const remotescan::WorkerStats& stats) {
    const auto elapsed = std::chrono::duration<double>(stats.end_tp - stats.start_tp).count();
    const double bandwidth = elapsed > 0 ? static_cast<double>(stats.bytes) / elapsed : 0.0;
    Logger::info("backend=", cfg.backend,
                 " mode=", cfg.mode,
                 " num_workers=", cfg.num_workers,
                 " time_s=", elapsed,
                 " bytes=", stats.bytes,
                 " ssd_read_bytes=", stats.ssd_read_bytes,
                 " send_bytes=", stats.send_bytes,
                 " recv_bytes=", stats.recv_bytes,
                 " bandwidth_Bps=", static_cast<uint64_t>(bandwidth),
                 " bandwidth_GiB_s=", bandwidth / double(1_GiB));
}

template <class Worker>
remotescan::WorkerStats run_workers(std::vector<std::unique_ptr<Worker>>& workers) {
    std::vector<std::jthread> threads;
    threads.reserve(workers.size());
    for (auto& worker : workers) {
        threads.emplace_back([&worker]() { worker->run(); });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    remotescan::WorkerStats total;
    for (const auto& worker : workers) {
        total = remotescan::aggregate_worker_stats(total, worker->stats());
    }
    return total;
}

template <class Worker>
remotescan::WorkerStats run_timed_workers(std::vector<std::unique_ptr<Worker>>& workers, std::chrono::milliseconds duration) {
    std::vector<std::jthread> threads;
    threads.reserve(workers.size());
    for (auto& worker : workers) {
        threads.emplace_back([&worker](std::stop_token stop_token) { worker->run(stop_token); });
    }

    std::this_thread::sleep_for(duration);
    for (auto& thread : threads) {
        thread.request_stop();
    }
    for (auto& thread : threads) {
        thread.join();
    }

    remotescan::WorkerStats total;
    for (const auto& worker : workers) {
        total = remotescan::aggregate_worker_stats(total, worker->stats());
    }
    return total;
}

} // namespace

int main(int argc, char** argv) {
    remotescan::Config cfg;
    cfg.parse(argc, argv);

    if (cfg.backend == remotescan::Backend::IO_URING) {
        if (cfg.mode == remotescan::Mode::SERVER) {
            std::vector<std::unique_ptr<remotescan::ServerWorker>> workers;
            for (size_t i = 0; i < cfg.num_workers; ++i) {
                workers.push_back(std::make_unique<remotescan::ServerWorker>(cfg, i));
            }
            report_summary(cfg, run_workers(workers));
        } else {
            std::vector<std::unique_ptr<remotescan::ClientWorker>> workers;
            for (size_t i = 0; i < cfg.num_workers; ++i) {
                workers.push_back(std::make_unique<remotescan::ClientWorker>(cfg, i));
            }
            if (remotescan::has_duration_target(cfg)) {
                report_summary(cfg, run_timed_workers(workers, std::chrono::milliseconds(cfg.duration)));
            } else {
                report_summary(cfg, run_workers(workers));
            }
        }
    } else {
        if (cfg.mode == remotescan::Mode::SERVER) {
            std::vector<std::unique_ptr<remotescan::EpollLibaioServer>> workers;
            for (size_t i = 0; i < cfg.num_workers; ++i) {
                workers.push_back(std::make_unique<remotescan::EpollLibaioServer>(cfg, i));
            }
            report_summary(cfg, run_workers(workers));
        } else {
            std::vector<std::unique_ptr<remotescan::EpollLibaioClient>> workers;
            for (size_t i = 0; i < cfg.num_workers; ++i) {
                workers.push_back(std::make_unique<remotescan::EpollLibaioClient>(cfg, i));
            }
            if (remotescan::has_duration_target(cfg)) {
                report_summary(cfg, run_timed_workers(workers, std::chrono::milliseconds(cfg.duration)));
            } else {
                report_summary(cfg, run_workers(workers));
            }
        }
    }
    return 0;
}
