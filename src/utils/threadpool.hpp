#pragma once

#include <thread>
#include <vector>

class ThreadPool {
    std::vector<std::jthread> threads;

public:
    template <typename Fn>
    void parallel_n(int n, Fn&& fn) {
        for (int i = 0; i < n; ++i) {
            threads.emplace_back(std::jthread(fn, i));
        }
    }

    template <typename Rep, typename Period>
    void join_after(std::chrono::duration<Rep, Period> d) {
        std::this_thread::sleep_for(d);
        for (auto& t : threads) {
            t.request_stop();
        }
        for (auto& t : threads) {
            t.join();
        }
    }

    void join() {
        for (auto& t : threads) {
            t.request_stop();
        }
        for (auto& t : threads) {
            t.join();
        }
    }
};