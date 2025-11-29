#pragma once

#include "utils/cpu_map.hpp"
#include "utils/my_asserts.hpp"

#include <thread>


struct TimedStopper {
    TimedStopper() {
        stop_token = stop_source.get_token();
    }

    ~TimedStopper() {
        stop_source.request_stop();
        if (timer_thread.joinable()) {
            timer_thread.join();
        }
    }


    template <typename Duration>
    void after(Duration duration) {
        ensure(!timer_thread.joinable(), "timer already running");

        // Start a timer thread to stop after the given duration
        timer_thread = std::jthread([this, duration](std::stop_token) {
            CPUMap::get().unpin();

            std::this_thread::sleep_for(duration);
            stop_source.request_stop();
            triggered = true;
        });
    }

    // Check if the loop can continue running
    bool can_run() const {
        return !stop_token.stop_requested();
    }

    std::stop_token stop_token; // Token to check for stop conditions
    bool triggered = false;

private:
    std::stop_source stop_source; // Controls the stop request
    std::jthread timer_thread;    // Thread to manage the timing
};
