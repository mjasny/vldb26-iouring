#pragma once
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#undef DEBUG

// Define logging levels
enum class LogLevel {
    NONE,
    ERROR,
    INFO,
    DEBUG,
};

// Set the compile-time log level
constexpr LogLevel LOG_LEVEL = LogLevel::DEBUG;

class Logger {
public:
    template <typename... Args>
    static void log(LogLevel level, const std::string& levelStr, Args&&... args) {
        if (level <= LOG_LEVEL) {
            std::cout << "[" << currentDateTime() << " " << levelStr << "] ";
            (std::cout << ... << std::forward<Args>(args)) << "\n";
        }
    }

    template <typename... Args>
    static void error(Args&&... args) {
        log(LogLevel::ERROR, "ERROR", std::forward<Args>(args)...);
    }

    template <typename... Args>
    static void info(Args&&... args) {
        log(LogLevel::INFO, "INFO", std::forward<Args>(args)...);
    }

    template <typename... Args>
    static void debug(Args&&... args) {
        log(LogLevel::DEBUG, "DEBUG", std::forward<Args>(args)...);
    }

    static void flush() {
        std::cout << std::flush;
    }

private:
    static std::string currentDateTime() {
        // Get the current time as a time_point
        auto now = std::chrono::system_clock::now();

        // Convert to time_t to extract the calendar time
        auto now_time_t = std::chrono::system_clock::to_time_t(now);

        // Convert to a tm structure for formatting
        std::tm now_tm;
        gmtime_r(&now_time_t, &now_tm); // Use gmtime_r for thread-safe conversion

        // Extract milliseconds
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now.time_since_epoch()) %
                      1000;

        // Format the time including milliseconds
        std::ostringstream oss;
        oss << std::put_time(&now_tm, "%Y-%m-%dT%H:%M:%S");
        oss << '.' << std::setw(3) << std::setfill('0') << now_ms.count() << 'Z';

        return oss.str();
    }
};
