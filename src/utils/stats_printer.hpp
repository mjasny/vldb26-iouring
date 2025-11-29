#pragma once

#include "utils/singleton.hpp"

#include <functional>
#include <map>
#include <sstream>
#include <thread>
#include <vector>


struct StatsPrinter : Singleton<StatsPrinter> {
    struct Variable {
        std::string name;
        uint64_t& var;
        bool diff;
        uint64_t last = 0;
        uint64_t last_diff = 0;

        Variable(std::string name, uint64_t& var, bool diff);

        uint64_t get();
    };

    struct Constant {
        std::string name;
        uint64_t val;

        Constant(std::string name, uint64_t val);
    };

    class Scope {
        friend StatsPrinter;
        std::vector<uint64_t> ids;

    public:
        Scope() = default;

        ~Scope();

        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;

        Scope(Scope&&) = default;
        Scope& operator=(Scope&&) = default;
    };

    using fn_t = std::function<void(std::stringstream& ss)>;

    std::jthread thread;
    std::mutex mutex;
    uint64_t var_id = 0;
    std::map<uint64_t, Variable> variables;
    std::map<uint64_t, fn_t> functions;
    std::map<uint64_t, Constant> constants;
    std::map<uint64_t, Variable> aggregates;
    std::multimap<std::string, uint64_t> aggregate_groups;

    uint64_t interval = 1'000'000; // microseconds
    bool use_busy_sleep = false;   // good for small intervals

    StatsPrinter() = default;

    void start();

    void stop();

    void register_const(Scope& scope_guard, uint64_t val, std::string name);

    void register_var(Scope& scope_guard, uint64_t& var, std::string name, bool diff = true);

    void register_func(Scope& scope_guard, fn_t fn);

    void register_aggr(Scope& scope_guard, uint64_t& var, std::string name, bool diff = true);

private:
    void thread_fn(std::stop_token token);

    void unregister(std::vector<uint64_t> ids);

    void print(uint64_t ts);
};
