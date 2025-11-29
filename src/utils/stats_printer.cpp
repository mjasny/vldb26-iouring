#include "stats_printer.hpp"

#include "utils.hpp"
#include "utils/cpu_map.hpp"
#include "utils/my_asserts.hpp"

#include <functional>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>
#include <vector>


StatsPrinter::Variable::Variable(std::string name, uint64_t& var, bool diff)
    : name(name), var(var), diff(diff) {}

uint64_t StatsPrinter::Variable::get() {
    if (!diff) {
        return var;
    }
    auto current = var;
    auto diff = current - last;
    last = current;
    last_diff = diff;
    return diff;
}

StatsPrinter::Constant::Constant(std::string name, uint64_t val)
    : name(name), val(val) {}


StatsPrinter::Scope::~Scope() {
    auto& stats = StatsPrinter::get();
    stats.unregister(ids);
}


void StatsPrinter::start() {
    if (interval == 0) {
        return;
    }
    const std::lock_guard<std::mutex> guard(mutex);
    if (thread.joinable()) {
        return;
    }
    thread = std::jthread([&](std::stop_token token) {
        CPUMap::get().unpin();
        thread_fn(token);
    });
    check_zero(pthread_setname_np(thread.native_handle(), "StatsPrinter"));
}

void StatsPrinter::stop() {
    if (thread.joinable()) {
        thread.request_stop();
        thread.join();
    }
}

void StatsPrinter::register_const(Scope& scope_guard, uint64_t val, std::string name) {
    const std::lock_guard<std::mutex> guard(mutex);
    auto [it, success] = constants.emplace(++var_id, Constant(name, val));
    ensure(success, "Insert failed");
    auto id = it->first;
    scope_guard.ids.push_back(id);
}

void StatsPrinter::register_var(Scope& scope_guard, uint64_t& var, std::string name, bool diff) {
    const std::lock_guard<std::mutex> guard(mutex);
    auto [it, success] = variables.emplace(++var_id, Variable(name, var, diff));
    ensure(success, "Insert failed");
    auto id = it->first;
    scope_guard.ids.push_back(id);
}

void StatsPrinter::register_func(Scope& scope_guard, fn_t fn) {
    const std::lock_guard<std::mutex> guard(mutex);
    auto [it, success] = functions.emplace(++var_id, fn);
    ensure(success, "Insert failed");
    auto id = it->first;
    scope_guard.ids.push_back(id);
}

void StatsPrinter::register_aggr(Scope& scope_guard, uint64_t& var, std::string name, bool diff) {
    const std::lock_guard<std::mutex> guard(mutex);
    auto [it, success] = aggregates.emplace(++var_id, Variable(name, var, diff));
    ensure(success, "Insert failed");
    auto id = it->first;
    aggregate_groups.emplace(name, id);
    scope_guard.ids.push_back(id);
}

// private:
void StatsPrinter::thread_fn(std::stop_token token) {
    uint64_t ts = 0;
    while (!token.stop_requested()) {
        auto start = std::chrono::high_resolution_clock::now();
        print(ts++);
        auto end = std::chrono::high_resolution_clock::now();

        auto duration = end - start;
        auto target = std::chrono::microseconds(interval) - duration;
        if (use_busy_sleep) {
            busy_sleep(target);
        } else {
            std::this_thread::sleep_for(target);
        }
    }
}

void StatsPrinter::unregister(std::vector<uint64_t> ids) {
    const std::lock_guard<std::mutex> guard(mutex);
    for (auto& id : ids) {
        if (variables.contains(id)) {
            ensure(variables.erase(id) == 1, "Delete failed");
        } else if (functions.contains(id)) {
            ensure(functions.erase(id) == 1, "Delete failed");
        } else if (constants.contains(id)) {
            ensure(constants.erase(id) == 1, "Delete failed");
        } else if (aggregates.contains(id)) {
            std::string name = aggregates.at(id).name;
            ensure(aggregates.erase(id) == 1, "Delete failed");
            // ensure(aggregate_groups.erase() == 1, "Delete failed");
            int erased = 0;
            auto range = aggregate_groups.equal_range(name);
            for (auto it = range.first; it != range.second; ++it) {
                if (it->second == id) {
                    aggregate_groups.erase(it);
                    ++erased;
                    break;
                }
            }
            ensure(erased == 1, "Delete failed");
        } else {
            ensure(false, "unknown id");
        }
    }
}


void StatsPrinter::print(uint64_t ts) {
    const std::lock_guard<std::mutex> guard(mutex);
    if (variables.size() == 0 && aggregates.size() == 0 && functions.size() == 0) {
        return;
    }
    std::stringstream ss;
    ss << "ts=" << ts;
    for (auto& [id, var] : constants) {
        ss << " " << var.name << "=" << var.val;
    }
    for (auto& [id, var] : variables) {
        ss << " " << var.name << "=" << var.get();
    }
    for (auto& [id, fn] : functions) {
        fn(ss);
    }

    for (auto it = aggregate_groups.begin(); it != aggregate_groups.end(); it = aggregate_groups.upper_bound(it->first)) {
        auto range = aggregate_groups.equal_range(it->first);

        uint64_t sum = 0;
        for (auto it = range.first; it != range.second; ++it) {
            sum += aggregates.at(it->second).get();
        }
        ss << " " << it->first << "=" << sum;
    }


    ss << "\n";
    std::cout << ss.str();
}
