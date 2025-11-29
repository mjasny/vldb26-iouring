#pragma once

#include "utils/nostd.hpp"

#include <cstring>
#include <functional>
#include <source_location>


namespace detail {

[[noreturn]] [[gnu::cold]] [[gnu::noinline]]
static void do_fail(const std::source_location& loc, const char* what) {
    // Use std::fprintf to avoid iostream bloat in hot code
    std::fprintf(stderr, "%s:%u: %s\n", loc.file_name(), loc.line(), what);
    throw std::runtime_error(what);
}

[[noreturn]] [[gnu::cold]] [[gnu::noinline]]
static void do_fail(const std::source_location& loc, const std::string& what) {
    do_fail(loc, what.c_str());
}

} // namespace detail


inline void ensure(bool value, const std::source_location& loc = std::source_location::current()) {
    if (value) [[likely]] {
        return;
    }
    detail::do_fail(loc, "does not evaluate to true");
}

inline void ensure(bool value, const char* what, const std::source_location& loc = std::source_location::current()) {
    if (value) [[likely]] {
        return;
    }
    detail::do_fail(loc, what);
}

template <typename Fn>
inline void ensure(bool value, Fn&& make_msg, const std::source_location& loc = std::source_location::current()) {
    if (value) [[likely]] {
        return;
    }
    detail::do_fail(loc, std::invoke(std::forward<Fn>(make_msg)));
}

// ---- helpers ----
template <class T>
inline constexpr bool is_ptr_like_v =
    std::is_pointer_v<std::decay_t<T>> || nostd::is_smart_ptr_v<std::decay_t<T>>;


template <typename T>
inline void check_ptr(T& ptr, const std::source_location& loc = std::source_location::current()) {
    static_assert(is_ptr_like_v<T>, "check_ptr: T must be raw or smart pointer");
    if (ptr != nullptr) [[likely]] {
        return;
    }
    detail::do_fail(loc, std::strerror(errno));
}


template <typename T>
inline void check_ret(T ret, const std::source_location& loc = std::source_location::current()) {
    if (ret >= 0) [[likely]] {
        return;
    }
    detail::do_fail(loc, std::strerror(errno));
}

template <typename T>
inline void check_zero(T ret, const std::source_location& loc = std::source_location::current()) {
    if (ret == 0) [[likely]] {
        return;
    }
    detail::do_fail(loc, std::strerror(errno));
}

inline void check_iou(int res, const std::source_location& loc = std::source_location::current()) {
    if (res >= 0) [[likely]] {
        return;
    }
    detail::do_fail(loc, std::strerror(-res));
}
