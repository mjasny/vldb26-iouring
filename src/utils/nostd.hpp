#pragma once

#include <memory>
#include <string>
#include <type_traits>

namespace nostd {

#if !defined(__CUDACC__)
// std::visit
// helper type for the visitor #4
template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
// explicit deduction guide (not needed as of C++20)
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;
#endif

// static_assert(sizeof_equals_v<ycsb_t, 136>);
template <typename T, std::size_t expected, std::size_t actual = sizeof(T)>
struct sizeof_equals {
    static_assert(expected == actual, "sizeof_equals<> failed!");
    static constexpr bool value = (expected == actual);
};

template <typename T, std::size_t size>
inline constexpr bool sizeof_equals_v = sizeof_equals<T, size>::value;


// std::is_pointer does not work for smart-ptrs

template <class T>
struct is_unique_ptr : std::false_type {};

template <class T>
struct is_unique_ptr<std::unique_ptr<T>> : std::true_type {};

template <class T>
struct is_shared_ptr : std::false_type {};

template <class T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};


template <typename T>
inline constexpr bool is_unique_ptr_v = is_unique_ptr<T>::value;

template <typename T>
inline constexpr bool is_shared_ptr_v = is_shared_ptr<T>::value;

template <typename T>
inline constexpr bool is_smart_ptr_v = is_unique_ptr<T>::value || is_shared_ptr<T>::value;


inline uint8_t stou8(const std::string& s) {
    int i = std::stoi(s);
    if (i <= static_cast<int>(UINT8_MAX) && i >= 0) {
        return static_cast<uint8_t>(i);
    }
    throw std::out_of_range("stou8() failed");
}

inline uint16_t stou16(const std::string& s) {
    int i = std::stoi(s);
    if (i <= static_cast<int>(UINT16_MAX) && i >= 0) {
        return static_cast<uint16_t>(i);
    }
    throw std::out_of_range("stou16() failed");
}

// https://en.cppreference.com/w/cpp/experimental/make_array
template <typename... T>
constexpr auto make_array(T&&... values) -> std::array<
                                             typename std::decay<typename std::common_type<T...>::type>::type,
                                             sizeof...(T)> {
    return std::array<
        typename std::decay<typename std::common_type<T...>::type>::type,
        sizeof...(T)>{std::forward<T>(values)...};
}
// Wrote this for Tobi
template <class T>
using expr_type = std::remove_cv_t<std::remove_reference_t<T>>;

template <typename T, typename R, R T::*member>
struct has_member {
    using member_type = expr_type<decltype(T{}.*member)>;

    static constexpr bool value = std::is_same<expr_type<R>, member_type>::value;
};


} // namespace nostd
