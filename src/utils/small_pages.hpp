#pragma once

#include <cstddef>
#include <cstdint>
#include <numa.h>
#include <type_traits>

struct SmallPages {
    static constexpr size_t PAGE_SIZE = 4 * 1024;

    size_t size;
    void* addr;

    SmallPages(size_t size);
    ~SmallPages();


    template <typename T>
    auto as() {
        static_assert(std::is_pointer_v<T>, "T must be a pointer");
        return reinterpret_cast<T>(addr);
    }

    template <typename T>
    auto offset_as(size_t offset) {
        static_assert(std::is_pointer_v<T>, "T must be a pointer");
        return reinterpret_cast<T>(reinterpret_cast<uint8_t*>(addr) + offset);
    }

    static void* malloc(size_t size);

    static void free(void* ptr, size_t size);

private:
    static constexpr size_t roundToPageSize(size_t size) {
        size_t remainder = size % PAGE_SIZE;
        return size + PAGE_SIZE - remainder;
    }
};