#pragma once

#include <cstddef>
#include <cstdint>
#include <numa.h>
#include <type_traits>

struct HugePages {
    static constexpr size_t PAGE_SIZE = 2 * 1024 * 1024;

    size_t size;
    void* addr;

    HugePages();
    HugePages(size_t size);
    HugePages(size_t size, int numa_node);

    ~HugePages();

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

    template <typename T>
    static auto malloc_array(size_t count) {
        auto size = sizeof(T) * count;
        return reinterpret_cast<T*>(malloc(size));
    }

    template <typename T>
    static auto free_array(T* ptr, size_t count) {
        auto size = sizeof(T) * count;
        free(ptr, size);
    }

    static void* malloc(size_t size);
    static void* malloc_file_backed(size_t size);

    static void* malloc_on_socket(size_t size, int numa_node);

    static void free(void* ptr, size_t size);

private:
    static constexpr size_t roundToPageSize(size_t size) {
        size_t remainder = size % PAGE_SIZE;
        return size + PAGE_SIZE - remainder;
    }
};
