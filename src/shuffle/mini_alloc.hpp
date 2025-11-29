#pragma once


#include "utils/my_asserts.hpp"

#include <cstdint>
#include <sstream>
#include <utility>


class MiniAlloc {
    uintptr_t offset = 0;
    size_t max_size = 0;
    uint8_t* mem;

public:
    MiniAlloc(void* mem, size_t max_size) : MiniAlloc(mem, max_size, 0) {}

    MiniAlloc(void* mem, size_t max_size, size_t offset)
        : offset(offset), max_size(max_size), mem(reinterpret_cast<uint8_t*>(mem)) {}


    auto allocate(size_t size) {
        auto* local_ptr = reinterpret_cast<void*>(mem + offset);
        size_t remote_offset = offset;
        offset += size;
        ensure(offset <= max_size, [&] {
            std::stringstream ss;
            ss << "Out of bounds by " << offset - max_size << " bytes";
            return ss.str();
        });
        return std::pair(local_ptr, remote_offset);
    }

    template <typename T, typename... Args>
    auto create_flexible(Args&&... args, size_t total_size) {
        constexpr size_t alignment = alignof(T);
        auto [local_ptr, remote_offset] = allocate_memory(alignment, total_size);
        new (local_ptr) T{std::forward<Args>(args)...};
        return std::pair(reinterpret_cast<T*>(local_ptr), remote_offset);
    }

    template <typename T>
    auto allocate_array(size_t items) {
        constexpr size_t alignment = alignof(T);
        auto [local_ptr, remote_offset] = allocate_memory(alignment, sizeof(T) * items);
        return std::pair(reinterpret_cast<T*>(local_ptr), remote_offset);
    }


    template <typename T, typename... Args>
    auto create(Args&&... args) {
        constexpr size_t alignment = alignof(T);
        auto [local_ptr, remote_offset] = allocate_memory(alignment, sizeof(T));
        new (local_ptr) T{std::forward<Args>(args)...};
        return std::pair(reinterpret_cast<T*>(local_ptr), remote_offset);
    }

private:
    auto allocate_memory(size_t alignment, size_t size) -> std::pair<void*, size_t> {
        size_t padding = (alignment - (offset % alignment)) % alignment;
        offset += padding;

        void* local_ptr = mem + offset;
        size_t remote_offset = offset;
        offset += size;

        ensure(offset <= max_size, [&] {
            std::stringstream ss;
            ss << "Out of bounds by " << offset - max_size << " bytes";
            return ss.str();
        });

        return std::pair(local_ptr, remote_offset);
    }
};
