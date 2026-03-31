#pragma once

#include "utils/my_asserts.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <liburing.h>
#include <sys/socket.h>

namespace remotescan {

constexpr uint32_t kAlignment = 4096;

enum class OpKind : uint8_t {
    ACCEPT = 1,
    RECV_REQ = 2,
    READ = 3,
    SEND = 4,
    RECV_PAYLOAD = 5,
    SEND_REQ = 6,
};

uint64_t encode_user_data(OpKind kind, uint32_t idx);
OpKind decode_kind(uint64_t user_data);
uint32_t decode_index(uint64_t user_data);

size_t round_up(size_t value, size_t alignment);
void* alloc_aligned(size_t size, size_t alignment = kAlignment);
bool has_cqe_notification(const io_uring_cqe* cqe);
void set_socket_buffer(int fd, int optname, int size);

struct AlignedBuffer {
    void* data = nullptr;
    size_t size = 0;

    AlignedBuffer() = default;
    explicit AlignedBuffer(size_t size);

    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;

    AlignedBuffer(AlignedBuffer&& other) noexcept;
    AlignedBuffer& operator=(AlignedBuffer&& other) noexcept;

    ~AlignedBuffer();

    template <typename T = std::byte>
    T* as() {
        return reinterpret_cast<T*>(data);
    }

    const void* ptr() const;
    void reset();
};

} // namespace remotescan
