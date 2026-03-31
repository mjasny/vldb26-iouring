#include "remotescan/common.hpp"

namespace remotescan {

uint64_t encode_user_data(OpKind kind, uint32_t idx) {
    return (static_cast<uint64_t>(kind) << 56) | idx;
}

OpKind decode_kind(uint64_t user_data) {
    return static_cast<OpKind>((user_data >> 56) & 0xff);
}

uint32_t decode_index(uint64_t user_data) {
    return static_cast<uint32_t>(user_data & 0x00ff'ffff'ffff'ffffULL);
}

size_t round_up(size_t value, size_t alignment) {
    return ((value + alignment - 1) / alignment) * alignment;
}

void* alloc_aligned(size_t size, size_t alignment) {
    void* ptr = nullptr;
    check_ret(posix_memalign(&ptr, alignment, round_up(size, alignment)));
    std::memset(ptr, 0, round_up(size, alignment));
    return ptr;
}

bool has_cqe_notification(const io_uring_cqe* cqe) {
    return (cqe->flags & IORING_CQE_F_NOTIF) != 0;
}

void set_socket_buffer(int fd, int optname, int size) {
    if (size <= 0) {
        return;
    }
    check_ret(setsockopt(fd, SOL_SOCKET, optname, &size, sizeof(size)));
}

AlignedBuffer::AlignedBuffer(size_t size)
    : data(alloc_aligned(size)), size(round_up(size, kAlignment)) {
}

AlignedBuffer::AlignedBuffer(AlignedBuffer&& other) noexcept
    : data(other.data), size(other.size) {
    other.data = nullptr;
    other.size = 0;
}

AlignedBuffer& AlignedBuffer::operator=(AlignedBuffer&& other) noexcept {
    if (this != &other) {
        reset();
        data = other.data;
        size = other.size;
        other.data = nullptr;
        other.size = 0;
    }
    return *this;
}

AlignedBuffer::~AlignedBuffer() {
    reset();
}

const void* AlignedBuffer::ptr() const {
    return data;
}

void AlignedBuffer::reset() {
    if (data) {
        std::free(data);
        data = nullptr;
        size = 0;
    }
}

} // namespace remotescan
