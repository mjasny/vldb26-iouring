// Implementation split from types.hpp
#include "types.hpp"

unsigned fold(u8* writer, const Integer& x) {
    *reinterpret_cast<u32*>(writer) = __builtin_bswap32(x ^ (1ul << 31));
    return sizeof(x);
}

unsigned fold(u8* writer, const Timestamp& x) {
    *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
    return sizeof(x);
}

unsigned fold(u8* writer, const u64& x) {
    *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x);
    return sizeof(x);
}

unsigned unfold(const u8* input, Integer& x) {
    x = __builtin_bswap32(*reinterpret_cast<const u32*>(input)) ^ (1ul << 31);
    return sizeof(x);
}

unsigned unfold(const u8* input, Timestamp& x) {
    x = __builtin_bswap64(*reinterpret_cast<const u64*>(input)) ^ (1ull << 63);
    return sizeof(x);
}

unsigned unfold(const u8* input, u64& x) {
    x = __builtin_bswap64(*reinterpret_cast<const u64*>(input));
    return sizeof(x);
}

