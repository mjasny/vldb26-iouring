// The MIT License (MIT)
//
// Copyright (c) 2024 Kris Jusiak <kris@jusiak.net>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
#pragma once
#pragma GCC system_header

extern "C" int getpagesize() throw();
extern "C" int mprotect(void*, __SIZE_TYPE__, int) throw();
extern "C" void* __start___jmp [[gnu::section("__jmp")]] [[gnu::weak]];
extern "C" void* __stop___jmp [[gnu::section("__jmp")]] [[gnu::weak]];

namespace jmp::inline v5_0_4 {
using u8 = __UINT8_TYPE__;
using u16 = __UINT16_TYPE__;
using u32 = __UINT32_TYPE__;
using u64 = __UINT64_TYPE__;
using size_t = __SIZE_TYPE__;

template <class T, size_t N>
struct array {
    static constexpr auto size() noexcept { return N; }
    [[nodiscard]] constexpr const auto& operator[](size_t index) const noexcept {
        return data[index];
    }
    T data[N]{};
};

template <size_t N>
struct [[gnu::packed]] entry {
    u64 size{};         /// sizeof(entry<N>)
    void* code{};       /// code memory (to be patched)
    u64 len{};          /// code length
    const void* self{}; /// self identifier
    u32 offsets[N]{};   /// jmp offsets
};

template <class T, T...>
struct static_branch;

#if defined(__x86_64__)
template <>
class static_branch<bool> final {
    using instr_t = array<u8, 5u>; /// sizeof(nop/jmp)
    using entry_t = entry<2u>;
    static constexpr instr_t NOP{0x0f, 0x1f, 0x44, 0x00, 0x00}; /// https://www.felixcloutier.com/x86/nop
    static constexpr instr_t JMP{0xe9, 0x00, 0x00, 0x00, 0x00}; /// https://www.felixcloutier.com/x86/jmp
    static_assert(sizeof(NOP) == sizeof(JMP));

public:
    constexpr explicit(false) static_branch(const bool value) noexcept {
        void failed();
        if (value)
            failed(); /// { false: nop, true: jmp }
    }
    constexpr static_branch(const static_branch&) noexcept = delete;
    constexpr static_branch(static_branch&&) noexcept = delete;
    constexpr static_branch& operator=(const static_branch&) noexcept = delete;
    constexpr static_branch& operator=(static_branch&&) noexcept = delete;

    inline const auto& operator=(const bool value) const noexcept {
        struct [[gnu::packed]] {
            u8 op{JMP[0u]};
            u32 offset{};
        } jmp{};
        const instr_t* ops[]{&NOP, (const instr_t*)&jmp};
        auto data = u64(&__start___jmp);
        while (data != u64(&__stop___jmp)) {
            const auto* entry = (const entry_t*)data;
            data += entry->size;
            if (entry->self != this)
                continue;
            jmp.offset = entry->offsets[size_t(value)];
            *static_cast<instr_t*>(entry->code) = *ops[size_t(value)];
        }
        return *this;
    }

    [[gnu::always_inline]] [[nodiscard]] inline explicit(false) operator bool() const noexcept {
        asm volatile goto(
            "0: \n"
            ".byte %c0, %c1, %c2, %c3, %c4 \n"
            ".pushsection __jmp, \"aw\" \n"
            ".quad %c7, 0b, %c5, %c6 \n"
            ".long 0, %l[_true] - (0b + %c5) \n"
            ".popsection \n"
            : : "i"(NOP[0]), "i"(NOP[1]), "i"(NOP[2]), "i"(NOP[3]), "i"(NOP[4]),
                "i"(sizeof(instr_t)),
                "i"(this),
                "i"(sizeof(entry_t))
            : : _true);
        return false;
    _true:
        return true;
    }
};

template <class T, T Min, T Max>
    requires requires(T t) { reinterpret_cast<T>(t); } and (Max - Min >= 2 and Max - Min <= 7)
class static_branch<T, Min, Max> final {
    using entry_t = entry<(Max - Min) + T(1)>;
    static constexpr u8 JMP[]{0xe9, 0x00, 0x00, 0x00, 0x00}; /// https://www.felixcloutier.com/x86/jmp

public:
    constexpr explicit(false) static_branch(const T value) noexcept {
        void failed();
        if (value != Min)
            failed();
    }
    constexpr static_branch(const static_branch&) noexcept = delete;
    constexpr static_branch(static_branch&&) noexcept = delete;
    constexpr static_branch& operator=(const static_branch&) noexcept = delete;
    constexpr static_branch& operator=(static_branch&&) noexcept = delete;

    inline const auto& operator=(const T value) const noexcept {
        auto data = u64(&__start___jmp);
        while (data != u64(&__stop___jmp)) {
            const auto* entry = (const entry_t*)data;
            data += entry->size;
            if (entry->self != this)
                continue;
            *(u32*)(entry->code) = entry->offsets[value - Min];
        }
        return *this;
    }

    [[gnu::always_inline]] [[nodiscard]] inline explicit(false) operator T() const noexcept
        requires(Max - Min == T(2))
    {
        asm volatile goto(
            "0: \n"
            ".byte %c0, %c1, %c2, %c3, %c4 \n"
            ".pushsection __jmp, \"aw\" \n"
            ".quad %c7, 1 + 0b, %c5, %c6 \n"
            ".long 0 \n"
            ".long %l[_1] - (1 + 0b + %c5) \n"
            ".long %l[_2] - (1 + 0b + %c5) \n"
            ".popsection \n"
            : : "i"(JMP[0]), "i"(JMP[1]), "i"(JMP[2]), "i"(JMP[3]), "i"(JMP[4]),
                "i"(sizeof(u32)),
                "i"(this),
                "i"(sizeof(entry_t))
            : : _1, _2);
        return T() + Min;
    _1:
        return T(1) + Min;
    _2:
        return T(2) + Min;
    }

    [[gnu::always_inline]] [[nodiscard]] inline explicit(false) operator T() const noexcept
        requires(Max - Min == T(3))
    {
        asm volatile goto(
            "0: \n"
            ".byte %c0, %c1, %c2, %c3, %c4 \n"
            ".pushsection __jmp, \"aw\" \n"
            ".quad %c7, 1 + 0b, %c5, %c6 \n"
            ".long 0 \n"
            ".long %l[_1] - (1 + 0b + %c5) \n"
            ".long %l[_2] - (1 + 0b + %c5) \n"
            ".long %l[_3] - (1 + 0b + %c5) \n"
            ".popsection \n"
            : : "i"(JMP[0]), "i"(JMP[1]), "i"(JMP[2]), "i"(JMP[3]), "i"(JMP[4]),
                "i"(sizeof(u32)),
                "i"(this),
                "i"(sizeof(entry_t))
            : : _1, _2, _3);
        return T() + Min;
    _1:
        return T(1) + Min;
    _2:
        return T(2) + Min;
    _3:
        return T(3) + Min;
    }

    [[gnu::always_inline]] [[nodiscard]] inline explicit(false) operator T() const noexcept
        requires(Max - Min == T(4))
    {
        asm volatile goto(
            "0: \n"
            ".byte %c0, %c1, %c2, %c3, %c4 \n"
            ".pushsection __jmp, \"aw\" \n"
            ".quad %c7, 1 + 0b, %c5, %c6 \n"
            ".long 0 \n"
            ".long %l[_1] - (1 + 0b + %c5) \n"
            ".long %l[_2] - (1 + 0b + %c5) \n"
            ".long %l[_3] - (1 + 0b + %c5) \n"
            ".long %l[_4] - (1 + 0b + %c5) \n"
            ".popsection \n"
            : : "i"(JMP[0]), "i"(JMP[1]), "i"(JMP[2]), "i"(JMP[3]), "i"(JMP[4]),
                "i"(sizeof(u32)),
                "i"(this),
                "i"(sizeof(entry_t))
            : : _1, _2, _3, _4);
        return T() + Min;
    _1:
        return T(1) + Min;
    _2:
        return T(2) + Min;
    _3:
        return T(3) + Min;
    _4:
        return T(4) + Min;
    }

    [[gnu::always_inline]] [[nodiscard]] inline explicit(false) operator T() const noexcept
        requires(Max - Min == T(5))
    {
        asm volatile goto(
            "0: \n"
            ".byte %c0, %c1, %c2, %c3, %c4 \n"
            ".pushsection __jmp, \"aw\" \n"
            ".quad %c7, 1 + 0b, %c5, %c6 \n"
            ".long 0 \n"
            ".long %l[_1] - (1 + 0b + %c5) \n"
            ".long %l[_2] - (1 + 0b + %c5) \n"
            ".long %l[_3] - (1 + 0b + %c5) \n"
            ".long %l[_4] - (1 + 0b + %c5) \n"
            ".long %l[_5] - (1 + 0b + %c5) \n"
            ".popsection \n"
            : : "i"(JMP[0]), "i"(JMP[1]), "i"(JMP[2]), "i"(JMP[3]), "i"(JMP[4]),
                "i"(sizeof(u32)),
                "i"(this),
                "i"(sizeof(entry_t))
            : : _1, _2, _3, _4, _5);
        return T() + Min;
    _1:
        return T(1) + Min;
    _2:
        return T(2) + Min;
    _3:
        return T(3) + Min;
    _4:
        return T(4) + Min;
    _5:
        return T(5) + Min;
    }

    [[gnu::always_inline]] [[nodiscard]] inline explicit(false) operator T() const noexcept
        requires(Max - Min == T(6))
    {
        asm volatile goto(
            "0: \n"
            ".byte %c0, %c1, %c2, %c3, %c4 \n"
            ".pushsection __jmp, \"aw\" \n"
            ".quad %c7, 1 + 0b, %c5, %c6 \n"
            ".long 0 \n"
            ".long %l[_1] - (1 + 0b + %c5) \n"
            ".long %l[_2] - (1 + 0b + %c5) \n"
            ".long %l[_3] - (1 + 0b + %c5) \n"
            ".long %l[_4] - (1 + 0b + %c5) \n"
            ".long %l[_5] - (1 + 0b + %c5) \n"
            ".long %l[_6] - (1 + 0b + %c5) \n"
            ".popsection \n"
            : : "i"(JMP[0]), "i"(JMP[1]), "i"(JMP[2]), "i"(JMP[3]), "i"(JMP[4]),
                "i"(sizeof(u32)),
                "i"(this),
                "i"(sizeof(entry_t))
            : : _1, _2, _3, _4, _5, _6);
        return T() + Min;
    _1:
        return T(1) + Min;
    _2:
        return T(2) + Min;
    _3:
        return T(3) + Min;
    _4:
        return T(4) + Min;
    _5:
        return T(5) + Min;
    _6:
        return T(6) + Min;
    }

    [[gnu::always_inline]] [[nodiscard]] inline explicit(false) operator T() const noexcept
        requires(Max - Min == T(7))
    {
        asm volatile goto(
            "0: \n"
            ".byte %c0, %c1, %c2, %c3, %c4 \n"
            ".pushsection __jmp, \"aw\" \n"
            ".quad %c7, 1 + 0b, %c5, %c6 \n"
            ".long 0 \n"
            ".long %l[_1] - (1 + 0b + %c5) \n"
            ".long %l[_2] - (1 + 0b + %c5) \n"
            ".long %l[_3] - (1 + 0b + %c5) \n"
            ".long %l[_4] - (1 + 0b + %c5) \n"
            ".long %l[_5] - (1 + 0b + %c5) \n"
            ".long %l[_6] - (1 + 0b + %c5) \n"
            ".long %l[_7] - (1 + 0b + %c5) \n"
            ".popsection \n"
            : : "i"(JMP[0]), "i"(JMP[1]), "i"(JMP[2]), "i"(JMP[3]), "i"(JMP[4]),
                "i"(sizeof(u32)),
                "i"(this),
                "i"(sizeof(entry_t))
            : : _1, _2, _3, _4, _5, _6, _7);
        return T() + Min;
    _1:
        return T(1) + Min;
    _2:
        return T(2) + Min;
    _3:
        return T(3) + Min;
    _4:
        return T(4) + Min;
    _5:
        return T(5) + Min;
    _6:
        return T(6) + Min;
    _7:
        return T(7) + Min;
    }
};
#endif

/**
 * Makes required pages writable for code patching
 * Note: Must be called before changing the branch value (`branch = ...`)
 *       Should be called once at the startup
 * @param page_size page size (default: getpagesize())
 * @param permissions protect permissions (default: PROT_READ | PROT_WRITE | PROT_EXEC)
 * @return true if succesful, false on error (errno is set to indicate the error)
 */
[[nodiscard]] static inline auto init(const u64 page_size = getpagesize(), const u64 permissions = 0b111) noexcept -> bool {
    using entry_t = entry<0u>;
    auto data = u64(&__start___jmp);
    while (data != u64(&__stop___jmp)) {
        const auto* entry = (const entry_t*)data;
        data += entry->size;
        if (const auto memory = u64(entry->code) & ~(page_size - 1u);
            mprotect((void*)memory, u64(entry->code) - memory + entry->len, permissions)) {
            return false;
        }
    }
    return true;
}
} // namespace jmp::inline v5_0_4

#ifndef NTEST
namespace jmp::inline v5_0_4 {
inline auto test() {
    static_assert(([] {
        constexpr auto expect = [](bool cond) {
            if (not cond) {
                void failed();
                failed();
            }
        };

        // jmp::array
        {
            {
                jmp::array<int, 1u> array{42};
                expect(1u == array.size());
                expect(42 == array[0u]);
            }

            {
                jmp::array<jmp::u32, 2u> array{4u, 2u};
                expect(2u == array.size());
                expect(4u == array[0u]);
                expect(2u == array[1u]);
            }
        }

        // jmp::static_branch
        {
            static_assert(1u == sizeof(jmp::static_branch<bool>));
            static_assert(1u == sizeof(jmp::static_branch<jmp::u8, 0, 2>));
            static_assert(1u == sizeof(jmp::static_branch<jmp::u16, 0, 3>));
            static_assert(1u == sizeof(jmp::static_branch<jmp::u32, 1, 4>));
            static_assert(1u == sizeof(jmp::static_branch<jmp::u64, 2, 7>));
            static_assert(1u == sizeof(jmp::static_branch<jmp::size_t, 1, 5>));

            static_assert(not[](auto... ts) { return requires { jmp::static_branch<bool>{ts...}; }; }());
            static_assert(not[](auto... ts) { return requires { jmp::static_branch<bool>{ts...}; }; }(jmp::static_branch<bool>{false}));
            static_assert([](auto value) { return requires { jmp::static_branch<bool>{value}; }; }(false));

            static_assert(not[](auto... ts) { return requires { jmp::static_branch<jmp::u32, 0, 2>{ts...}; }; }());
            static_assert(not[](auto... ts) { return requires { jmp::static_branch<jmp::u32, 0, 3>{ts...}; }; }(jmp::static_branch<jmp::u32, 0, 3>{0u}));
            static_assert([](auto value) { return requires { jmp::static_branch<jmp::u32, 1, 5>{value}; }; }(0u));
        }
    }(),
                   true));

    constexpr auto expect = [](bool cond) {
        if (not cond) {
            __builtin_abort();
        }
    };

    static constexpr jmp::static_branch<bool> b = false;

    auto fn_bool = [&] {
        if (b) {
            return 42;
        } else {
            return 0;
        }
    };

    static constexpr jmp::static_branch<int, 0, 2> i = 0;

    auto fn_int = [&] {
        switch (i) {
            default:
                return 0;
            case 0:
                return 42;
            case 1:
                return 99;
            case 2:
                return 123;
        }
    };

    (void)jmp::init();

    {
        expect(0 == fn_bool());

        b = false;
        expect(0 == fn_bool());

        b = true;
        expect(42 == fn_bool());
    }

    {
        expect(42 == fn_int());

        i = 0;
        expect(42 == fn_int());

        i = 1;
        expect(99 == fn_int());

        i = 2;
        expect(123 == fn_int());
    }
}
} // namespace jmp::inline v5_0_4
#endif // NTEST
