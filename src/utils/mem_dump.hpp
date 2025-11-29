#pragma once

#include <bitset>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>


// TODO endianness
struct MemoryDump {
    static constexpr size_t CACHELINE = 64;
    static constexpr size_t ADDR_BYTES = 8;

    MemoryDump(void* p, size_t size)
        : mem(reinterpret_cast<uint8_t*>(p)), size(size) {}


    template <typename T>
    auto& hex(size_t per_row = CACHELINE / sizeof(T), std::ostream& os = std::cout) {
        static_assert(std::is_integral<T>::value, "The type T must be an integral type.");

        printer<T>(os, per_row, [](std::ostream& os, uint8_t* addr) {
            os << "0x"
               << std::hex
               << std::noshowbase
               << std::setw(sizeof(T) * 2)
               << std::setfill('0')
               << +*reinterpret_cast<T*>(addr) << " ";
        });
        return *this;
    }


    template <typename T>
    auto& dec(size_t per_row = CACHELINE / sizeof(T), std::ostream& os = std::cout) {
        static_assert(std::is_integral<T>::value, "The type T must be an integral type.");

        printer<T>(os, per_row, [](std::ostream& os, uint8_t* addr) {
            os << std::dec
               << std::noshowbase
               << std::setw(sizeof(T) * 4) // guess, might not be optimal
               << std::setfill(' ')
               << +*reinterpret_cast<T*>(addr) << " ";
        });

        return *this;
    }


    template <typename T>
    auto& bit(size_t per_row = CACHELINE / sizeof(T), std::ostream& os = std::cout) {
        static_assert(std::is_integral<T>::value, "The type T must be an integral type.");

        printer<T>(os, per_row, [](std::ostream& os, uint8_t* addr) {
            std::bitset<sizeof(T) * 8> b(*reinterpret_cast<T*>(addr));
            os << "0b" << b << " ";
        });

        return *this;
    }

private:
    uint8_t* mem;
    size_t size;

    template <typename T, typename Fn>
    void printer(std::ostream& os, size_t per_row, Fn print_val) {
        std::ios_base::fmtflags fmtflags(os.flags());
        const size_t elements = size / sizeof(T);
        const size_t max_offset_indent = std::to_string(size).size();
        for (size_t i = 0; i < elements; ++i) {
            uint8_t* addr = mem + sizeof(T) * i;
            if (i % per_row == 0) {
                if (i != 0) {
                    os << "\n";
                }
                if (i == 0) {
                    os << "Address: 0x"
                       << std::hex
                       << std::noshowbase
                       << std::setw(ADDR_BYTES * 2)
                       << std::setfill('0')
                       << reinterpret_cast<uintptr_t>(addr)
                       << "\n";
                }
                os << "  +"
                   << std::dec
                   << std::setw(static_cast<int>(max_offset_indent))
                   << std::setfill(' ')
                   << (sizeof(T) * i) << "    "; // indent
            }

            print_val(os, addr);
        }
        os << "\n";
        os.flags(fmtflags);
    }
};
