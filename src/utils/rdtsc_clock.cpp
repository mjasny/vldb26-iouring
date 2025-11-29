#include "rdtsc_clock.hpp"

#include <x86intrin.h>


RDTSCClock::RDTSCClock(uint64_t cpu_frequency_hz) : cpu_frequency_hz(cpu_frequency_hz) {}

uint64_t RDTSCClock::start() {
    // Serialize execution to ensure accurate timing
    //_mm_lfence();
    // start_cycles = __rdtsc();

    unsigned cyc_high, cyc_low;
    __asm volatile("cpuid\n\t"
                   "rdtsc\n\t"
                   "mov %%edx, %0\n\t"
                   "mov %%eax, %1\n\t"
                   : "=r"(cyc_high), "=r"(cyc_low)::"%rax", "%rbx", "%rcx",
                     "%rdx");
    start_cycles = ((uint64_t)cyc_high << 32) | cyc_low;

    return start_cycles;
}

uint64_t RDTSCClock::stop() {
    // Serialize execution to ensure all instructions have completed
    //_mm_lfence();
    // end_cycles = __rdtsc();
    // return end_cycles;

    unsigned cyc_high, cyc_low;
    __asm volatile("rdtscp\n\t"
                   "mov %%edx, %0\n\t"
                   "mov %%eax, %1\n\t"
                   "cpuid\n\t"
                   : "=r"(cyc_high), "=r"(cyc_low)::"%rax", "%rbx", "%rcx",
                     "%rdx");
    end_cycles = ((uint64_t)cyc_high << 32) | cyc_low;
    return end_cycles;
}

uint64_t RDTSCClock::read() {
    // Serialize execution to ensure all instructions have completed
    _mm_lfence();
    return __rdtsc();
}

uint64_t RDTSCClock::cycles() const {
    return end_cycles - start_cycles;
}
