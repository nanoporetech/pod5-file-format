#pragma once

#include "common.hpp"  // architecture macros

#if defined(SVB16_X64)

#ifdef _MSC_VER
#include <intrin.h>
#endif

// __AVX__ is documented for MSVC, but __SSE4_1__ isn't
#if defined(__AVX__) || defined(__SSE4_1__)

inline constexpr bool has_ssse3() { return true; }

inline constexpr bool has_sse4_1() { return true; }

#else

struct CpuidResult {
    unsigned int eax;
    unsigned int ebx;
    unsigned int ecx;
    unsigned int edx;
};

inline CpuidResult cpuid(unsigned int leaf, unsigned int subleaf)
{
#ifdef _MSC_VER
    int info[4];
    __cpuidex(info, static_cast<int>(leaf), static_cast<int>(subleaf));
    return CpuidResult{
        static_cast<unsigned int>(info[0]),
        static_cast<unsigned int>(info[1]),
        static_cast<unsigned int>(info[2]),
        static_cast<unsigned int>(info[3]),
    };
#else
    CpuidResult info;
    asm("cpuid\n\t"
        : "=a"(info.eax), "=b"(info.ebx), "=c"(info.ecx), "=d"(info.edx)
        : "0"(leaf), "2"(subleaf));
    return info;
#endif
}

inline unsigned int cpuid_leaf1_ecx()
{
    // using C++11 atomic static variables
    static unsigned int const ecx = cpuid(1, 0).ecx;
    return ecx;
}

#if defined(__SSSE3__)
inline constexpr bool has_ssse3() { return true; }
#else
inline bool has_ssse3() { return (cpuid_leaf1_ecx() & (1 << 9)) != 0; }
#endif

inline bool has_sse4_1() { return (cpuid_leaf1_ecx() & (1 << 19)) != 0; }

#endif  // defined(__SSE4_1__)
#endif  // defined(SVB16_X64)
