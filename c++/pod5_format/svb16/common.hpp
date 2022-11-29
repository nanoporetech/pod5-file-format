#pragma once

#if __cplusplus >= 201703L
#define SVB16_IF_CONSTEXPR if constexpr
#else
#define SVB16_IF_CONSTEXPR if
#endif

#ifdef _MSC_VER
#define SVB_RESTRICT __restrict
#else
#define SVB_RESTRICT __restrict__
#endif

#if defined(__x86_64__) || defined(_M_AMD64)  // x64
#define SVB16_X64
#elif defined(__arm__) || defined(__aarch64__)
#define SVB16_ARM
#endif

#ifndef __has_builtin
#define __has_builtin(x) 0
#endif

#if __has_builtin(__builtin_popcount)
// likely to be a single instruction (POPCNT) on x86_64
#define svb16_popcount __builtin_popcount
#else
// optimising compilers can often convert this pattern to POPCNT on x86_64
inline int svb16_popcount(unsigned int i)
{
    i = i - ((i >> 1) & 0x55555555);                 // add pairs of bits
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333);  // quads
    i = (i + (i >> 4)) & 0x0F0F0F0F;                 // groups of 8
    return (i * 0x01010101) >> 24;                   // horizontal sum of bytes
}
#endif
