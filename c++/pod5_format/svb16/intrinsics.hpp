#pragma once

#include "common.hpp"  // architecture macros

#if defined(_MSC_VER)
#include <intrin.h>
#elif defined(__GNUC__) && defined(SVB16_X64)
#include <x86intrin.h>
#elif defined(__GNUC__) && defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

#include <cstdint>

namespace svb16 { namespace detail {
[[gnu::target("sse2")]] inline constexpr __m128i m128i_from_bytes(
    uint8_t a,
    uint8_t b,
    uint8_t c,
    uint8_t d,
    uint8_t e,
    uint8_t f,
    uint8_t g,
    uint8_t h,
    uint8_t i,
    uint8_t j,
    uint8_t k,
    uint8_t l,
    uint8_t m,
    uint8_t n,
    uint8_t o,
    uint8_t p)
{
#ifdef _MSC_VER
    return __m128i{
        (char)a,
        (char)b,
        (char)c,
        (char)d,
        (char)e,
        (char)f,
        (char)g,
        (char)h,
        (char)i,
        (char)j,
        (char)k,
        (char)l,
        (char)m,
        (char)n,
        (char)o,
        (char)p};
#else
    return __m128i{
        static_cast<int64_t>(static_cast<uint64_t>(h) << 56) + (static_cast<int64_t>(g) << 48)
            + (static_cast<int64_t>(f) << 40) + (static_cast<int64_t>(e) << 32)
            + (static_cast<int64_t>(d) << 24) + (static_cast<int64_t>(c) << 16)
            + (static_cast<int64_t>(b) << 8) + static_cast<int64_t>(a),
        static_cast<int64_t>(static_cast<uint64_t>(h) << 56) + (static_cast<int64_t>(g) << 48)
            + (static_cast<int64_t>(f) << 40) + (static_cast<int64_t>(e) << 32)
            + (static_cast<int64_t>(d) << 24) + (static_cast<int64_t>(c) << 16)
            + (static_cast<int64_t>(b) << 8) + static_cast<int64_t>(a)};
#endif
}
}}  // namespace svb16::detail
