#pragma once

#if defined(__ARM_NEON) || defined(__aarch64__)

#include <arm_neon.h>

namespace pod5 { namespace pdz {

// NEON implementation of the Rice / zig-zag mapping:
//   z = 2|d| - s(d),  implemented as (d + d) ^ (d >> 15).
class RiceMapNeon
{
public:
    static inline uint16x8_t encode_single_sample(const uint16x8_t x)
    {
        const auto doubled = vshlq_n_u16(x, 1);
        const auto sign_bits = vreinterpretq_u16_s16(vshrq_n_s16(vreinterpretq_s16_u16(x), 15));
        return veorq_u16(doubled, sign_bits);
    }

    static inline uint16x8_t decode_single_sample(const uint16x8_t x)
    {
        const auto shifted = vshrq_n_u16(x, 1);
        const auto low_bits = vandq_u16(x, vdupq_n_u16(0x1));
        const auto sign_mask = vsubq_u16(vdupq_n_u16(0), low_bits);
        return veorq_u16(shifted, sign_mask);
    }
};

}}  // namespace pod5::pdz

#endif
