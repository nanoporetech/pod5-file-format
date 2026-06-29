#pragma once

#include <cstddef>
#include <cstdint>

#if defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64)
#include <immintrin.h>
#endif

namespace pod5 { namespace pdz {

// Rice / zig-zag mapping of the difference sequence:
//   z = 2|d| - s(d),  implemented as (d + d) ^ (d >> 15).
// The scalar overloads are also used by the scalar reference kernel, so they
// must compile on every target.
class RiceMapSse
{
public:
#if defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64)
    static inline __m128i encode_single_sample(const __m128i & x);
    static inline __m128i decode_single_sample(const __m128i & x);
#endif
    static constexpr inline uint16_t encode_single_sample(const int16_t x);
    static constexpr inline int16_t decode_single_sample(const uint16_t x);
};

#if defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64)
inline __m128i RiceMapSse::encode_single_sample(const __m128i & x)
{
    return _mm_xor_si128(_mm_add_epi16(x, x), _mm_srai_epi16(x, 16));
}

inline const __m128i lowest_bit_mask_128_ = _mm_set1_epi16(0x01);

inline __m128i RiceMapSse::decode_single_sample(const __m128i & x)
{
    return _mm_xor_si128(
        _mm_srli_epi16(x, 1),
        _mm_sub_epi16(_mm_setzero_si128(), _mm_and_si128(x, lowest_bit_mask_128_)));
}
#endif

constexpr uint16_t RiceMapSse::encode_single_sample(const int16_t x)
{
    return static_cast<uint16_t>((x + x) ^ static_cast<uint16_t>(static_cast<int16_t>(x) >> 15));
}

constexpr int16_t RiceMapSse::decode_single_sample(const uint16_t x)
{
    return static_cast<int16_t>((x >> 1) ^ static_cast<uint16_t>(0 - (x & 1)));
}

}}  // namespace pod5::pdz
