
#include "streamvbyte_isadetection.h"
#include "streamvbyte_shuffle_tables_encode_16.h"

#include <stdio.h>
#include <string.h>

#ifdef STREAMVBYTE_X64

STREAMVBYTE_TARGET_SSSE3
static __m128i Delta(__m128i curr, __m128i prev)
{
    // _mm_alignr_epi8: SSSE3
    return _mm_sub_epi16(curr, _mm_alignr_epi8(curr, prev, 14));
}

STREAMVBYTE_UNTARGET_REGION

STREAMVBYTE_TARGET_SSSE3
static __m128i zigzag_16(__m128i buf)
{
    return _mm_xor_si128(_mm_add_epi16(buf, buf), _mm_srai_epi16(buf, 16));
}

STREAMVBYTE_UNTARGET_REGION

// based on code by aqrit  (streamvbyte_encode_SSSE3)
STREAMVBYTE_TARGET_SSSE3
size_t streamvbyte_zigzag_delta_encode_SSSE3_d1_init(
    uint16_t const * in,
    uint32_t count,
    uint8_t * out,
    uint16_t prev)
{
    __m128i Prev = _mm_set1_epi16(prev);
    uint32_t keyLen = (count >> 3) + (((count & 7) + 7) >> 3);  // 1-bit rounded to full byte
    uint8_t * restrict keyPtr = &out[0];
    uint8_t * restrict dataPtr = &out[keyLen];  // variable length data after keys

    const __m128i mask_01 = _mm_set1_epi8(0x01);

    for (uint16_t const * end = &in [(count & ~15)]; in != end; in += 16) {
        __m128i rawr0, r0, rawr1, r1, r2, r3;
        size_t keys;

        rawr0 = _mm_loadu_si128((__m128i *)&in[0]);
        r0 = zigzag_16(Delta(rawr0, Prev));
        Prev = rawr0;
        rawr1 = _mm_loadu_si128((__m128i *)&in[8]);
        r1 = zigzag_16(Delta(rawr1, Prev));
        Prev = rawr1;

        // 1 if the byte is set, 0 if not
        r2 = _mm_min_epu8(mask_01, r0);
        r3 = _mm_min_epu8(mask_01, r1);
        // for each (u)int16, FF if the MSB is set, 00 or 01 if not (us = unsigned saturation)
        r2 = _mm_packus_epi16(r2, r3);
        // for each byte, store a bit: 1 if FF, 0 if 00 or 01 (so 1 if MSB is set, 0 if not)
        keys = (size_t)_mm_movemask_epi8(r2);

        r2 = _mm_loadu_si128((__m128i *)&shuf_lut[(keys << 4) & 0x07F0]);
        r3 = _mm_loadu_si128((__m128i *)&shuf_lut[(keys >> 4) & 0x07F0]);
        // _mm_shuffle_epi8: SSSE3
        r0 = _mm_shuffle_epi8(r0, r2);
        r1 = _mm_shuffle_epi8(r1, r3);

        _mm_storeu_si128((__m128i *)dataPtr, r0);
        dataPtr += 8 + popcount(keys & 0xFF);
        _mm_storeu_si128((__m128i *)dataPtr, r1);
        dataPtr += 8 + popcount(keys >> 8);

        *((uint16_t *)keyPtr) = (uint16_t)keys;
        keyPtr += 2;
    }
    prev = _mm_extract_epi16(Prev, 7);

    // do remaining - max two control bytes left
    uint16_t key = 0;
    for (size_t i = 0; i < (count & 15); i++) {
        // TODO: can we factor this out to reuse the non-intrinsic code?
        uint16_t dw = in[i] - prev;
        prev = in[i];
        uint16_t zz = (dw + dw) ^ ((int16_t)dw >> 15);
        uint16_t symbol = (zz > 0x00FF);
        key |= symbol << (i + i);
        *((uint16_t *)dataPtr) = zz;
        dataPtr += 1 + symbol;
    }
    memcpy(keyPtr, &key, ((count & 15) + 5) >> 3);

    return dataPtr - out;
}

STREAMVBYTE_UNTARGET_REGION
#endif
