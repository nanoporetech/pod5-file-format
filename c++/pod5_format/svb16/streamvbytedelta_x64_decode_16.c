#include "streamvbyte_isadetection.h"
#include "streamvbyte_shuffle_tables_decode_16.h"

#include <string.h>  // for memcpy
#ifdef STREAMVBYTE_X64

STREAMVBYTE_TARGET_SSSE3
static __m128i undo_zigzag_16(__m128i buf)
{
    return _mm_xor_si128(
        // N >> 1
        _mm_srli_epi16(buf, 1),
        // 0xFFFF if N & 1 else 0x0000
        _mm_srai_epi16(_mm_slli_epi16(buf, 15), 15)
        // alternative: _mm_sign_epi16(ones, _mm_slli_epi16(buf, 15))
    );
}

STREAMVBYTE_UNTARGET_REGION

STREAMVBYTE_TARGET_SSSE3
static inline __m128i _decode_avx(uint32_t key, uint8_t const * __restrict__ * dataPtrPtr)
{
    uint8_t len = 8 + popcount(key);
    __m128i Data = _mm_loadu_si128((__m128i *)*dataPtrPtr);
    __m128i Shuf = *(__m128i *)&shuffleTable[key];

    Data = _mm_shuffle_epi8(Data, Shuf);
    *dataPtrPtr += len;

    return Data;
}

STREAMVBYTE_UNTARGET_REGION

STREAMVBYTE_TARGET_SSSE3
static inline void _write_avx(uint16_t * out, __m128i Vec)
{
    _mm_storeu_si128((__m128i *)out, Vec);
}

STREAMVBYTE_UNTARGET_REGION

STREAMVBYTE_TARGET_SSSE3
static inline __m128i _write_16bit_avx_d1(uint16_t * out, __m128i Vec, __m128i Prev)
{
#ifndef _MSC_VER
    __m128i BroadcastLast16 = {0x0F0E0F0E0F0E0F0E, 0x0F0E0F0E0F0E0F0E};
#else
    __m128i BroadcastLast16 = {14, 15, 14, 15, 14, 15, 14, 15, 14, 15, 14, 15, 14, 15, 14, 15};
#endif
    Vec = undo_zigzag_16(Vec);
    // vec == [A B C D E F G H] (16 bit values)
    __m128i Add = _mm_slli_si128(Vec, 2);            // [- A B C D E F G]
    Prev = _mm_shuffle_epi8(Prev, BroadcastLast16);  // [P P P P P P P P]
    Vec = _mm_add_epi16(Vec, Add);                   // [A AB BC CD DE FG GH]
    Add = _mm_slli_si128(Vec, 4);                    // [- - A AB BC CD DE EF]
    Vec = _mm_add_epi16(Vec, Add);                   // [A AB ABC ABCD BCDE CDEF DEFG EFGH]
    Add = _mm_slli_si128(Vec, 8);                    // [- - - - A AB ABC ABCD]
    Vec = _mm_add_epi16(Vec, Add);   // [A AB ABC ABCD ABCDE ABCDEF ABCDEFG ABCDEFGH]
    Vec = _mm_add_epi16(Vec, Prev);  // [PA PAB PABC PABCD PABCDE PABCDEF PABCDEFG PABCDEFGH]
    _write_avx(out, Vec);
    return Vec;
}

STREAMVBYTE_UNTARGET_REGION

STREAMVBYTE_TARGET_SSSE3
static uint8_t const * svb_decode_avx_d1_init(
    uint16_t * out,
    uint8_t const * __restrict__ keyPtr,
    uint8_t const * __restrict__ dataPtr,
    uint64_t count,
    uint16_t prev)
{
    uint64_t keybytes = count / 4;  // number of key bytes
    if (keybytes >= 8) {
        __m128i Prev = _mm_set1_epi16(prev);
        __m128i Data;

        int64_t Offset = -(int64_t)keybytes / 8 + 1;

        uint64_t const * keyPtr64 = (uint64_t const *)keyPtr - Offset;
        uint64_t nextkeys;
        memcpy(&nextkeys, keyPtr64 + Offset, sizeof(nextkeys));
        for (; Offset != 0; ++Offset) {
            uint64_t keys = nextkeys;
            memcpy(&nextkeys, keyPtr64 + Offset + 1, sizeof(nextkeys));
            // faster 16-bit delta since we only have 8-bit values
            if (!keys) {  // 32 1-byte ints in a row

                // _mm_cvtepu8_epi16: SSE4.1
                Data = _mm_cvtepu8_epi16(_mm_lddqu_si128((__m128i *)(dataPtr)));
                Prev = _write_16bit_avx_d1(out, Data, Prev);
                Data = _mm_cvtepu8_epi16(_mm_lddqu_si128((__m128i *)(dataPtr + 8)));
                Prev = _write_16bit_avx_d1(out + 8, Data, Prev);
                Data = _mm_cvtepu8_epi16(_mm_lddqu_si128((__m128i *)(dataPtr + 16)));
                Prev = _write_16bit_avx_d1(out + 16, Data, Prev);
                Data = _mm_cvtepu8_epi16(_mm_lddqu_si128((__m128i *)(dataPtr + 24)));
                Prev = _write_16bit_avx_d1(out + 24, Data, Prev);
                out += 32;
                dataPtr += 32;
                continue;
            }

            Data = _decode_avx(keys & 0x00FF, &dataPtr);
            Prev = _write_16bit_avx_d1(out, Data, Prev);
            Data = _decode_avx((keys & 0xFF00) >> 8, &dataPtr);
            Prev = _write_16bit_avx_d1(out + 4, Data, Prev);

            keys >>= 16;
            Data = _decode_avx((keys & 0x00FF), &dataPtr);
            Prev = _write_16bit_avx_d1(out + 8, Data, Prev);
            Data = _decode_avx((keys & 0xFF00) >> 8, &dataPtr);
            Prev = _write_16bit_avx_d1(out + 12, Data, Prev);

            keys >>= 16;
            Data = _decode_avx((keys & 0x00FF), &dataPtr);
            Prev = _write_16bit_avx_d1(out + 16, Data, Prev);
            Data = _decode_avx((keys & 0xFF00) >> 8, &dataPtr);
            Prev = _write_16bit_avx_d1(out + 20, Data, Prev);

            keys >>= 16;
            Data = _decode_avx((keys & 0x00FF), &dataPtr);
            Prev = _write_16bit_avx_d1(out + 24, Data, Prev);
            Data = _decode_avx((keys & 0xFF00) >> 8, &dataPtr);
            Prev = _write_16bit_avx_d1(out + 28, Data, Prev);

            out += 32;
        }
        {
            uint64_t keys = nextkeys;
            // faster 16-bit delta since we only have 8-bit values
            if (!keys) {  // 32 1-byte ints in a row
                Data = _mm_cvtepu8_epi16(_mm_lddqu_si128((__m128i *)(dataPtr)));
                Prev = _write_16bit_avx_d1(out, Data, Prev);
                Data = _mm_cvtepu8_epi16(_mm_lddqu_si128((__m128i *)(dataPtr + 8)));
                Prev = _write_16bit_avx_d1(out + 8, Data, Prev);
                Data = _mm_cvtepu8_epi16(_mm_lddqu_si128((__m128i *)(dataPtr + 16)));
                Prev = _write_16bit_avx_d1(out + 16, Data, Prev);
                Data = _mm_cvtepu8_epi16(_mm_loadl_epi64((__m128i *)(dataPtr + 24)));
                Prev = _write_16bit_avx_d1(out + 24, Data, Prev);
                out += 32;
                dataPtr += 32;

            } else {
                Data = _decode_avx(keys & 0x00FF, &dataPtr);
                Prev = _write_16bit_avx_d1(out, Data, Prev);
                Data = _decode_avx((keys & 0xFF00) >> 8, &dataPtr);
                Prev = _write_16bit_avx_d1(out + 4, Data, Prev);

                keys >>= 16;
                Data = _decode_avx((keys & 0x00FF), &dataPtr);
                Prev = _write_16bit_avx_d1(out + 8, Data, Prev);
                Data = _decode_avx((keys & 0xFF00) >> 8, &dataPtr);
                Prev = _write_16bit_avx_d1(out + 12, Data, Prev);

                keys >>= 16;
                Data = _decode_avx((keys & 0x00FF), &dataPtr);
                Prev = _write_16bit_avx_d1(out + 16, Data, Prev);
                Data = _decode_avx((keys & 0xFF00) >> 8, &dataPtr);
                Prev = _write_16bit_avx_d1(out + 20, Data, Prev);

                keys >>= 16;
                Data = _decode_avx((keys & 0x00FF), &dataPtr);
                Prev = _write_16bit_avx_d1(out + 24, Data, Prev);
                Data = _decode_avx((keys & 0xFF00) >> 8, &dataPtr);
                Prev = _write_16bit_avx_d1(out + 28, Data, Prev);

                out += 32;
            }
        }
        prev = out[-1];
    }
    uint64_t consumedkeys = keybytes - (keybytes & 7);
    return svb_decode_scalar_d1_init(out, keyPtr + consumedkeys, dataPtr, count & 31, prev);
}

STREAMVBYTE_UNTARGET_REGION
#endif
