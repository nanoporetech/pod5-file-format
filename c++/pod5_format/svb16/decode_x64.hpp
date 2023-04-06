#pragma once

#include "common.hpp"
#include "decode_scalar.hpp"
#include "intrinsics.hpp"
#include "shuffle_tables.hpp"
#include "svb16.h"  // svb16_key_length

#include <gsl/gsl-lite.hpp>

#include <cstddef>
#include <cstdint>

#ifdef SVB16_X64

namespace svb16 {
namespace detail {
[[gnu::target("ssse3")]] inline __m128i zigzag_decode(__m128i val)
{
    return _mm_xor_si128(
        // N >> 1
        _mm_srli_epi16(val, 1),
        // 0xFFFF if N & 1 else 0x0000
        _mm_srai_epi16(_mm_slli_epi16(val, 15), 15)
        // alternative: _mm_sign_epi16(ones, _mm_slli_epi16(buf, 15))
    );
}

[[gnu::target("ssse3")]] inline __m128i unpack(uint32_t key, uint8_t const * SVB_RESTRICT * data)
{
    auto const len = static_cast<uint8_t>(8 + svb16_popcount(key));
    __m128i data_reg = _mm_loadu_si128(reinterpret_cast<__m128i const *>(*data));
    __m128i const shuffle = *reinterpret_cast<__m128i const *>(&g_decode_shuffle_table[key]);

    data_reg = _mm_shuffle_epi8(data_reg, shuffle);
    *data += len;

    return data_reg;
}

template <typename Int16T, bool UseDelta, bool UseZigzag>
[[gnu::target("ssse3")]] inline void store_8(Int16T * to, __m128i value, __m128i * prev)
{
    SVB16_IF_CONSTEXPR(UseZigzag) { value = zigzag_decode(value); }

    SVB16_IF_CONSTEXPR(UseDelta)
    {
        auto const broadcast_last_16 =
            m128i_from_bytes(14, 15, 14, 15, 14, 15, 14, 15, 14, 15, 14, 15, 14, 15, 14, 15);
        // value == [A B C D E F G H] (16 bit values)
        __m128i add = _mm_slli_si128(value, 2);
        // add   == [- A B C D E F G]
        *prev = _mm_shuffle_epi8(*prev, broadcast_last_16);
        // *prev == [P P P P P P P P]
        value = _mm_add_epi16(value, add);
        // value == [A AB BC CD DE FG GH]
        add = _mm_slli_si128(value, 4);
        // add   == [- - A AB BC CD DE EF]
        value = _mm_add_epi16(value, add);
        // value == [A AB ABC ABCD BCDE CDEF DEFG EFGH]
        add = _mm_slli_si128(value, 8);
        // add   == [- - - - A AB ABC ABCD]
        value = _mm_add_epi16(value, add);
        // value == [A AB ABC ABCD ABCDE ABCDEF ABCDEFG ABCDEFGH]
        value = _mm_add_epi16(value, *prev);
        // value == [PA PAB PABC PABCD PABCDE PABCDEF PABCDEFG PABCDEFGH]
        *prev = value;
    }

    _mm_storeu_si128(reinterpret_cast<__m128i *>(to), value);
}
}  // namespace detail

template <typename Int16T, bool UseDelta, bool UseZigzag>
[[gnu::target("sse4.1")]] uint8_t const * decode_sse(
    gsl::span<Int16T> out_span,
    gsl::span<uint8_t const> keys_span,
    gsl::span<uint8_t const> data_span,
    Int16T prev = 0)
{
    auto store_8 = [](Int16T * to, __m128i value, __m128i * prev) {
        detail::store_8<Int16T, UseDelta, UseZigzag>(to, value, prev);
    };
    // this code treats all input as uint16_t (except the zigzag code, which treats it as int16_t)
    // this isn't a problem, as the scalar code does the same

    auto out = out_span.begin();
    auto const count = out_span.size();
    auto keys_it = keys_span.begin();
    auto data = data_span.begin();

    // handle blocks of 32 values
    if (count >= 64) {
        size_t const key_bytes = count / 8;

        __m128i prev_reg;
        SVB16_IF_CONSTEXPR(UseDelta) { prev_reg = _mm_set1_epi16(prev); }

        int64_t offset = -static_cast<int64_t>(key_bytes) / 8 + 1;  // 8 -> 4?
        uint64_t const * keyPtr64 = reinterpret_cast<uint64_t const *>(keys_it) - offset;
        uint64_t nextkeys;
        memcpy(&nextkeys, keyPtr64 + offset, sizeof(nextkeys));

        __m128i data_reg;

        for (; offset != 0; ++offset) {
            uint64_t keys = nextkeys;
            memcpy(&nextkeys, keyPtr64 + offset + 1, sizeof(nextkeys));
            // faster 16-bit delta since we only have 8-bit values
            if (!keys) {  // 64 1-byte ints in a row

                // _mm_cvtepu8_epi16: SSE4.1
                data_reg =
                    _mm_cvtepu8_epi16(_mm_lddqu_si128(reinterpret_cast<__m128i const *>(data)));
                store_8(out, data_reg, &prev_reg);
                data_reg =
                    _mm_cvtepu8_epi16(_mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 8)));
                store_8(out + 8, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 16)));
                store_8(out + 16, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 24)));
                store_8(out + 24, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 32)));
                store_8(out + 32, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + +40)));
                store_8(out + 40, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 48)));
                store_8(out + 48, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 56)));
                store_8(out + 56, data_reg, &prev_reg);
                out += 64;
                data += 64;
                continue;
            }

            data_reg = detail::unpack(keys & 0x00FF, &data);
            store_8(out, data_reg, &prev_reg);
            data_reg = detail::unpack((keys & 0xFF00) >> 8, &data);
            store_8(out + 8, data_reg, &prev_reg);

            keys >>= 16;
            data_reg = detail::unpack((keys & 0x00FF), &data);
            store_8(out + 16, data_reg, &prev_reg);
            data_reg = detail::unpack((keys & 0xFF00) >> 8, &data);
            store_8(out + 24, data_reg, &prev_reg);

            keys >>= 16;
            data_reg = detail::unpack((keys & 0x00FF), &data);
            store_8(out + 32, data_reg, &prev_reg);
            data_reg = detail::unpack((keys & 0xFF00) >> 8, &data);
            store_8(out + 40, data_reg, &prev_reg);

            keys >>= 16;
            data_reg = detail::unpack((keys & 0x00FF), &data);
            store_8(out + 48, data_reg, &prev_reg);

            // Note we load at least sizeof(__m128i) bytes from the end of data
            // here, need to ensure that is available to read.
            //
            // But we might not use it all depending on the unpacking.
            //
            // This is ok due to `decode_input_buffer_padding_byte_count` enuring
            // extra space on the input buffer.
            data_reg = detail::unpack((keys & 0xFF00) >> 8, &data);
            store_8(out + 56, data_reg, &prev_reg);

            out += 64;
        }
        {
            uint64_t keys = nextkeys;
            // faster 16-bit delta since we only have 8-bit values
            if (!keys) {  // 64 1-byte ints in a row
                data_reg =
                    _mm_cvtepu8_epi16(_mm_lddqu_si128(reinterpret_cast<__m128i const *>(data)));
                store_8(out, data_reg, &prev_reg);
                data_reg =
                    _mm_cvtepu8_epi16(_mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 8)));
                store_8(out + 8, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 16)));
                store_8(out + 16, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 24)));
                store_8(out + 24, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 32)));
                store_8(out + 32, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + +40)));
                store_8(out + 40, data_reg, &prev_reg);
                data_reg = _mm_cvtepu8_epi16(
                    _mm_lddqu_si128(reinterpret_cast<__m128i const *>(data + 48)));
                store_8(out + 48, data_reg, &prev_reg);
                // Only load the first 8 bytes here, otherwise we may run off the end of the buffer
                data_reg = _mm_cvtepu8_epi16(
                    _mm_loadl_epi64(reinterpret_cast<__m128i const *>(data + 56)));
                store_8(out + 56, data_reg, &prev_reg);
                out += 64;
                data += 64;

            } else {
                data_reg = detail::unpack(keys & 0x00FF, &data);
                store_8(out, data_reg, &prev_reg);
                data_reg = detail::unpack((keys & 0xFF00) >> 8, &data);
                store_8(out + 8, data_reg, &prev_reg);

                keys >>= 16;
                data_reg = detail::unpack((keys & 0x00FF), &data);
                store_8(out + 16, data_reg, &prev_reg);
                data_reg = detail::unpack((keys & 0xFF00) >> 8, &data);
                store_8(out + 24, data_reg, &prev_reg);

                keys >>= 16;
                data_reg = detail::unpack((keys & 0x00FF), &data);
                store_8(out + 32, data_reg, &prev_reg);
                data_reg = detail::unpack((keys & 0xFF00) >> 8, &data);
                store_8(out + 40, data_reg, &prev_reg);

                keys >>= 16;
                data_reg = detail::unpack((keys & 0x00FF), &data);
                store_8(out + 48, data_reg, &prev_reg);
                data_reg = detail::unpack((keys & 0xFF00) >> 8, &data);
                store_8(out + 56, data_reg, &prev_reg);

                out += 64;
            }
        }
        prev = out[-1];

        keys_it += key_bytes - (key_bytes & 7);
    }

    assert(out <= out_span.end());
    assert(keys_it <= keys_span.end());
    assert(data <= data_span.end());

    auto out_scalar_span = gsl::make_span(out, out_span.end());
    assert(out_scalar_span.size() == (count & 63));

    auto keys_scalar_span = gsl::make_span(keys_it, keys_span.end());
    auto data_scalar_span = gsl::make_span(data, data_span.end());

    return decode_scalar<Int16T, UseDelta, UseZigzag>(
        out_scalar_span, keys_scalar_span, data_scalar_span, prev);
}

#endif  // SVB16_X64

}  // namespace svb16
