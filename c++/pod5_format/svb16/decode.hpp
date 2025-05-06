#pragma once

#include "common.hpp"
#include "decode_scalar.hpp"
#include "svb16.h"  // svb16_key_length

#include <type_traits>

#ifdef SVB16_X64
#include "decode_x64.hpp"
#include "simd_detect_x64.hpp"
#endif

namespace svb16 {

// Required extra space after readable buffers passed in.
//
// Require 1 128 bit buffer beyond the end of all input readable buffers.
inline std::size_t decode_input_buffer_padding_byte_count()
{
#ifdef SVB16_X64
    return sizeof(__m128i);
#else
    return 0;
#endif
}

template <typename Int16T, bool UseDelta, bool UseZigzag>
size_t decode(gsl::span<Int16T> out, gsl::span<uint8_t const> in, Int16T prev = 0)
{
    auto keys_length = ::svb16_key_length(out.size());
    auto const keys = in.subspan(0, keys_length);
    auto const data = in.subspan(keys_length);
#ifdef SVB16_X64
    if (has_sse4_1()) {
        return decode_sse<Int16T, UseDelta, UseZigzag>(out, keys, data, prev) - in.begin();
    }
#endif
    return decode_scalar<Int16T, UseDelta, UseZigzag>(out, keys, data, prev) - in.begin();
}

inline bool validate(gsl::span<uint8_t const> compressed_input, std::size_t out_size)
{
    auto const keys_length = ::svb16_key_length(out_size);
    if (keys_length > compressed_input.size()) {
        return false;
    }

    // Pull out the parts of the input data.
    auto const keys_span = compressed_input.subspan(0, keys_length);
    auto const data_span = compressed_input.subspan(keys_length);
    auto keys_ptr = keys_span.begin();

    // Accumulate the key sizes in a wider type to avoid overflow.
    using Accumulator = std::
        conditional_t<sizeof(std::size_t) >= sizeof(std::uint64_t), std::size_t, std::uint64_t>;
    Accumulator encoded_size = 0;

    // Give the compiler a hint that it can avoid branches in the inner loop.
    for (std::size_t c = 0; c < out_size / 8; c++) {
        uint8_t const key_byte = *keys_ptr++;
        for (uint8_t shift = 0; shift < 8; shift++) {
            uint8_t const code = (key_byte >> shift) & 0x01;
            encoded_size += code + 1;
        }
    }
    out_size &= 7;

    // Process the remainder one at a time.
    uint8_t shift = 0;
    uint8_t key_byte = *keys_ptr++;
    for (std::size_t c = 0; c < out_size; c++) {
        if (shift == 8) {
            shift = 0;
            key_byte = *keys_ptr++;
        }
        uint8_t const code = (key_byte >> shift) & 0x01;
        encoded_size += code + 1;
        shift++;
    }

    return encoded_size == data_span.size();
}

}  // namespace svb16
