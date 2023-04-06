#pragma once

#include "common.hpp"
#include "decode_scalar.hpp"
#include "svb16.h"  // svb16_key_length
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

}  // namespace svb16
