#pragma once

#include "common.hpp"
#include "decode_scalar.hpp"
#include "svb16.h"  // svb16_key_length
#ifdef SVB16_X64
#include "decode_x64.hpp"
#include "simd_detect_x64.hpp"
#endif

namespace svb16 {

template <typename Int16T, bool UseDelta, bool UseZigzag>
size_t decode(Int16T* out, uint8_t const* SVB_RESTRICT in, uint32_t count, Int16T prev = 0) {
    auto const keys = in;
    auto const data = keys + ::svb16_key_length(count);
#ifdef SVB16_X64
    if (has_sse4_1()) {
        // Gotta have some tests for this before we turn it on...
        //return decode_sse<Int16T, UseDelta, UseZigzag>(out, keys, data, count, prev) - in;
    }
#endif
    return decode_scalar<Int16T, UseDelta, UseZigzag>(out, keys, data, count, prev) - in;
}

}  // namespace svb16
