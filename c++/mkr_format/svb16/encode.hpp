#pragma once

#include "common.hpp"
#include "encode_scalar.hpp"
#include "svb16.h"  // svb16_key_length
#ifdef SVB16_X64
#include "encode_x64.hpp"
#include "simd_detect_x64.hpp"
#endif

namespace svb16 {

template <typename Int16T, bool UseDelta, bool UseZigzag>
size_t encode(Int16T const* in, uint8_t* SVB_RESTRICT out, uint32_t count, Int16T prev = 0) {
    auto const keys = out;
    auto const data = keys + ::svb16_key_length(count);
#ifdef SVB16_X64
    if (has_ssse3()) {
        return encode_sse<Int16T, UseDelta, UseZigzag>(in, keys, data, count, prev) - out;
    }
#endif
    return encode_scalar<Int16T, UseDelta, UseZigzag>(in, keys, data, count, prev) - out;
}

}  // namespace svb16
