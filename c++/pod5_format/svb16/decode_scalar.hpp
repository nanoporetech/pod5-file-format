#pragma once

#include "common.hpp"

#include <gsl/gsl-lite.hpp>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace svb16 {
namespace detail {
inline uint16_t zigzag_decode(uint16_t val)
{
    return (val >> 1) ^ static_cast<uint16_t>(0 - (val & 1));
}

inline uint16_t decode_data(gsl::span<uint8_t const>::iterator & dataPtr, uint8_t code)
{
    uint16_t val;

    if (code == 0) {  // 1 byte
        val = (uint16_t)*dataPtr;
        dataPtr += 1;
    } else {  // 2 bytes
        val = 0;
        memcpy(&val, dataPtr, 2);  // assumes little endian
        dataPtr += 2;
    }

    return val;
}
}  // namespace detail

template <typename Int16T, bool UseDelta, bool UseZigzag>
uint8_t const * decode_scalar(
    gsl::span<Int16T> out_span,
    gsl::span<uint8_t const> keys_span,
    gsl::span<uint8_t const> data_span,
    Int16T prev = 0)
{
    auto const count = out_span.size();
    if (count == 0) {
        return data_span.begin();
    }

    auto out = out_span.begin();
    auto keys = keys_span.begin();
    auto data = data_span.begin();

    uint8_t shift = 0;  // cycles 0 through 7 then resets
    uint8_t key_byte = *keys++;
    // need to do the arithmetic in unsigned space so it wraps
    auto u_prev = static_cast<uint16_t>(prev);
    for (uint32_t c = 0; c < count; c++, shift++) {
        if (shift == 8) {
            shift = 0;
            key_byte = *keys++;
        }
        uint16_t value = detail::decode_data(data, (key_byte >> shift) & 0x01);
        SVB16_IF_CONSTEXPR(UseZigzag) { value = detail::zigzag_decode(value); }
        SVB16_IF_CONSTEXPR(UseDelta)
        {
            value += u_prev;
            u_prev = value;
        }
        *out++ = static_cast<Int16T>(value);
    }

    assert(out == out_span.end());
    assert(keys == keys_span.end());
    assert(data <= data_span.end());
    return data;
}

}  // namespace svb16
