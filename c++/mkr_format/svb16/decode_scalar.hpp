#pragma once

#include "common.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace svb16 {
namespace detail {
inline uint16_t zigzag_decode(uint16_t val) {
    return (val >> 1) ^ static_cast<uint16_t>(0 - (val & 1));
}
inline uint16_t decode_data(uint8_t const *SVB_RESTRICT *dataPtrPtr, uint8_t code) {
    const uint8_t *dataPtr = *dataPtrPtr;
    uint16_t val;

    if (code == 0) {  // 1 byte
        val = (uint16_t)*dataPtr;
        dataPtr += 1;
    } else {  // 2 bytes
        val = 0;
        memcpy(&val, dataPtr, 2);  // assumes little endian
        dataPtr += 2;
    }

    *dataPtrPtr = dataPtr;
    return val;
}
}  // namespace detail

template <typename Int16T, bool UseDelta, bool UseZigzag>
uint8_t const *decode_scalar(Int16T *out,
                             uint8_t const *SVB_RESTRICT keys,
                             uint8_t const *SVB_RESTRICT data,
                             uint32_t count,
                             Int16T prev = 0) {
    if (count == 0) {
        return 0;
    }

    uint8_t shift = 0;  // cycles 0 through 7 then resets
    uint8_t key_byte = *keys++;
    // need to do the arithmetic in unsigned space so it wraps
    auto u_prev = static_cast<uint16_t>(prev);
    for (uint32_t c = 0; c < count; c++, shift++) {
        if (shift == 8) {
            shift = 0;
            key_byte = *keys++;
        }
        uint16_t value = detail::decode_data(&data, (key_byte >> shift) & 0x01);
        SVB16_IF_CONSTEXPR(UseZigzag) { value = detail::zigzag_decode(value); }
        SVB16_IF_CONSTEXPR(UseDelta) {
            value += u_prev;
            u_prev = value;
        }
        *out++ = static_cast<Int16T>(value);
    }
    return data;
}

}  // namespace svb16
