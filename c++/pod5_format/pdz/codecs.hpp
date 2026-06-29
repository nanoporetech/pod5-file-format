#pragma once

#include <cstdint>

namespace pod5 { namespace pdz {

// Little-endian fixed-width (8-byte) integer serialisation used by the PDZ
// block header. `constexpr` implies `inline`, so these are ODR-safe in a header.
constexpr void encode_uint64(uint64_t x, uint8_t * buffer)
{
    for (int i = 0; i < 8; i++) {
        const unsigned shift_expression = static_cast<unsigned>(i) * 8;
        buffer[i] =
            static_cast<uint8_t>((x & (static_cast<uint64_t>(0xFF) << shift_expression))
                                 >> shift_expression);
    }
}

constexpr uint64_t decode_uint64(uint8_t const * buffer)
{
    return static_cast<uint64_t>(*buffer) | static_cast<uint64_t>(*(buffer + 1)) << 8
           | static_cast<uint64_t>(*(buffer + 2)) << 16
           | static_cast<uint64_t>(*(buffer + 3)) << 24
           | static_cast<uint64_t>(*(buffer + 4)) << 32
           | static_cast<uint64_t>(*(buffer + 5)) << 40
           | static_cast<uint64_t>(*(buffer + 6)) << 48
           | static_cast<uint64_t>(*(buffer + 7)) << 56;
}

}}  // namespace pod5::pdz
