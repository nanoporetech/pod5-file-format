#pragma once

#include "../attributes.hpp"

#include <cstddef>
#include <cstdint>

namespace pod5 { namespace pdz {

// Writes one byte-plane buffer, B_l or B_h: one byte per zig-zag value, laid
// out in the interleaved order the SIMD kernels produce (two values per 16-byte
// register stride). Never needs flushing.
class BufferLHWriter
{
public:
    inline BufferLHWriter(uint8_t * pdz_restrict base) noexcept
        : ptr(base)
    {
    }

    inline void write(
        uint8_t x_0,
        uint8_t x_1,
        uint8_t x_2,
        uint8_t x_3,
        uint8_t x_4,
        uint8_t x_5,
        uint8_t x_6,
        uint8_t x_7) noexcept
    {
        ptr[cycle] = x_0;
        ptr[cycle + 2] = x_1;
        ptr[cycle + 4] = x_2;
        ptr[cycle + 6] = x_3;
        ptr[cycle + 8] = x_4;
        ptr[cycle + 10] = x_5;
        ptr[cycle + 12] = x_6;
        ptr[cycle + 14] = x_7;

        ++cycle;

        if (cycle == 2) {
            cycle = 0;
            ptr += 16;
        }
    }

private:
    uint8_t * pdz_restrict ptr;
    size_t cycle = 0;
};

}}  // namespace pod5::pdz
