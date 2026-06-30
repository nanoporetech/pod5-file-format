#pragma once

#include "../attributes.hpp"
#include "buffer_reader_types.hpp"

#include <cstddef>
#include <cstdint>

namespace pod5 { namespace pdz {

// Reads B_u back in the interleaved 2-bit layout written by BufferUWriter.
// Each call to next() returns a block of eight lanes.
class BufferUStream
{
public:
    BufferUStream(const uint8_t * pdz_restrict base)
        : ptr(reinterpret_cast<const uint16_t *>(base)) {};

    BufferUBlock next(void)
    {
        const auto res = BufferUBlock(ptr, cycle);
        cycle = static_cast<uint16_t>(cycle + 2);
        if (cycle == 16) {
            cycle = 0;
            ptr += 8;
        }
        return res;
    }

private:
    uint16_t cycle = 0;
    const uint16_t * pdz_restrict ptr;
};

// Reads one byte-plane buffer (B_l or B_h) back in the interleaved layout
// written by BufferLHWriter.
class BufferLHStream
{
public:
    BufferLHStream(const uint8_t * pdz_restrict base)
        : ptr(base) {};

    uint16_t next()
    {
        const auto res = ptr[cycle + in_cycle_iteration * 2];
        ++in_cycle_iteration;
        if (in_cycle_iteration == 8) {
            in_cycle_iteration = 0;
            ++cycle;
            if (cycle == 2) {
                cycle = 0;
                ptr += 16;
            }
        }
        return res;
    }

private:
    uint_fast8_t cycle = 0;
    uint_fast8_t in_cycle_iteration = 0;
    const uint8_t * pdz_restrict ptr;
};

}}  // namespace pod5::pdz
