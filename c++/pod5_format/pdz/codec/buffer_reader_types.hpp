#pragma once

#include "../attributes.hpp"

#include <cstddef>
#include <cstdint>

namespace pod5 { namespace pdz {

class BufferUStream;

// One block of eight 2-bit values read back from B_u. `get(i)` extracts the
// value for lane `i` at the current bit offset.
class BufferUBlock
{
public:
    inline uint16_t get(size_t i) const noexcept { return (ptr[i] >> cycle) & 0x3; }

private:
    BufferUBlock(const uint16_t * pdz_restrict _ptr, uint16_t _cycle)
        : ptr(_ptr)
        , cycle(_cycle) {};

    const uint16_t * pdz_restrict ptr;
    const uint16_t cycle;

    friend class BufferUStream;
};

}}  // namespace pod5::pdz
