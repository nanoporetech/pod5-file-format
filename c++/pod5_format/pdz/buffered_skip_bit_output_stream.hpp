#pragma once

#include "attributes.hpp"
#include "utils.hpp"

#include <cstddef>
#include <cstdint>

namespace pod5 { namespace pdz {

// Writes fixed-width elements packed into 16-bit words, advancing the output
// pointer by `BytesSkip` after each full word. This lets the scalar reference
// reproduce the exact interleaved memory layout the SIMD kernels emit for B_u.
// The stream never needs flushing because the buffer sizes are chosen so every
// word fills completely.
template <size_t BitsPerElement, size_t BytesSkip>
class BufferedSkipOutputBitStream
{
public:
    BufferedSkipOutputBitStream(uint16_t * pdz_restrict _out)
        : out(_out)
        , buffer(0)
        , bits_written(0)
    {
    }

    inline void write(const uint16_t x) noexcept;

private:
    uint16_t * pdz_restrict out;
    uint16_t buffer;
    size_t bits_written;
};

template <size_t BitsPerElement, size_t BytesSkip>
inline void BufferedSkipOutputBitStream<BitsPerElement, BytesSkip>::write(uint16_t x) noexcept
{
    PDZ_FAIL_COMPILE_IF(
        BitsPerElement != 2 && BitsPerElement != 1 && BitsPerElement != 4 && BitsPerElement != 8
        && BitsPerElement != 16);
    // Masking is assumed to have been done by the caller.
    buffer = static_cast<uint16_t>(buffer | (x << bits_written));
    bits_written += BitsPerElement;
    if (bits_written >= 16) {
        *out = buffer;
        out = reinterpret_cast<uint16_t *>(reinterpret_cast<uint8_t *>(out) + BytesSkip);
        bits_written = 0;
        buffer = 0;
    }
}

}}  // namespace pod5::pdz
