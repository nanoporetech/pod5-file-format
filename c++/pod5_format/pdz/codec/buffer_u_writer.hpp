#pragma once

#include "../buffered_skip_bit_output_stream.hpp"

#include <cstddef>
#include <cstdint>

namespace pod5 { namespace pdz {

// Writes the uncompressed buffer B_u: the u = 2 least significant bits of eight
// consecutive zig-zag values, packed across eight interleaved 2-bit lanes so
// the layout matches the SIMD kernels byte-for-byte.
class BufferUWriter
{
public:
    BufferUWriter(uint8_t * base_ptr)
        : lane_0(reinterpret_cast<uint16_t *>(base_ptr))
        , lane_1(reinterpret_cast<uint16_t *>(base_ptr) + 1)
        , lane_2(reinterpret_cast<uint16_t *>(base_ptr) + 2)
        , lane_3(reinterpret_cast<uint16_t *>(base_ptr) + 3)
        , lane_4(reinterpret_cast<uint16_t *>(base_ptr) + 4)
        , lane_5(reinterpret_cast<uint16_t *>(base_ptr) + 5)
        , lane_6(reinterpret_cast<uint16_t *>(base_ptr) + 6)
        , lane_7(reinterpret_cast<uint16_t *>(base_ptr) + 7)
    {
    }

    inline void write(
        uint16_t value_0,
        uint16_t value_1,
        uint16_t value_2,
        uint16_t value_3,
        uint16_t value_4,
        uint16_t value_5,
        uint16_t value_6,
        uint16_t value_7) noexcept
    {
        lane_0.write(value_0);
        lane_1.write(value_1);
        lane_2.write(value_2);
        lane_3.write(value_3);
        lane_4.write(value_4);
        lane_5.write(value_5);
        lane_6.write(value_6);
        lane_7.write(value_7);
    }

private:
    // u = 2 bits per element; lanes are 16 bytes (one SIMD register) apart.
    BufferedSkipOutputBitStream<2, 16> lane_0;
    BufferedSkipOutputBitStream<2, 16> lane_1;
    BufferedSkipOutputBitStream<2, 16> lane_2;
    BufferedSkipOutputBitStream<2, 16> lane_3;
    BufferedSkipOutputBitStream<2, 16> lane_4;
    BufferedSkipOutputBitStream<2, 16> lane_5;
    BufferedSkipOutputBitStream<2, 16> lane_6;
    BufferedSkipOutputBitStream<2, 16> lane_7;
};

}}  // namespace pod5::pdz
