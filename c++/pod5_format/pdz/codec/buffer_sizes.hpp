#pragma once

#include "../utils.hpp"

#include <cstddef>

namespace pod5 { namespace pdz {

// PDZ piecewise split: every 16-bit zig-zag value z_i is split into three
// constant-width pieces  h + l + u = 16  (each <= 8 bits):
//   u = 2  least-significant bits  -> B_u  (stored uncompressed)
//   l = 8  next bits               -> B_l  -> ZSTD -> Z_l
//   h = 6  most-significant bits   -> B_h  -> ZSTD -> Z_h
struct PieceWidths {
    static constexpr unsigned u = 2;
    static constexpr unsigned l = 8;
    static constexpr unsigned h = 6;
    static constexpr unsigned low_piece_shift = u;       // first bit of the l piece (= 2)
    static constexpr unsigned high_piece_shift = u + l;  // first bit of the h piece (= 10)
};

// Size formulas for the three split buffers B_u / B_l / B_h.
// |B_l| = |B_h| = n bytes; |B_u| = ceil(n * u / 8) = ceil(n / 4) bytes.
// The split is computed in two parts: a vectorised section that processes a
// whole number of 128-sample blocks, and a scalar tail for the remainder.
class BufferSizes
{
public:
    // Samples per vectorised block: 8 registers * 16 samples = 128.
    static constexpr size_t samples_per_block = 8 * 16;

    static constexpr size_t parallel_sample_count(const size_t n)
    {
        return (n / samples_per_block) * samples_per_block;
    }

    static constexpr size_t serial_sample_count(const size_t n)
    {
        return n % samples_per_block;
    }

    static constexpr size_t parallel_buffer_u_size_bytes(const size_t n)
    {
        return round_up_division<size_t>(parallel_sample_count(n), 4);
    }

    static constexpr size_t parallel_buffer_l_size_bytes(const size_t n)
    {
        return parallel_sample_count(n);
    }

    static constexpr size_t parallel_buffer_h_size_bytes(const size_t n)
    {
        return parallel_sample_count(n);
    }

    static constexpr size_t serial_buffer_u_size_bytes(const size_t n)
    {
        return round_up_division<size_t>(serial_sample_count(n), 4);
    }

    static constexpr size_t serial_buffer_l_size_bytes(const size_t n)
    {
        return serial_sample_count(n);
    }

    static constexpr size_t serial_buffer_h_size_bytes(const size_t n)
    {
        return serial_sample_count(n);
    }

    static constexpr size_t buffer_u_size_bytes(const size_t n)
    {
        return parallel_buffer_u_size_bytes(n) + serial_buffer_u_size_bytes(n);
    }

    static constexpr size_t buffer_l_size_bytes(const size_t n)
    {
        return parallel_buffer_l_size_bytes(n) + serial_buffer_l_size_bytes(n);
    }

    static constexpr size_t buffer_h_size_bytes(const size_t n)
    {
        return parallel_buffer_h_size_bytes(n) + serial_buffer_h_size_bytes(n);
    }
};

}}  // namespace pod5::pdz
