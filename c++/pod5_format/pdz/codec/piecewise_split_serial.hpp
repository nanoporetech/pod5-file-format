#pragma once

#include "../attributes.hpp"
#include "buffer_sizes.hpp"

#include <cstddef>
#include <cstdint>

namespace pod5 { namespace pdz {

// Scalar tail shared by every backend (scalar / SSE / NEON). Like the vectorised
// sections it computes the difference d = x_i - x_{i-1} from the raw signal,
// seeded with the running `previous_sample` carried over from the parallel
// region. Unlike the vectorised sections it does NOT apply the Rice map. All
// three backends use this exact implementation for the remainder samples, which
// is what keeps the encoded byte layout identical across architectures.
inline void encode_serial_section(
    const int16_t * pdz_restrict samples,
    const size_t n,
    uint8_t * pdz_restrict buffer_u_ptr,
    uint8_t * pdz_restrict buffer_l_ptr,
    uint8_t * pdz_restrict buffer_h_ptr,
    uint16_t previous_sample)
{
    constexpr uint16_t u_mask = 0x0003;
    constexpr uint16_t l_mask = 0x03FC;
    constexpr uint16_t h_mask = 0xFC00;
    constexpr uint_fast8_t low_piece_shift = PieceWidths::low_piece_shift;
    constexpr uint_fast8_t high_piece_shift = PieceWidths::high_piece_shift;
    uint_fast8_t u_shift = 0;
    uint8_t u_buffer = 0;
    for (size_t i = 0; i < BufferSizes::serial_sample_count(n); i++) {
        const auto current_sample = static_cast<uint16_t>(samples[i]);
        const auto d_val = static_cast<uint16_t>(current_sample - previous_sample);
        previous_sample = current_sample;
        const auto u_bits = d_val & u_mask;
        const auto l_bits = d_val & l_mask;
        const auto h_bits = d_val & h_mask;
        *buffer_l_ptr = static_cast<uint8_t>(l_bits >> low_piece_shift);
        buffer_l_ptr++;
        *buffer_h_ptr = static_cast<uint8_t>(h_bits >> high_piece_shift);
        buffer_h_ptr++;
        u_buffer = static_cast<uint8_t>(u_buffer | (u_bits << u_shift));
        u_shift += 2;
        if (u_shift == 8) {
            *buffer_u_ptr = u_buffer;
            buffer_u_ptr++;
            u_buffer = 0;
            u_shift = 0;
        }
    }
    if (u_shift != 0) {
        *buffer_u_ptr = u_buffer;
    }
}

inline void decode_serial_section(
    const uint8_t * pdz_restrict buffer_u_ptr,
    const uint8_t * pdz_restrict buffer_l_ptr,
    const uint8_t * pdz_restrict buffer_h_ptr,
    size_t n,
    uint16_t * out)
{
    constexpr uint8_t u_mask = 0x03;
    constexpr uint_fast8_t low_piece_shift = PieceWidths::low_piece_shift;
    constexpr uint_fast8_t high_piece_shift = PieceWidths::high_piece_shift;
    uint_fast8_t u_shift = 8;
    uint8_t u_buffer = 0;
    for (size_t i = 0; i < BufferSizes::serial_sample_count(n); i++) {
        const auto h_byte = *(buffer_h_ptr++);
        const auto l_byte = *(buffer_l_ptr++);
        if (u_shift == 8) {
            u_buffer = *(buffer_u_ptr++);
            u_shift = 0;
        }
        const auto u_bits = static_cast<uint8_t>((u_buffer >> u_shift) & u_mask);
        u_shift += 2;
        // Serial tail stores the raw difference d (no Rice map) -- see encode.
        const auto d_val = static_cast<uint16_t>(
            static_cast<uint16_t>(u_bits) | static_cast<uint16_t>(l_byte << low_piece_shift)
            | static_cast<uint16_t>(h_byte << high_piece_shift));
        *(out++) = d_val;
    }
}

}}  // namespace pod5::pdz
