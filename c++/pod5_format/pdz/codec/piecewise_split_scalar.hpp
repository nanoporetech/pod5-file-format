#pragma once

#include "../attributes.hpp"
#include "../rice_map_sse.hpp"
#include "buffer_lh_writer.hpp"
#include "buffer_readers.hpp"
#include "buffer_sizes.hpp"
#include "buffer_u_writer.hpp"
#include "piecewise_split_serial.hpp"

#include <cstddef>
#include <cstdint>

namespace pod5 { namespace pdz {

// Portable reference implementation of the PDZ piecewise split. It produces
// byte-for-byte identical B_u / B_l / B_h to the SSE and NEON kernels, so it is
// the oracle for cross-architecture compatibility.
//
// The input is the original signal x; differential coding to d and the Rice /
// zig-zag mapping to z happen inside this kernel. Buffers are split into a
// vectorised region (whole 128-sample blocks, laid out to mirror the SIMD
// kernels) and a scalar tail for the remainder.
class PiecewiseSplitScalar
{
public:
    static void encode(
        const int16_t * pdz_restrict samples,
        const std::size_t n,
        uint8_t * pdz_restrict buffer_u_ptr,
        uint8_t * pdz_restrict buffer_l_ptr,
        uint8_t * pdz_restrict buffer_h_ptr);

    static void decode(
        const uint8_t * pdz_restrict buffer_u_ptr,
        const uint8_t * pdz_restrict buffer_l_ptr,
        const uint8_t * pdz_restrict buffer_h_ptr,
        size_t n,
        uint16_t * out);

private:
    static void encode_parallel_section(
        const int16_t * pdz_restrict samples,
        const std::size_t n,
        uint8_t * pdz_restrict buffer_u_ptr,
        uint8_t * pdz_restrict buffer_l_ptr,
        uint8_t * pdz_restrict buffer_h_ptr);

    static void decode_parallel_section(
        const uint8_t * pdz_restrict buffer_u_ptr,
        const uint8_t * pdz_restrict buffer_l_ptr,
        const uint8_t * pdz_restrict buffer_h_ptr,
        size_t n,
        uint16_t * out);

    // The serial tail is identical across all backends; see
    // piecewise_split_serial.hpp.

    static constexpr uint_fast8_t low_piece_shift = PieceWidths::low_piece_shift;  // = 2

    // B_h in the vectorised section stores the whole high byte of z (z >> 8), so
    // the 6-bit h piece (z bits 10..15) lands pre-shifted by low_piece_shift (=2)
    // inside that byte. This is the layout the SSE/NEON kernels get for free from
    // the high byte, which is why the shift here is 8 and not high_piece_shift
    // (=10). The serial tail instead normalises h (>> high_piece_shift); each
    // section's decode applies the matching inverse shift, so the two conventions
    // never mix within a buffer.
    static constexpr uint_fast8_t high_byte_shift = 8;
};

inline void PiecewiseSplitScalar::encode(
    const int16_t * pdz_restrict samples,
    const size_t n,
    uint8_t * pdz_restrict buffer_u_ptr,
    uint8_t * pdz_restrict buffer_l_ptr,
    uint8_t * pdz_restrict buffer_h_ptr)
{
    const size_t parallel_samples = BufferSizes::parallel_sample_count(n);
    const int16_t * pdz_restrict serial_start_pointer = samples + parallel_samples;
    const auto previous_sample = parallel_samples == 0
        ? static_cast<uint16_t>(0)
        : static_cast<uint16_t>(samples[parallel_samples - 1]);
    encode_parallel_section(samples, n, buffer_u_ptr, buffer_l_ptr, buffer_h_ptr);

    encode_serial_section(
        serial_start_pointer,
        n,
        buffer_u_ptr + BufferSizes::parallel_buffer_u_size_bytes(n),
        buffer_l_ptr + BufferSizes::parallel_buffer_l_size_bytes(n),
        buffer_h_ptr + BufferSizes::parallel_buffer_h_size_bytes(n),
        previous_sample);
}

inline void PiecewiseSplitScalar::decode(
    const uint8_t * pdz_restrict buffer_u_ptr,
    const uint8_t * pdz_restrict buffer_l_ptr,
    const uint8_t * pdz_restrict buffer_h_ptr,
    size_t n,
    uint16_t * out)
{
    const size_t parallel_samples = BufferSizes::parallel_sample_count(n);
    decode_parallel_section(buffer_u_ptr, buffer_l_ptr, buffer_h_ptr, n, out);

    decode_serial_section(
        buffer_u_ptr + BufferSizes::parallel_buffer_u_size_bytes(n),
        buffer_l_ptr + BufferSizes::parallel_buffer_l_size_bytes(n),
        buffer_h_ptr + BufferSizes::parallel_buffer_h_size_bytes(n),
        n,
        out + parallel_samples);
}

inline void PiecewiseSplitScalar::encode_parallel_section(
    const int16_t * pdz_restrict samples,
    const std::size_t n,
    uint8_t * pdz_restrict buffer_u_ptr,
    uint8_t * pdz_restrict buffer_l_ptr,
    uint8_t * pdz_restrict buffer_h_ptr)
{
    const auto iterations = BufferSizes::parallel_sample_count(n) / 8;

    const uint16_t u_mask = 0x0003;  // u = 2 low bits          -> B_u
    const uint16_t l_mask = 0x03FC;  // l = 8 bits (bits 2..9)  -> B_l
    const uint16_t h_mask = 0xFC00;  // h = 6 bits (bits 10..15) -> B_h

    BufferUWriter buffer_u_writer(buffer_u_ptr);
    BufferLHWriter buffer_l_writer(buffer_l_ptr);
    BufferLHWriter buffer_h_writer(buffer_h_ptr);
    uint16_t previous_sample = 0;

    for (size_t i = 0; i < iterations; ++i) {
        const auto sample_0 = static_cast<uint16_t>(*samples++);
        const auto d_0 = static_cast<uint16_t>(sample_0 - previous_sample);
        previous_sample = sample_0;
        const auto sample_1 = static_cast<uint16_t>(*samples++);
        const auto d_1 = static_cast<uint16_t>(sample_1 - previous_sample);
        previous_sample = sample_1;
        const auto sample_2 = static_cast<uint16_t>(*samples++);
        const auto d_2 = static_cast<uint16_t>(sample_2 - previous_sample);
        previous_sample = sample_2;
        const auto sample_3 = static_cast<uint16_t>(*samples++);
        const auto d_3 = static_cast<uint16_t>(sample_3 - previous_sample);
        previous_sample = sample_3;
        const auto sample_4 = static_cast<uint16_t>(*samples++);
        const auto d_4 = static_cast<uint16_t>(sample_4 - previous_sample);
        previous_sample = sample_4;
        const auto sample_5 = static_cast<uint16_t>(*samples++);
        const auto d_5 = static_cast<uint16_t>(sample_5 - previous_sample);
        previous_sample = sample_5;
        const auto sample_6 = static_cast<uint16_t>(*samples++);
        const auto d_6 = static_cast<uint16_t>(sample_6 - previous_sample);
        previous_sample = sample_6;
        const auto sample_7 = static_cast<uint16_t>(*samples++);
        const auto d_7 = static_cast<uint16_t>(sample_7 - previous_sample);
        previous_sample = sample_7;

        const auto z_0 = RiceMapSse::encode_single_sample(static_cast<int16_t>(d_0));
        const auto z_1 = RiceMapSse::encode_single_sample(static_cast<int16_t>(d_1));
        const auto z_2 = RiceMapSse::encode_single_sample(static_cast<int16_t>(d_2));
        const auto z_3 = RiceMapSse::encode_single_sample(static_cast<int16_t>(d_3));
        const auto z_4 = RiceMapSse::encode_single_sample(static_cast<int16_t>(d_4));
        const auto z_5 = RiceMapSse::encode_single_sample(static_cast<int16_t>(d_5));
        const auto z_6 = RiceMapSse::encode_single_sample(static_cast<int16_t>(d_6));
        const auto z_7 = RiceMapSse::encode_single_sample(static_cast<int16_t>(d_7));

        const uint8_t l_0 = static_cast<uint8_t>((z_0 & l_mask) >> low_piece_shift);
        const uint8_t l_1 = static_cast<uint8_t>((z_1 & l_mask) >> low_piece_shift);
        const uint8_t l_2 = static_cast<uint8_t>((z_2 & l_mask) >> low_piece_shift);
        const uint8_t l_3 = static_cast<uint8_t>((z_3 & l_mask) >> low_piece_shift);
        const uint8_t l_4 = static_cast<uint8_t>((z_4 & l_mask) >> low_piece_shift);
        const uint8_t l_5 = static_cast<uint8_t>((z_5 & l_mask) >> low_piece_shift);
        const uint8_t l_6 = static_cast<uint8_t>((z_6 & l_mask) >> low_piece_shift);
        const uint8_t l_7 = static_cast<uint8_t>((z_7 & l_mask) >> low_piece_shift);

        buffer_l_writer.write(l_0, l_1, l_2, l_3, l_4, l_5, l_6, l_7);

        const uint8_t h_0 = static_cast<uint8_t>((z_0 & h_mask) >> high_byte_shift);
        const uint8_t h_1 = static_cast<uint8_t>((z_1 & h_mask) >> high_byte_shift);
        const uint8_t h_2 = static_cast<uint8_t>((z_2 & h_mask) >> high_byte_shift);
        const uint8_t h_3 = static_cast<uint8_t>((z_3 & h_mask) >> high_byte_shift);
        const uint8_t h_4 = static_cast<uint8_t>((z_4 & h_mask) >> high_byte_shift);
        const uint8_t h_5 = static_cast<uint8_t>((z_5 & h_mask) >> high_byte_shift);
        const uint8_t h_6 = static_cast<uint8_t>((z_6 & h_mask) >> high_byte_shift);
        const uint8_t h_7 = static_cast<uint8_t>((z_7 & h_mask) >> high_byte_shift);

        buffer_h_writer.write(h_0, h_1, h_2, h_3, h_4, h_5, h_6, h_7);

        const uint8_t u_0 = static_cast<uint8_t>(z_0 & u_mask);
        const uint8_t u_1 = static_cast<uint8_t>(z_1 & u_mask);
        const uint8_t u_2 = static_cast<uint8_t>(z_2 & u_mask);
        const uint8_t u_3 = static_cast<uint8_t>(z_3 & u_mask);
        const uint8_t u_4 = static_cast<uint8_t>(z_4 & u_mask);
        const uint8_t u_5 = static_cast<uint8_t>(z_5 & u_mask);
        const uint8_t u_6 = static_cast<uint8_t>(z_6 & u_mask);
        const uint8_t u_7 = static_cast<uint8_t>(z_7 & u_mask);

        buffer_u_writer.write(u_0, u_1, u_2, u_3, u_4, u_5, u_6, u_7);
    }
}

inline void PiecewiseSplitScalar::decode_parallel_section(
    const uint8_t * pdz_restrict buffer_u_ptr,
    const uint8_t * pdz_restrict buffer_l_ptr,
    const uint8_t * pdz_restrict buffer_h_ptr,
    size_t n,
    uint16_t * out)
{
    size_t iterations = BufferSizes::parallel_sample_count(n) / 8;
    BufferUStream buffer_u_stream(buffer_u_ptr);
    BufferLHStream buffer_l_stream(buffer_l_ptr);
    BufferLHStream buffer_h_stream(buffer_h_ptr);

    for (size_t i = 0; i < iterations; ++i) {
        const auto u_block = buffer_u_stream.next();

        for (size_t j = 0; j < 8; ++j) {
            const auto l_piece = buffer_l_stream.next() << low_piece_shift;
            const auto h_piece = buffer_h_stream.next() << high_byte_shift;
            const auto z = static_cast<uint16_t>(u_block.get(j) | l_piece | h_piece);
            *out++ = static_cast<uint16_t>(RiceMapSse::decode_single_sample(z));
        }
    }
}

}}  // namespace pod5::pdz
