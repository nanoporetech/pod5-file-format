#pragma once

#include "../attributes.hpp"

#if defined(__ARM_NEON) || defined(__aarch64__)

#include <arm_neon.h>

#include <cstddef>
#include <cstdint>

#include "../rice_map_neon.hpp"
#include "buffer_sizes.hpp"
#include "piecewise_split_serial.hpp"

namespace pod5 { namespace pdz {

// NEON implementation of the PDZ piecewise split. Emits B_u / B_l / B_h in
// exactly the same byte layout as the SSE and scalar kernels. The serial tail
// is identical to the scalar reference.
class PiecewiseSplitNeon
{
public:
    static void encode(
        const int16_t * pdz_restrict samples,
        const size_t n,
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
        const uint16_t * pdz_restrict d,
        size_t n,
        uint16_t * pdz_restrict buffer_u_ptr,
        uint16_t * pdz_restrict buffer_l_ptr,
        uint16_t * pdz_restrict buffer_h_ptr);

    static void decode_parallel_section(
        const uint16_t * pdz_restrict buffer_u_ptr,
        const uint16_t * pdz_restrict buffer_l_ptr,
        const uint16_t * pdz_restrict buffer_h_ptr,
        size_t n,
        uint16_t * pdz_restrict out);

    // The serial tail is identical across all backends; see
    // piecewise_split_serial.hpp.

    static inline uint16x8_t delta_encode_chunk(const uint16x8_t curr, uint16x8_t * prev)
    {
        const auto shifted = vextq_u16(*prev, curr, 7);
        *prev = curr;
        return vsubq_u16(curr, shifted);
    }
};

inline void PiecewiseSplitNeon::encode(
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

    encode_parallel_section(
        reinterpret_cast<const uint16_t *>(samples),
        n,
        reinterpret_cast<uint16_t *>(buffer_u_ptr),
        reinterpret_cast<uint16_t *>(buffer_l_ptr),
        reinterpret_cast<uint16_t *>(buffer_h_ptr));

    encode_serial_section(
        serial_start_pointer,
        n,
        buffer_u_ptr + BufferSizes::parallel_buffer_u_size_bytes(n),
        buffer_l_ptr + BufferSizes::parallel_buffer_l_size_bytes(n),
        buffer_h_ptr + BufferSizes::parallel_buffer_h_size_bytes(n),
        previous_sample);
}

inline void PiecewiseSplitNeon::decode(
    const uint8_t * pdz_restrict buffer_u_ptr,
    const uint8_t * pdz_restrict buffer_l_ptr,
    const uint8_t * pdz_restrict buffer_h_ptr,
    size_t n,
    uint16_t * out)
{
    const size_t parallel_samples = BufferSizes::parallel_sample_count(n);

    decode_parallel_section(
        reinterpret_cast<const uint16_t *>(buffer_u_ptr),
        reinterpret_cast<const uint16_t *>(buffer_l_ptr),
        reinterpret_cast<const uint16_t *>(buffer_h_ptr),
        n,
        out);

    decode_serial_section(
        buffer_u_ptr + BufferSizes::parallel_buffer_u_size_bytes(n),
        buffer_l_ptr + BufferSizes::parallel_buffer_l_size_bytes(n),
        buffer_h_ptr + BufferSizes::parallel_buffer_h_size_bytes(n),
        n,
        out + parallel_samples);
}

inline void PiecewiseSplitNeon::encode_parallel_section(
    const uint16_t * pdz_restrict samples,
    const size_t n,
    uint16_t * pdz_restrict buffer_u_ptr,
    uint16_t * pdz_restrict buffer_l_ptr,
    uint16_t * pdz_restrict buffer_h_ptr)
{
    const uint16_t * end_ptr = samples + BufferSizes::parallel_sample_count(n);
    auto curr_ptr = samples;

    const auto u_mask = vdupq_n_u16(0x0003);
    const auto l_mask = vdupq_n_u16(0x03FC);
    const auto h_mask = vdupq_n_u16(0xFC00);
    auto previous_samples = vdupq_n_u16(0);

    for (; curr_ptr != end_ptr; curr_ptr += 64) {
        const auto chunk_0 = vld1q_u16(curr_ptr + 0);
        const auto chunk_1 = vld1q_u16(curr_ptr + 8);
        const auto chunk_2 = vld1q_u16(curr_ptr + 16);
        const auto chunk_3 = vld1q_u16(curr_ptr + 24);
        const auto chunk_4 = vld1q_u16(curr_ptr + 32);
        const auto chunk_5 = vld1q_u16(curr_ptr + 40);
        const auto chunk_6 = vld1q_u16(curr_ptr + 48);
        const auto chunk_7 = vld1q_u16(curr_ptr + 56);

        const auto z_0 = RiceMapNeon::encode_single_sample(delta_encode_chunk(chunk_0, &previous_samples));
        const auto z_1 = RiceMapNeon::encode_single_sample(delta_encode_chunk(chunk_1, &previous_samples));
        const auto z_2 = RiceMapNeon::encode_single_sample(delta_encode_chunk(chunk_2, &previous_samples));
        const auto z_3 = RiceMapNeon::encode_single_sample(delta_encode_chunk(chunk_3, &previous_samples));
        const auto z_4 = RiceMapNeon::encode_single_sample(delta_encode_chunk(chunk_4, &previous_samples));
        const auto z_5 = RiceMapNeon::encode_single_sample(delta_encode_chunk(chunk_5, &previous_samples));
        const auto z_6 = RiceMapNeon::encode_single_sample(delta_encode_chunk(chunk_6, &previous_samples));
        const auto z_7 = RiceMapNeon::encode_single_sample(delta_encode_chunk(chunk_7, &previous_samples));

        const auto u_0 = vandq_u16(z_0, u_mask);
        const auto u_1 = vandq_u16(z_1, u_mask);
        const auto u_2 = vandq_u16(z_2, u_mask);
        const auto u_3 = vandq_u16(z_3, u_mask);
        const auto u_4 = vandq_u16(z_4, u_mask);
        const auto u_5 = vandq_u16(z_5, u_mask);
        const auto u_6 = vandq_u16(z_6, u_mask);
        const auto u_7 = vandq_u16(z_7, u_mask);

        const auto l_0 = vandq_u16(z_0, l_mask);
        const auto l_1 = vandq_u16(z_1, l_mask);
        const auto l_2 = vandq_u16(z_2, l_mask);
        const auto l_3 = vandq_u16(z_3, l_mask);
        const auto l_4 = vandq_u16(z_4, l_mask);
        const auto l_5 = vandq_u16(z_5, l_mask);
        const auto l_6 = vandq_u16(z_6, l_mask);
        const auto l_7 = vandq_u16(z_7, l_mask);

        const auto h_0 = vandq_u16(z_0, h_mask);
        const auto h_1 = vandq_u16(z_1, h_mask);
        const auto h_2 = vandq_u16(z_2, h_mask);
        const auto h_3 = vandq_u16(z_3, h_mask);
        const auto h_4 = vandq_u16(z_4, h_mask);
        const auto h_5 = vandq_u16(z_5, h_mask);
        const auto h_6 = vandq_u16(z_6, h_mask);
        const auto h_7 = vandq_u16(z_7, h_mask);

        const auto packed_h_0 = vorrq_u16(h_1, vshrq_n_u16(h_0, 8));
        const auto packed_h_1 = vorrq_u16(h_3, vshrq_n_u16(h_2, 8));
        const auto packed_h_2 = vorrq_u16(h_5, vshrq_n_u16(h_4, 8));
        const auto packed_h_3 = vorrq_u16(h_7, vshrq_n_u16(h_6, 8));

        const auto packed_l_0 = vorrq_u16(vshlq_n_u16(l_1, 6), vshrq_n_u16(l_0, 2));
        const auto packed_l_1 = vorrq_u16(vshlq_n_u16(l_3, 6), vshrq_n_u16(l_2, 2));
        const auto packed_l_2 = vorrq_u16(vshlq_n_u16(l_5, 6), vshrq_n_u16(l_4, 2));
        const auto packed_l_3 = vorrq_u16(vshlq_n_u16(l_7, 6), vshrq_n_u16(l_6, 2));

        const auto packed_u = vorrq_u16(
            vorrq_u16(
                vorrq_u16(
                    vorrq_u16(
                        vorrq_u16(
                            vorrq_u16(
                                vorrq_u16(vshlq_n_u16(u_7, 14), vshlq_n_u16(u_6, 12)),
                                vshlq_n_u16(u_5, 10)),
                            vshlq_n_u16(u_4, 8)),
                        vshlq_n_u16(u_3, 6)),
                    vshlq_n_u16(u_2, 4)),
                vshlq_n_u16(u_1, 2)),
            u_0);

        vst1q_u16(buffer_h_ptr, packed_h_0);
        buffer_h_ptr += 8;
        vst1q_u16(buffer_h_ptr, packed_h_1);
        buffer_h_ptr += 8;
        vst1q_u16(buffer_h_ptr, packed_h_2);
        buffer_h_ptr += 8;
        vst1q_u16(buffer_h_ptr, packed_h_3);
        buffer_h_ptr += 8;

        vst1q_u16(buffer_l_ptr, packed_l_0);
        buffer_l_ptr += 8;
        vst1q_u16(buffer_l_ptr, packed_l_1);
        buffer_l_ptr += 8;
        vst1q_u16(buffer_l_ptr, packed_l_2);
        buffer_l_ptr += 8;
        vst1q_u16(buffer_l_ptr, packed_l_3);
        buffer_l_ptr += 8;

        vst1q_u16(buffer_u_ptr, packed_u);
        buffer_u_ptr += 8;
    }
}

inline void PiecewiseSplitNeon::decode_parallel_section(
    const uint16_t * pdz_restrict buffer_u_ptr,
    const uint16_t * pdz_restrict buffer_l_ptr,
    const uint16_t * pdz_restrict buffer_h_ptr,
    size_t n,
    uint16_t * pdz_restrict out)
{
    const auto u_extract_mask = vdupq_n_u16(0x0003);
    const auto low_byte_extract_mask = vdupq_n_u16(0x00FF);
    const auto high_byte_extract_mask = vdupq_n_u16(0xFF00);

    size_t remaining_iterations = 2 * (BufferSizes::parallel_sample_count(n) / (8 * 16));
    for (; remaining_iterations != 0; --remaining_iterations) {
        const auto u_data = vld1q_u16(buffer_u_ptr);
        buffer_u_ptr += 8;
        const auto h_quarter_0 = vld1q_u16(buffer_h_ptr);
        buffer_h_ptr += 8;
        const auto h_quarter_1 = vld1q_u16(buffer_h_ptr);
        buffer_h_ptr += 8;
        const auto h_quarter_2 = vld1q_u16(buffer_h_ptr);
        buffer_h_ptr += 8;
        const auto h_quarter_3 = vld1q_u16(buffer_h_ptr);
        buffer_h_ptr += 8;
        const auto l_quarter_0 = vld1q_u16(buffer_l_ptr);
        buffer_l_ptr += 8;
        const auto l_quarter_1 = vld1q_u16(buffer_l_ptr);
        buffer_l_ptr += 8;
        const auto l_quarter_2 = vld1q_u16(buffer_l_ptr);
        buffer_l_ptr += 8;
        const auto l_quarter_3 = vld1q_u16(buffer_l_ptr);
        buffer_l_ptr += 8;

        const auto first_u = vandq_u16(u_data, u_extract_mask);
        const auto first_l = vshlq_n_u16(vandq_u16(l_quarter_0, low_byte_extract_mask), 2);
        const auto first_h = vshlq_n_u16(vandq_u16(h_quarter_0, low_byte_extract_mask), 8);
        const auto first_z = vorrq_u16(first_u, vorrq_u16(first_l, first_h));
        vst1q_u16(out, RiceMapNeon::decode_single_sample(first_z));
        out += 8;

        const auto second_u = vandq_u16(vshrq_n_u16(u_data, 2), u_extract_mask);
        const auto second_l = vshrq_n_u16(vandq_u16(l_quarter_0, high_byte_extract_mask), 6);
        const auto second_h = vandq_u16(h_quarter_0, high_byte_extract_mask);
        const auto second_z = vorrq_u16(second_u, vorrq_u16(second_l, second_h));
        vst1q_u16(out, RiceMapNeon::decode_single_sample(second_z));
        out += 8;

        const auto third_u = vandq_u16(vshrq_n_u16(u_data, 4), u_extract_mask);
        const auto third_l = vshlq_n_u16(vandq_u16(l_quarter_1, low_byte_extract_mask), 2);
        const auto third_h = vshlq_n_u16(vandq_u16(h_quarter_1, low_byte_extract_mask), 8);
        const auto third_z = vorrq_u16(third_u, vorrq_u16(third_l, third_h));
        vst1q_u16(out, RiceMapNeon::decode_single_sample(third_z));
        out += 8;

        const auto fourth_u = vandq_u16(vshrq_n_u16(u_data, 6), u_extract_mask);
        const auto fourth_l = vshrq_n_u16(vandq_u16(l_quarter_1, high_byte_extract_mask), 6);
        const auto fourth_h = vandq_u16(h_quarter_1, high_byte_extract_mask);
        const auto fourth_z = vorrq_u16(fourth_u, vorrq_u16(fourth_l, fourth_h));
        vst1q_u16(out, RiceMapNeon::decode_single_sample(fourth_z));
        out += 8;

        const auto fifth_u = vandq_u16(vshrq_n_u16(u_data, 8), u_extract_mask);
        const auto fifth_l = vshlq_n_u16(vandq_u16(l_quarter_2, low_byte_extract_mask), 2);
        const auto fifth_h = vshlq_n_u16(vandq_u16(h_quarter_2, low_byte_extract_mask), 8);
        const auto fifth_z = vorrq_u16(fifth_u, vorrq_u16(fifth_l, fifth_h));
        vst1q_u16(out, RiceMapNeon::decode_single_sample(fifth_z));
        out += 8;

        const auto sixth_u = vandq_u16(vshrq_n_u16(u_data, 10), u_extract_mask);
        const auto sixth_l = vshrq_n_u16(vandq_u16(l_quarter_2, high_byte_extract_mask), 6);
        const auto sixth_h = vandq_u16(h_quarter_2, high_byte_extract_mask);
        const auto sixth_z = vorrq_u16(sixth_u, vorrq_u16(sixth_l, sixth_h));
        vst1q_u16(out, RiceMapNeon::decode_single_sample(sixth_z));
        out += 8;

        const auto seventh_u = vandq_u16(vshrq_n_u16(u_data, 12), u_extract_mask);
        const auto seventh_l = vshlq_n_u16(vandq_u16(l_quarter_3, low_byte_extract_mask), 2);
        const auto seventh_h = vshlq_n_u16(vandq_u16(h_quarter_3, low_byte_extract_mask), 8);
        const auto seventh_z = vorrq_u16(seventh_u, vorrq_u16(seventh_l, seventh_h));
        vst1q_u16(out, RiceMapNeon::decode_single_sample(seventh_z));
        out += 8;

        const auto eighth_u = vandq_u16(vshrq_n_u16(u_data, 14), u_extract_mask);
        const auto eighth_l = vshrq_n_u16(vandq_u16(l_quarter_3, high_byte_extract_mask), 6);
        const auto eighth_h = vandq_u16(h_quarter_3, high_byte_extract_mask);
        const auto eighth_z = vorrq_u16(eighth_u, vorrq_u16(eighth_l, eighth_h));
        vst1q_u16(out, RiceMapNeon::decode_single_sample(eighth_z));
        out += 8;
    }
}

}}  // namespace pod5::pdz

#endif
