#pragma once

#include "../attributes.hpp"

#if defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64)

#include <immintrin.h>

#include <cstddef>
#include <cstdint>

#include "../rice_map_sse.hpp"
#include "buffer_sizes.hpp"
#include "piecewise_split_serial.hpp"

namespace pod5 { namespace pdz {

// SSE implementation of the PDZ piecewise split. Emits B_u / B_l / B_h in
// exactly the same byte layout as the NEON and scalar kernels. The serial tail
// is identical to the scalar reference.
class PiecewiseSplitSse
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
        const __m128i * pdz_restrict samples,
        const size_t n,
        __m128i * pdz_restrict buffer_u_ptr,
        __m128i * pdz_restrict buffer_l_ptr,
        __m128i * pdz_restrict buffer_h_ptr);

    static void decode_parallel_section(
        const __m128i * pdz_restrict buffer_u_ptr,
        const __m128i * pdz_restrict buffer_l_ptr,
        const __m128i * pdz_restrict buffer_h_ptr,
        size_t n,
        __m128i * out);

    // The serial tail is identical across all backends; see
    // piecewise_split_serial.hpp.

    static inline void store(__m128i * ptr, const __m128i & x) { _mm_storeu_si128(ptr, x); }

    static inline __m128i load(const __m128i * ptr) { return _mm_loadu_si128(ptr); }

    static inline __m128i delta_encode_chunk(const __m128i & curr, __m128i * prev)
    {
        const auto shifted = _mm_or_si128(_mm_slli_si128(curr, 2), _mm_srli_si128(*prev, 14));
        *prev = curr;
        return _mm_sub_epi16(curr, shifted);
    }
};

inline void PiecewiseSplitSse::encode(
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
        reinterpret_cast<const __m128i *>(samples),
        n,
        reinterpret_cast<__m128i *>(buffer_u_ptr),
        reinterpret_cast<__m128i *>(buffer_l_ptr),
        reinterpret_cast<__m128i *>(buffer_h_ptr));

    encode_serial_section(
        serial_start_pointer,
        n,
        buffer_u_ptr + BufferSizes::parallel_buffer_u_size_bytes(n),
        buffer_l_ptr + BufferSizes::parallel_buffer_l_size_bytes(n),
        buffer_h_ptr + BufferSizes::parallel_buffer_h_size_bytes(n),
        previous_sample);
}

inline void PiecewiseSplitSse::decode(
    const uint8_t * pdz_restrict buffer_u_ptr,
    const uint8_t * pdz_restrict buffer_l_ptr,
    const uint8_t * pdz_restrict buffer_h_ptr,
    size_t n,
    uint16_t * out)
{
    const size_t parallel_samples = BufferSizes::parallel_sample_count(n);
    decode_parallel_section(
        reinterpret_cast<const __m128i *>(buffer_u_ptr),
        reinterpret_cast<const __m128i *>(buffer_l_ptr),
        reinterpret_cast<const __m128i *>(buffer_h_ptr),
        n,
        reinterpret_cast<__m128i *>(out));

    decode_serial_section(
        buffer_u_ptr + BufferSizes::parallel_buffer_u_size_bytes(n),
        buffer_l_ptr + BufferSizes::parallel_buffer_l_size_bytes(n),
        buffer_h_ptr + BufferSizes::parallel_buffer_h_size_bytes(n),
        n,
        out + parallel_samples);
}

inline void PiecewiseSplitSse::encode_parallel_section(
    const __m128i * pdz_restrict samples,
    const size_t n,
    __m128i * pdz_restrict buffer_u_ptr,
    __m128i * pdz_restrict buffer_l_ptr,
    __m128i * pdz_restrict buffer_h_ptr)
{
    // Process 8 SSE registers (64 samples) per iteration.
    const __m128i * end_ptr = samples + 2 * (BufferSizes::parallel_sample_count(n) / 16);
    auto curr_ptr = samples;

    const __m128i u_mask = _mm_set1_epi16(0x0003);
    const __m128i l_mask = _mm_set1_epi16(0x03FC);
    const __m128i h_mask = _mm_set1_epi16(static_cast<short>(0xFC00));
    auto previous_samples = _mm_setzero_si128();

    for (; curr_ptr != end_ptr;) {
        const auto chunk_0 = load(curr_ptr++);
        const auto chunk_1 = load(curr_ptr++);
        const auto chunk_2 = load(curr_ptr++);
        const auto chunk_3 = load(curr_ptr++);
        const auto chunk_4 = load(curr_ptr++);
        const auto chunk_5 = load(curr_ptr++);
        const auto chunk_6 = load(curr_ptr++);
        const auto chunk_7 = load(curr_ptr++);

        const auto z_0 = RiceMapSse::encode_single_sample(delta_encode_chunk(chunk_0, &previous_samples));
        const auto z_1 = RiceMapSse::encode_single_sample(delta_encode_chunk(chunk_1, &previous_samples));
        const auto z_2 = RiceMapSse::encode_single_sample(delta_encode_chunk(chunk_2, &previous_samples));
        const auto z_3 = RiceMapSse::encode_single_sample(delta_encode_chunk(chunk_3, &previous_samples));
        const auto z_4 = RiceMapSse::encode_single_sample(delta_encode_chunk(chunk_4, &previous_samples));
        const auto z_5 = RiceMapSse::encode_single_sample(delta_encode_chunk(chunk_5, &previous_samples));
        const auto z_6 = RiceMapSse::encode_single_sample(delta_encode_chunk(chunk_6, &previous_samples));
        const auto z_7 = RiceMapSse::encode_single_sample(delta_encode_chunk(chunk_7, &previous_samples));

        const auto u_0 = _mm_and_si128(z_0, u_mask);
        const auto u_1 = _mm_and_si128(z_1, u_mask);
        const auto u_2 = _mm_and_si128(z_2, u_mask);
        const auto u_3 = _mm_and_si128(z_3, u_mask);
        const auto u_4 = _mm_and_si128(z_4, u_mask);
        const auto u_5 = _mm_and_si128(z_5, u_mask);
        const auto u_6 = _mm_and_si128(z_6, u_mask);
        const auto u_7 = _mm_and_si128(z_7, u_mask);

        const auto l_0 = _mm_and_si128(z_0, l_mask);
        const auto l_1 = _mm_and_si128(z_1, l_mask);
        const auto l_2 = _mm_and_si128(z_2, l_mask);
        const auto l_3 = _mm_and_si128(z_3, l_mask);
        const auto l_4 = _mm_and_si128(z_4, l_mask);
        const auto l_5 = _mm_and_si128(z_5, l_mask);
        const auto l_6 = _mm_and_si128(z_6, l_mask);
        const auto l_7 = _mm_and_si128(z_7, l_mask);

        const auto h_0 = _mm_and_si128(z_0, h_mask);
        const auto h_1 = _mm_and_si128(z_1, h_mask);
        const auto h_2 = _mm_and_si128(z_2, h_mask);
        const auto h_3 = _mm_and_si128(z_3, h_mask);
        const auto h_4 = _mm_and_si128(z_4, h_mask);
        const auto h_5 = _mm_and_si128(z_5, h_mask);
        const auto h_6 = _mm_and_si128(z_6, h_mask);
        const auto h_7 = _mm_and_si128(z_7, h_mask);

        const auto packed_h_0 = _mm_or_si128(h_1, _mm_srli_epi16(h_0, 8));
        const auto packed_h_1 = _mm_or_si128(h_3, _mm_srli_epi16(h_2, 8));
        const auto packed_h_2 = _mm_or_si128(h_5, _mm_srli_epi16(h_4, 8));
        const auto packed_h_3 = _mm_or_si128(h_7, _mm_srli_epi16(h_6, 8));

        const auto packed_l_0 = _mm_or_si128(_mm_slli_epi16(l_1, 6), _mm_srli_epi16(l_0, 2));
        const auto packed_l_1 = _mm_or_si128(_mm_slli_epi16(l_3, 6), _mm_srli_epi16(l_2, 2));
        const auto packed_l_2 = _mm_or_si128(_mm_slli_epi16(l_5, 6), _mm_srli_epi16(l_4, 2));
        const auto packed_l_3 = _mm_or_si128(_mm_slli_epi16(l_7, 6), _mm_srli_epi16(l_6, 2));

        const auto packed_u = _mm_or_si128(
            _mm_or_si128(
                _mm_or_si128(
                    _mm_or_si128(
                        _mm_or_si128(
                            _mm_or_si128(
                                _mm_or_si128(
                                    _mm_slli_epi16(u_7, 14), _mm_slli_epi16(u_6, 12)),
                                _mm_slli_epi16(u_5, 10)),
                            _mm_slli_epi16(u_4, 8)),
                        _mm_slli_epi16(u_3, 6)),
                    _mm_slli_epi16(u_2, 4)),
                _mm_slli_epi16(u_1, 2)),
            u_0);

        store(buffer_h_ptr++, packed_h_0);
        store(buffer_h_ptr++, packed_h_1);
        store(buffer_h_ptr++, packed_h_2);
        store(buffer_h_ptr++, packed_h_3);

        store(buffer_l_ptr++, packed_l_0);
        store(buffer_l_ptr++, packed_l_1);
        store(buffer_l_ptr++, packed_l_2);
        store(buffer_l_ptr++, packed_l_3);

        store(buffer_u_ptr++, packed_u);
    }
}

inline void PiecewiseSplitSse::decode_parallel_section(
    const __m128i * pdz_restrict buffer_u_ptr,
    const __m128i * pdz_restrict buffer_l_ptr,
    const __m128i * pdz_restrict buffer_h_ptr,
    size_t n,
    __m128i * out)
{
    const auto u_extract_mask = _mm_set1_epi16(0x0003);
    const auto low_byte_extract_mask = _mm_set1_epi16(0x00FF);
    const auto high_byte_extract_mask = _mm_set1_epi16(static_cast<short>(0xFF00));

    size_t remaining_iterations = 2 * (BufferSizes::parallel_sample_count(n) / (8 * 16));
    for (; remaining_iterations != 0; remaining_iterations--) {
        const auto u_data = load(buffer_u_ptr++);
        const auto h_quarter_0 = load(buffer_h_ptr++);
        const auto h_quarter_1 = load(buffer_h_ptr++);
        const auto h_quarter_2 = load(buffer_h_ptr++);
        const auto h_quarter_3 = load(buffer_h_ptr++);
        const auto l_quarter_0 = load(buffer_l_ptr++);
        const auto l_quarter_1 = load(buffer_l_ptr++);
        const auto l_quarter_2 = load(buffer_l_ptr++);
        const auto l_quarter_3 = load(buffer_l_ptr++);

        const auto first_u = _mm_and_si128(u_data, u_extract_mask);
        const auto first_l = _mm_slli_epi16(_mm_and_si128(l_quarter_0, low_byte_extract_mask), 2);
        const auto first_h = _mm_slli_epi16(_mm_and_si128(h_quarter_0, low_byte_extract_mask), 8);
        const auto first_z = _mm_or_si128(first_u, _mm_or_si128(first_l, first_h));
        store(out++, RiceMapSse::decode_single_sample(first_z));

        const auto second_u = _mm_and_si128(_mm_srli_epi16(u_data, 2), u_extract_mask);
        const auto second_l = _mm_srli_epi16(_mm_and_si128(l_quarter_0, high_byte_extract_mask), 6);
        const auto second_h = _mm_and_si128(h_quarter_0, high_byte_extract_mask);
        const auto second_z = _mm_or_si128(second_u, _mm_or_si128(second_l, second_h));
        store(out++, RiceMapSse::decode_single_sample(second_z));

        const auto third_u = _mm_and_si128(_mm_srli_epi16(u_data, 4), u_extract_mask);
        const auto third_l = _mm_slli_epi16(_mm_and_si128(l_quarter_1, low_byte_extract_mask), 2);
        const auto third_h = _mm_slli_epi16(_mm_and_si128(h_quarter_1, low_byte_extract_mask), 8);
        const auto third_z = _mm_or_si128(third_u, _mm_or_si128(third_l, third_h));
        store(out++, RiceMapSse::decode_single_sample(third_z));

        const auto fourth_u = _mm_and_si128(_mm_srli_epi16(u_data, 6), u_extract_mask);
        const auto fourth_l = _mm_srli_epi16(_mm_and_si128(l_quarter_1, high_byte_extract_mask), 6);
        const auto fourth_h = _mm_and_si128(h_quarter_1, high_byte_extract_mask);
        const auto fourth_z = _mm_or_si128(fourth_u, _mm_or_si128(fourth_l, fourth_h));
        store(out++, RiceMapSse::decode_single_sample(fourth_z));

        const auto fifth_u = _mm_and_si128(_mm_srli_epi16(u_data, 8), u_extract_mask);
        const auto fifth_l = _mm_slli_epi16(_mm_and_si128(l_quarter_2, low_byte_extract_mask), 2);
        const auto fifth_h = _mm_slli_epi16(_mm_and_si128(h_quarter_2, low_byte_extract_mask), 8);
        const auto fifth_z = _mm_or_si128(fifth_u, _mm_or_si128(fifth_l, fifth_h));
        store(out++, RiceMapSse::decode_single_sample(fifth_z));

        const auto sixth_u = _mm_and_si128(_mm_srli_epi16(u_data, 10), u_extract_mask);
        const auto sixth_l = _mm_srli_epi16(_mm_and_si128(l_quarter_2, high_byte_extract_mask), 6);
        const auto sixth_h = _mm_and_si128(h_quarter_2, high_byte_extract_mask);
        const auto sixth_z = _mm_or_si128(sixth_u, _mm_or_si128(sixth_l, sixth_h));
        store(out++, RiceMapSse::decode_single_sample(sixth_z));

        const auto seventh_u = _mm_and_si128(_mm_srli_epi16(u_data, 12), u_extract_mask);
        const auto seventh_l = _mm_slli_epi16(_mm_and_si128(l_quarter_3, low_byte_extract_mask), 2);
        const auto seventh_h = _mm_slli_epi16(_mm_and_si128(h_quarter_3, low_byte_extract_mask), 8);
        const auto seventh_z = _mm_or_si128(seventh_u, _mm_or_si128(seventh_l, seventh_h));
        store(out++, RiceMapSse::decode_single_sample(seventh_z));

        const auto eighth_u = _mm_and_si128(_mm_srli_epi16(u_data, 14), u_extract_mask);
        const auto eighth_l = _mm_srli_epi16(_mm_and_si128(l_quarter_3, high_byte_extract_mask), 6);
        const auto eighth_h = _mm_and_si128(h_quarter_3, high_byte_extract_mask);
        const auto eighth_z = _mm_or_si128(eighth_u, _mm_or_si128(eighth_l, eighth_h));
        store(out++, RiceMapSse::decode_single_sample(eighth_z));
    }
}

}}  // namespace pod5::pdz

#endif
