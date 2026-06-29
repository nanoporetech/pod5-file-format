#pragma once

#include "../attributes.hpp"
#include "../codecs.hpp"
#include "../memory_utils.hpp"
#include "../simd_backend.hpp"
#include "buffer_sizes.hpp"
#include "piecewise_split_scalar.hpp"

#if defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64)
#include "piecewise_split_sse.hpp"
#endif
#if defined(__ARM_NEON) || defined(__aarch64__)
#include "piecewise_split_neon.hpp"
#endif

#include <zstd.h>

#include <cstddef>
#include <cstdint>
#include <limits>

namespace pod5 { namespace pdz {

namespace detail {

constexpr std::size_t split_scratch_alignment_bytes = 32;

inline std::size_t aligned_storage_bytes(std::size_t logical_bytes)
{
    if (logical_bytes == 0) {
        return split_scratch_alignment_bytes;
    }

    auto const remainder = logical_bytes % split_scratch_alignment_bytes;
    if (remainder == 0) {
        return logical_bytes;
    }

    return logical_bytes + split_scratch_alignment_bytes - remainder;
}

struct SplitScratch {
    uint8_t * storage;
    uint8_t * buffer_l;
    uint8_t * buffer_h;
};

inline SplitScratch allocate_split_scratch(std::size_t sample_count)
{
    auto const buffer_l_storage_bytes = aligned_storage_bytes(BufferSizes::buffer_l_size_bytes(sample_count));
    auto const buffer_h_storage_bytes = aligned_storage_bytes(BufferSizes::buffer_h_size_bytes(sample_count));
    auto * const storage = padded_aligned_malloc(
        buffer_l_storage_bytes + buffer_h_storage_bytes,
        split_scratch_alignment_bytes);

    return {storage, storage, storage == nullptr ? nullptr : storage + buffer_l_storage_bytes};
}

struct ZstdThreadContexts {
    ZSTD_CCtx * compression = ZSTD_createCCtx();
    ZSTD_DCtx * decompression = ZSTD_createDCtx();

    ~ZstdThreadContexts()
    {
        ZSTD_freeCCtx(compression);
        ZSTD_freeDCtx(decompression);
    }
};

inline ZstdThreadContexts & zstd_thread_contexts()
{
    static thread_local ZstdThreadContexts contexts{};
    return contexts;
}

}  // namespace detail

// Self-describing header prepended to every PDZ block. It stores the sample
// count n and the compressed sizes |Z_l| and |Z_h| so the block can be decoded
// without any external state.
class PdzBlockHeader
{
public:
    PdzBlockHeader(
        const uint64_t sample_count,
        const uint64_t z_l_compressed_size,
        const uint64_t z_h_compressed_size)
        : m_n(sample_count)
        , m_z_l_size(z_l_compressed_size)
        , m_z_h_size(z_h_compressed_size)
    {
    }

    void encode(uint8_t * ptr) const noexcept
    {
        encode_uint64(m_n, ptr);
        encode_uint64(m_z_l_size, ptr + sizeof(m_n));
        encode_uint64(m_z_h_size, ptr + sizeof(m_n) + sizeof(m_z_l_size));
    }

    static PdzBlockHeader decode(const uint8_t * ptr) noexcept
    {
        const auto n = decode_uint64(ptr);
        const auto z_l_size = decode_uint64(ptr + sizeof(uint64_t));
        const auto z_h_size = decode_uint64(ptr + 2 * sizeof(uint64_t));
        return PdzBlockHeader(n, z_l_size, z_h_size);
    }

    static constexpr size_t size() noexcept
    {
        return sizeof(m_n) + sizeof(m_z_l_size) + sizeof(m_z_h_size);
    }

    uint64_t sample_count() const noexcept { return m_n; }

    uint64_t z_l_compressed_size() const noexcept { return m_z_l_size; }

    uint64_t z_h_compressed_size() const noexcept { return m_z_h_size; }

private:
    uint64_t m_n;
    uint64_t m_z_l_size;
    uint64_t m_z_h_size;
};

// The PDZ codec: differential coding (d), then the piecewise split of the
// Rice-mapped values into B_u / B_l / B_h, then ZSTD on B_l and B_h. It is
// fully stateless: encode maps int16 samples (x) -> byte blob, decode maps the
// blob back, reading n from the block header.
class PdzCodec
{
public:
    // Returned by encode when ZSTD reports an error.
    static constexpr uint64_t encode_error = std::numeric_limits<uint64_t>::max();

    // Compresses `sample_count` samples (x) from `samples` into `output`, which
    // must have room for at least encode_bound(sample_count) bytes. Returns the
    // number of bytes written, or `encode_error` on failure.
    static uint64_t encode(
        const int16_t * pdz_restrict samples,
        const size_t sample_count,
        uint8_t * pdz_restrict output,
        SimdBackend backend = SimdBackend::Native);

    // Decompresses a block written by encode back into `output`
    // (`sample_count` int16 samples, taken from the block header). Returns true on
    // success, or false if either ZSTD sub-block fails to decompress to its
    // expected size (e.g. corrupt input); on failure `output` is left unwritten.
    static bool decode(
        const uint8_t * pdz_restrict bytes,
        int16_t * pdz_restrict output,
        SimdBackend backend = SimdBackend::Native);

    static size_t encode_bound(size_t sample_count);

    static PdzBlockHeader get_header(uint8_t * ptr) { return PdzBlockHeader::decode(ptr); }

private:
    static void cleanup(uint8_t * const scratch_storage)
    {
        padded_aligned_free(scratch_storage);
    }
};

inline uint64_t PdzCodec::encode(
    const int16_t * pdz_restrict samples,
    const size_t sample_count,
    uint8_t * pdz_restrict output,
    SimdBackend backend)
{
    const size_t n = sample_count;
    uint8_t * const buffer_u_ptr = output + PdzBlockHeader::size();
    auto const scratch = detail::allocate_split_scratch(n);
    uint8_t * const buffer_l_ptr = scratch.buffer_l;
    uint8_t * const buffer_h_ptr = scratch.buffer_h;
    if (scratch.storage == nullptr || buffer_l_ptr == nullptr || buffer_h_ptr == nullptr) {
        return encode_error;
    }

#if defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64)
    if (backend == SimdBackend::Sse || backend == SimdBackend::Native) {
        PiecewiseSplitSse::encode(samples, n, buffer_u_ptr, buffer_l_ptr, buffer_h_ptr);
    } else
#elif defined(__ARM_NEON) || defined(__aarch64__)
    if (backend == SimdBackend::Neon || backend == SimdBackend::Native) {
        PiecewiseSplitNeon::encode(samples, n, buffer_u_ptr, buffer_l_ptr, buffer_h_ptr);
    } else
#else
    static_cast<void>(backend);
#endif
    {
        PiecewiseSplitScalar::encode(samples, n, buffer_u_ptr, buffer_l_ptr, buffer_h_ptr);
    }

    const auto bound_bytes = encode_bound(n);
    const auto buffer_u_size = BufferSizes::buffer_u_size_bytes(n);
    auto * const compression_context = detail::zstd_thread_contexts().compression;

    const auto z_l_result = compression_context == nullptr
        ? ZSTD_compress(
              output + PdzBlockHeader::size() + buffer_u_size,
              bound_bytes - buffer_u_size,
              buffer_l_ptr,
              BufferSizes::buffer_l_size_bytes(n),
              1)
        : ZSTD_compressCCtx(
              compression_context,
              output + PdzBlockHeader::size() + buffer_u_size,
              bound_bytes - buffer_u_size,
              buffer_l_ptr,
              BufferSizes::buffer_l_size_bytes(n),
              1);
    if (ZSTD_isError(z_l_result)) {
        cleanup(scratch.storage);
        return encode_error;
    }

    const auto z_h_result = compression_context == nullptr
        ? ZSTD_compress(
              output + PdzBlockHeader::size() + buffer_u_size + z_l_result,
              bound_bytes - buffer_u_size - z_l_result,
              buffer_h_ptr,
              BufferSizes::buffer_h_size_bytes(n),
              1)
        : ZSTD_compressCCtx(
              compression_context,
              output + PdzBlockHeader::size() + buffer_u_size + z_l_result,
              bound_bytes - buffer_u_size - z_l_result,
              buffer_h_ptr,
              BufferSizes::buffer_h_size_bytes(n),
              1);
    if (ZSTD_isError(z_h_result)) {
        cleanup(scratch.storage);
        return encode_error;
    }

    PdzBlockHeader hdr(n, z_l_result, z_h_result);
    hdr.encode(output);

    cleanup(scratch.storage);
    return z_h_result + z_l_result + buffer_u_size + PdzBlockHeader::size();
}

inline bool PdzCodec::decode(
    const uint8_t * pdz_restrict bytes,
    int16_t * pdz_restrict output,
    SimdBackend backend)
{
    const auto hdr = PdzBlockHeader::decode(bytes);
    const auto n = hdr.sample_count();
    const auto buffer_u_size = BufferSizes::buffer_u_size_bytes(n);

    const uint8_t * pdz_restrict buffer_u_ptr = bytes + PdzBlockHeader::size();
    auto const scratch = detail::allocate_split_scratch(n);
    uint8_t * pdz_restrict buffer_l_ptr = scratch.buffer_l;
    uint8_t * pdz_restrict buffer_h_ptr = scratch.buffer_h;
    if (scratch.storage == nullptr || buffer_l_ptr == nullptr || buffer_h_ptr == nullptr) {
        return false;
    }

    auto * const decompression_context = detail::zstd_thread_contexts().decompression;
    const auto z_l_decompressed = decompression_context == nullptr
        ? ZSTD_decompress(
              buffer_l_ptr,
              BufferSizes::buffer_l_size_bytes(n),
              bytes + PdzBlockHeader::size() + buffer_u_size,
              hdr.z_l_compressed_size())
        : ZSTD_decompressDCtx(
              decompression_context,
              buffer_l_ptr,
              BufferSizes::buffer_l_size_bytes(n),
              bytes + PdzBlockHeader::size() + buffer_u_size,
              hdr.z_l_compressed_size());
    if (ZSTD_isError(z_l_decompressed)
        || z_l_decompressed != BufferSizes::buffer_l_size_bytes(n)) {
        cleanup(scratch.storage);
        return false;
    }

    const auto z_h_decompressed = decompression_context == nullptr
        ? ZSTD_decompress(
              buffer_h_ptr,
              BufferSizes::buffer_h_size_bytes(n),
              bytes + PdzBlockHeader::size() + buffer_u_size + hdr.z_l_compressed_size(),
              hdr.z_h_compressed_size())
        : ZSTD_decompressDCtx(
              decompression_context,
              buffer_h_ptr,
              BufferSizes::buffer_h_size_bytes(n),
              bytes + PdzBlockHeader::size() + buffer_u_size + hdr.z_l_compressed_size(),
              hdr.z_h_compressed_size());
    if (ZSTD_isError(z_h_decompressed)
        || z_h_decompressed != BufferSizes::buffer_h_size_bytes(n)) {
        cleanup(scratch.storage);
        return false;
    }

#if defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64)
    if (backend == SimdBackend::Sse || backend == SimdBackend::Native) {
        PiecewiseSplitSse::decode(
            buffer_u_ptr,
            buffer_l_ptr,
            buffer_h_ptr,
            n,
            reinterpret_cast<uint16_t *>(output));
    } else
#elif defined(__ARM_NEON) || defined(__aarch64__)
    if (backend == SimdBackend::Neon || backend == SimdBackend::Native) {
        PiecewiseSplitNeon::decode(
            buffer_u_ptr,
            buffer_l_ptr,
            buffer_h_ptr,
            n,
            reinterpret_cast<uint16_t *>(output));
    } else
#else
    static_cast<void>(backend);
#endif
    {
        PiecewiseSplitScalar::decode(
            buffer_u_ptr,
            buffer_l_ptr,
            buffer_h_ptr,
            n,
            reinterpret_cast<uint16_t *>(output));
    }

    // Invert the differential coding: x_i = d_i + x_{i-1}.
    uint_fast16_t previous_sample = 0;
    auto const * const differences = reinterpret_cast<const uint16_t *>(output);
    for (size_t i = 0; i < n; i++) {
        const auto current_difference = differences[i];
        const auto decoded_sample = static_cast<uint_fast16_t>(current_difference + previous_sample);
        output[i] = static_cast<int16_t>(decoded_sample);
        previous_sample = decoded_sample;
    }

    cleanup(scratch.storage);
    return true;
}

inline size_t PdzCodec::encode_bound(size_t sample_count)
{
    const auto n = sample_count;
    const auto buffer_u_bytes = BufferSizes::buffer_u_size_bytes(n);
    const auto z_l_bound = ZSTD_compressBound(BufferSizes::buffer_l_size_bytes(n));
    const auto z_h_bound = ZSTD_compressBound(BufferSizes::buffer_h_size_bytes(n));
    return PdzBlockHeader::size() + buffer_u_bytes + z_l_bound + z_h_bound;
}

}}  // namespace pod5::pdz
