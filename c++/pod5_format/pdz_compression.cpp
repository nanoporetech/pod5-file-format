#include "pod5_format/pdz_compression.h"

#include "pod5_format/pdz/pdz.hpp"  // PdzCodec / PdzBlockHeader

#include <arrow/buffer.h>
#include <zstd.h>

#include <cstdint>
#include <limits>

namespace pod5 {

namespace {

// PDZ reads its sample count from a 64-bit header field but, like VBZ, POD5 only
// ever stores up to a 32-bit sample count per block.
constexpr std::size_t max_uncompressed_samples = std::numeric_limits<std::uint32_t>::max();

}  // namespace

arrow::Result<std::size_t> pdz_compressed_signal_max_size(std::size_t sample_count)
{
    if (sample_count > max_uncompressed_samples) {
        return arrow::Status::Invalid(
            sample_count, " samples exceeds max of ", max_uncompressed_samples);
    }

    return pod5::pdz::PdzCodec::encode_bound(sample_count);
}

arrow::Result<std::size_t> compress_signal_pdz(
    gsl::span<SampleType const> samples,
    arrow::MemoryPool * /*pool*/,
    gsl::span<std::uint8_t> destination)
{
    std::size_t const sample_count = samples.size();
    ARROW_ASSIGN_OR_RAISE(
        std::size_t const max_size, pdz_compressed_signal_max_size(sample_count));

    // The raw codec writes up to encode_bound() bytes and is never told how big
    // `destination` is, so guard against an undersized output buffer here.
    if (destination.size() < max_size) {
        return arrow::Status::Invalid(
            "PDZ destination buffer (",
            destination.size(),
            " bytes) is smaller than the required maximum (",
            max_size,
            " bytes)");
    }

    auto const written =
        pod5::pdz::PdzCodec::encode(samples.data(), sample_count, destination.data());
    if (written == pod5::pdz::PdzCodec::encode_error) {
        return arrow::Status::Invalid("Failed to compress signal data using PDZ (zstd error)");
    }

    return static_cast<std::size_t>(written);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> compress_signal_pdz(
    gsl::span<SampleType const> samples,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(
        std::size_t const max_size, pdz_compressed_signal_max_size(samples.size()));

    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::ResizableBuffer> out,
        arrow::AllocateResizableBuffer(max_size, pool));

    ARROW_ASSIGN_OR_RAISE(
        auto const final_size,
        compress_signal_pdz(samples, pool, gsl::make_span(out->mutable_data(), out->size())));

    ARROW_RETURN_NOT_OK(out->Resize(final_size));
    return out;
}

arrow::Status decompress_signal_pdz(
    gsl::span<std::uint8_t const> compressed_bytes,
    arrow::MemoryPool * /*pool*/,
    gsl::span<std::int16_t> destination)
{
    using pod5::pdz::BufferSizes;
    using pod5::pdz::PdzBlockHeader;
    using pod5::pdz::PdzCodec;

    std::size_t const sample_count = destination.size();

    // 1. A PDZ block for this many samples can never exceed the encode bound.
    ARROW_ASSIGN_OR_RAISE(
        std::size_t const max_compressed_size, pdz_compressed_signal_max_size(sample_count));
    if (compressed_bytes.size() > max_compressed_size) {
        return arrow::Status::Invalid(
            "Input data corrupt: compressed input size (",
            compressed_bytes.size(),
            ") exceeds max compressed output size (",
            max_compressed_size,
            ")");
    }

    // 2. The block must contain its self-describing header, and the recorded
    //    sample count must match the caller's destination (the codec trusts that
    //    count to size every write it makes).
    if (compressed_bytes.size() < PdzBlockHeader::size()) {
        return arrow::Status::Invalid(
            "Input data corrupt: ",
            compressed_bytes.size(),
            " bytes is too small to contain a PDZ block header");
    }
    auto const header = PdzBlockHeader::decode(compressed_bytes.data());
    if (header.sample_count() != sample_count) {
        return arrow::Status::Invalid(
            "Input data corrupt: PDZ header sample count (",
            header.sample_count(),
            ") does not match expected sample count (",
            sample_count,
            ")");
    }

    // 3. Validate the on-disk layout is internally consistent: each ZSTD
    //    sub-block is within its bound and  header + |B_u| + |Z_l| + |Z_h|
    //    accounts for the supplied buffer exactly (no truncation, no trailing
    //    data the codec would silently ignore).
    std::size_t const buffer_u_size = BufferSizes::buffer_u_size_bytes(sample_count);
    std::uint64_t const z_l_size = header.z_l_compressed_size();
    std::uint64_t const z_h_size = header.z_h_compressed_size();
    if (z_l_size > ZSTD_compressBound(BufferSizes::buffer_l_size_bytes(sample_count))
        || z_h_size > ZSTD_compressBound(BufferSizes::buffer_h_size_bytes(sample_count)))
    {
        return arrow::Status::Invalid(
            "Input data corrupt: a PDZ ZSTD sub-block size exceeds its bound");
    }
    std::size_t const expected_size = PdzBlockHeader::size() + buffer_u_size
                                    + static_cast<std::size_t>(z_l_size)
                                    + static_cast<std::size_t>(z_h_size);
    if (compressed_bytes.size() != expected_size) {
        return arrow::Status::Invalid(
            "Input data corrupt: PDZ block size (",
            compressed_bytes.size(),
            ") does not match the size implied by its header (",
            expected_size,
            ")");
    }

    // 4. When fuzzing, skip pathologically large blocks.
    if (POD5_ENABLE_FUZZERS && sample_count > 1'000'000) {
        return arrow::Status::Invalid("Skipping huge sizes when fuzzing");
    }

    // 5. The block is structurally sound: decode it. decode() returns false if a
    //    ZSTD sub-block fails to decompress to its expected size.
    if (!PdzCodec::decode(compressed_bytes.data(), destination.data())) {
        return arrow::Status::Invalid("Input data failed to decompress using PDZ (zstd error)");
    }

    return arrow::Status::OK();
}

}  // namespace pod5
