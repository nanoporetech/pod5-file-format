#include "pod5_format/signal_compression.h"

#include "pod5_format/svb16/decode.hpp"
#include "pod5_format/svb16/encode.hpp"

#include <arrow/buffer.h>
#include <arrow/util/io_util.h>
#include <zstd.h>

#include <cassert>
#include <limits>

namespace pod5 {

namespace {

// SVB is designed around 32 bit sizes, so that's the maximum uncompressed samples allowed.
constexpr std::size_t max_uncompressed_samples = std::numeric_limits<std::uint32_t>::max();

}  // namespace

arrow::Result<std::size_t> compressed_signal_max_size(std::size_t sample_count)
{
    if (sample_count > max_uncompressed_samples) {
        return arrow::Status::Invalid(
            sample_count, " samples exceeds max of ", max_uncompressed_samples);
    }

    auto const max_svb_size = svb16_max_encoded_length(sample_count);
    auto const zstd_compressed_max_size = ZSTD_compressBound(max_svb_size);
    if (ZSTD_isError(zstd_compressed_max_size)) {
        return pod5::Status::Invalid(
            sample_count,
            " samples exceeds zstd limit: (",
            zstd_compressed_max_size,
            " ",
            ZSTD_getErrorName(zstd_compressed_max_size),
            ")");
    }

    return zstd_compressed_max_size;
}

arrow::Result<std::size_t> compress_signal(
    gsl::span<SampleType const> samples,
    arrow::MemoryPool * pool,
    gsl::span<std::uint8_t> destination)
{
    std::size_t const sample_count = samples.size();
    if (sample_count > max_uncompressed_samples) {
        return arrow::Status::Invalid(
            sample_count, " samples exceeds max of ", max_uncompressed_samples);
    }

    // First compress the data using svb:
    auto const max_size = svb16_max_encoded_length(sample_count);
    ARROW_ASSIGN_OR_RAISE(auto intermediate, arrow::AllocateResizableBuffer(max_size, pool));

    static constexpr bool UseDelta = true;
    static constexpr bool UseZigzag = true;
    auto const encoded_count = svb16::encode<SampleType, UseDelta, UseZigzag>(
        samples.data(), intermediate->mutable_data(), sample_count);
    ARROW_RETURN_NOT_OK(intermediate->Resize(encoded_count));

    // Now compress the svb data using zstd:
    size_t const zstd_compressed_max_size = ZSTD_compressBound(intermediate->size());
    if (ZSTD_isError(zstd_compressed_max_size)) {
        return pod5::Status::Invalid(
            "Failed to find zstd max size for data: (",
            zstd_compressed_max_size,
            " ",
            ZSTD_getErrorName(zstd_compressed_max_size),
            ")");
    }

    /* Compress.
     * If you are doing many compressions, you may want to reuse the context.
     * See the multiple_simple_compression.c example.
     */
    size_t const compressed_size = ZSTD_compress(
        destination.data(), destination.size(), intermediate->data(), intermediate->size(), 1);
    if (ZSTD_isError(compressed_size)) {
        return pod5::Status::Invalid(
            "Failed to compress data: (",
            compressed_size,
            " ",
            ZSTD_getErrorName(compressed_size),
            ")");
    }
    return compressed_size;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> compress_signal(
    gsl::span<SampleType const> samples,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(
        std::size_t const sample_count, compressed_signal_max_size(samples.size()));

    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::ResizableBuffer> out,
        arrow::AllocateResizableBuffer(sample_count, pool));

    ARROW_ASSIGN_OR_RAISE(
        auto final_size,
        compress_signal(samples, pool, gsl::make_span(out->mutable_data(), out->size())));

    ARROW_RETURN_NOT_OK(out->Resize(final_size));
    return out;
}

arrow::Status decompress_signal(
    gsl::span<std::uint8_t const> compressed_bytes,
    arrow::MemoryPool * pool,
    gsl::span<std::int16_t> destination)
{
    // Check that we could have compressed this size.
    ARROW_ASSIGN_OR_RAISE(
        std::size_t const max_compressed_size, compressed_signal_max_size(destination.size()));
    if (compressed_bytes.size() > max_compressed_size) {
        return pod5::Status::Invalid(
            "Input data corrupt: compressed input size (",
            compressed_bytes.size(),
            ") exceeds max compressed output size (",
            max_compressed_size,
            ")");
    }

    // Find out how big zstd thinks the data is.
    unsigned long long const decompressed_zstd_size =
        ZSTD_getFrameContentSize(compressed_bytes.data(), compressed_bytes.size());
    if (ZSTD_isError(decompressed_zstd_size)) {
        return pod5::Status::Invalid(
            "Input data not compressed by zstd: (",
            decompressed_zstd_size,
            " ",
            ZSTD_getErrorName(decompressed_zstd_size),
            ")");
    }

    // Documentation of |ZSTD_getFrameContentSize| explicitly states that we should bounds check this:
    //     *   note 5 : If source is untrusted, decompressed size could be wrong or intentionally modified.
    //     *            Always ensure return value fits within application's authorized limits.
    //     *            Each application can set its own limits.
    std::size_t const max_svb16_compressed_size = svb16_max_encoded_length(destination.size());
    if (decompressed_zstd_size > max_svb16_compressed_size) {
        return arrow::Status::Invalid(
            "Input data corrupt: claimed size (",
            decompressed_zstd_size,
            ") exceeds max compressed output size (",
            max_svb16_compressed_size,
            ")");
    }

    // Check that we have enough memory to decompress.
    // Note: this will return 0 on unsupported platforms, so we skip it there.
    std::int64_t const system_memory = arrow::internal::GetTotalMemoryBytes();
    assert(system_memory > 0);
    if (system_memory > 0 && decompressed_zstd_size >= static_cast<std::size_t>(system_memory)) {
        return arrow::Status::OutOfMemory(
            "Not enough system memory (",
            system_memory,
            ") to decompress file (",
            decompressed_zstd_size,
            ")");
    }

    if (POD5_ENABLE_FUZZERS && decompressed_zstd_size > 1'000'000) {
        return arrow::Status::Invalid("Skipping huge sizes when fuzzing");
    }

    // Decompress the data using zstd.
    auto const allocation_padding = svb16::decode_input_buffer_padding_byte_count();
    ARROW_ASSIGN_OR_RAISE(
        auto intermediate,
        arrow::AllocateResizableBuffer(decompressed_zstd_size + allocation_padding, pool));
    size_t const decompress_res = ZSTD_decompress(
        intermediate->mutable_data(),
        intermediate->size(),
        compressed_bytes.data(),
        compressed_bytes.size());
    if (ZSTD_isError(decompress_res)) {
        return pod5::Status::Invalid(
            "Input data failed to decompress using zstd: (",
            decompress_res,
            " ",
            ZSTD_getErrorName(decompress_res),
            ")");
    }

    auto const svb16_compressed_data_with_padding =
        gsl::make_span(intermediate->data(), intermediate->size());
    auto const svb16_compressed_data_no_padding =
        svb16_compressed_data_with_padding.subspan(0, decompressed_zstd_size);

    // Validate the data.
    if (!svb16::validate(svb16_compressed_data_no_padding, destination.size())) {
        return pod5::Status::Invalid("Compressed signal data is corrupt");
    }

    // Now decompress the data using svb:
    static constexpr bool UseDelta = true;
    static constexpr bool UseZigzag = true;
    auto consumed_count = svb16::decode<SampleType, UseDelta, UseZigzag>(
        destination, svb16_compressed_data_with_padding);
    if (consumed_count != decompressed_zstd_size) {
        return pod5::Status::Invalid("Remaining data at end of signal buffer");
    }

    return pod5::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> decompress_signal(
    gsl::span<std::uint8_t const> compressed_bytes,
    std::uint32_t samples_count,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::ResizableBuffer> out,
        arrow::AllocateResizableBuffer(samples_count * sizeof(SampleType), pool));

    auto signal_span = gsl::make_span(out->mutable_data(), out->size()).as_span<std::int16_t>();

    ARROW_RETURN_NOT_OK(decompress_signal(compressed_bytes, pool, signal_span));
    return out;
}
}  // namespace pod5
