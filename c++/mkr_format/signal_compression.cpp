#include "mkr_format/signal_compression.h"

#include "mkr_format/svb16/decode.hpp"
#include "mkr_format/svb16/encode.hpp"

#include <arrow/buffer.h>
#include <zstd.h>

namespace mkr {

std::size_t compressed_signal_max_size(std::size_t sample_count) {
    auto const max_svb_size = svb16_max_encoded_length(sample_count);
    auto const zstd_compressed_max_size = ZSTD_compressBound(max_svb_size);
    return zstd_compressed_max_size;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> compress_signal(
        gsl::span<SampleType const> const& samples,
        arrow::MemoryPool* pool) {
    // First compress the data using svb:
    auto const max_size = svb16_max_encoded_length(samples.size());
    ARROW_ASSIGN_OR_RAISE(auto intermediate, arrow::AllocateResizableBuffer(max_size, pool));

    static constexpr bool UseDelta = true;
    static constexpr bool UseZigzag = true;
    auto const encoded_count = svb16::encode<SampleType, UseDelta, UseZigzag>(
            samples.data(), intermediate->mutable_data(), samples.size());
    ARROW_RETURN_NOT_OK(intermediate->Resize(encoded_count));

    // Now compress the svb data using zstd:
    size_t const zstd_compressed_max_size = ZSTD_compressBound(intermediate->size());
    if (ZSTD_isError(zstd_compressed_max_size)) {
        return mkr::Status::Invalid("Failed to find zstd max size for data");
    }
    ARROW_ASSIGN_OR_RAISE(auto out, arrow::AllocateResizableBuffer(zstd_compressed_max_size, pool));

    /* Compress.
     * If you are doing many compressions, you may want to reuse the context.
     * See the multiple_simple_compression.c example.
     */
    size_t const compressed_size = ZSTD_compress(out->mutable_data(), out->size(),
                                                 intermediate->data(), intermediate->size(), 1);
    if (ZSTD_isError(compressed_size)) {
        return mkr::Status::Invalid("Failed to compress data");
    }
    ARROW_RETURN_NOT_OK(out->Resize(compressed_size));
    return out;
}

MKR_FORMAT_EXPORT arrow::Status decompress_signal(
        gsl::span<std::uint8_t const> const& compressed_bytes,
        arrow::MemoryPool* pool,
        gsl::span<std::int16_t> const& destination) {
    // First decompress the data using zstd:
    unsigned long long const decompressed_zstd_size =
            ZSTD_getFrameContentSize(compressed_bytes.data(), compressed_bytes.size());
    if (ZSTD_isError(decompressed_zstd_size)) {
        return mkr::Status::Invalid("Input data not compressed by zstd");
    }

    ARROW_ASSIGN_OR_RAISE(auto intermediate,
                          arrow::AllocateResizableBuffer(decompressed_zstd_size, pool));
    size_t const decompress_res =
            ZSTD_decompress(intermediate->mutable_data(), intermediate->size(),
                            compressed_bytes.data(), compressed_bytes.size());
    if (ZSTD_isError(decompress_res)) {
        return mkr::Status::Invalid("Input data failed to compress using zstd");
    }

    // Now decompress the data using svb:
    static constexpr bool UseDelta = true;
    static constexpr bool UseZigzag = true;
    auto consumed_count = svb16::decode<SampleType, UseDelta, UseZigzag>(
            reinterpret_cast<SampleType*>(destination.data()), intermediate->data(),
            destination.size());

    if (consumed_count != intermediate->size()) {
        return mkr::Status::Invalid("Remaining data at end of signal buffer");
    }

    return mkr::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> decompress_signal(
        gsl::span<std::uint8_t const> const& compressed_bytes,
        std::uint32_t samples_count,
        arrow::MemoryPool* pool) {
    ARROW_ASSIGN_OR_RAISE(auto out,
                          arrow::AllocateResizableBuffer(samples_count * sizeof(SampleType), pool));

    auto signal_span = gsl::make_span(out->mutable_data(), out->size()).as_span<std::int16_t>();

    ARROW_RETURN_NOT_OK(decompress_signal(compressed_bytes, pool, signal_span));
    return out;
}
}  // namespace mkr