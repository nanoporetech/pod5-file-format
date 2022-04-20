#include "mkr_format/signal_compression.h"

#include "mkr_format/svb16/decode.hpp"
#include "mkr_format/svb16/encode.hpp"

#include <arrow/buffer.h>
#include <zstd.h>

namespace mkr {

arrow::Result<std::shared_ptr<arrow::Buffer>> compress_signal(gsl::span<SampleType const> data,
                                                              arrow::MemoryPool* pool) {
    // First compress the data using svb:
    auto const max_size = svb16_max_encoded_length(data.size());
    ARROW_ASSIGN_OR_RAISE(auto intermediate, arrow::AllocateResizableBuffer(max_size, pool));

    static constexpr bool UseDelta = true;
    static constexpr bool UseZigzag = true;
    auto const encoded_count = svb16::encode<SampleType, UseDelta, UseZigzag>(
            data.data(), intermediate->mutable_data(), data.size());
    intermediate->Resize(encoded_count);

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
    out->Resize(compressed_size);
    return out;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> decompress_signal(gsl::span<std::uint8_t const> data,
                                                                std::uint32_t samples_count,
                                                                arrow::MemoryPool* pool) {
    // First decompress the data using zstd:
    unsigned long long const decompressed_zstd_size =
            ZSTD_getFrameContentSize(data.data(), data.size());
    if (ZSTD_isError(decompressed_zstd_size)) {
        return mkr::Status::Invalid("Input data not compressed by zstd");
    }

    ARROW_ASSIGN_OR_RAISE(auto intermediate,
                          arrow::AllocateResizableBuffer(decompressed_zstd_size, pool));
    size_t const decompress_res = ZSTD_decompress(intermediate->mutable_data(),
                                                  intermediate->size(), data.data(), data.size());
    if (ZSTD_isError(decompress_res)) {
        return mkr::Status::Invalid("Input data failed to compress using zstd");
    }

    ARROW_ASSIGN_OR_RAISE(auto out,
                          arrow::AllocateResizableBuffer(samples_count * sizeof(SampleType), pool));

    // Now decompress the data using svb:
    static constexpr bool UseDelta = true;
    static constexpr bool UseZigzag = true;
    auto consumed_count = svb16::decode<SampleType, UseDelta, UseZigzag>(
            reinterpret_cast<SampleType*>(out->mutable_data()), intermediate->data(),
            samples_count);

    if (consumed_count != intermediate->size()) {
        return mkr::Status::Invalid("Remaining data at end of signal buffer");
    }

    return out;
}
}  // namespace mkr