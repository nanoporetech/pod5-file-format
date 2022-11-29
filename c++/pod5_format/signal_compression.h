#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"

#include <gsl/gsl-lite.hpp>

namespace arrow {
class MemoryPool;
class Buffer;
}  // namespace arrow

namespace pod5 {

using SampleType = std::int16_t;

POD5_FORMAT_EXPORT std::size_t compressed_signal_max_size(std::size_t sample_count);

POD5_FORMAT_EXPORT arrow::Result<std::size_t> compress_signal(
    gsl::span<SampleType const> const & samples,
    arrow::MemoryPool * pool,
    gsl::span<std::uint8_t> const & destination);

POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<arrow::Buffer>> compress_signal(
    gsl::span<SampleType const> const & samples,
    arrow::MemoryPool * pool);

POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<arrow::Buffer>> decompress_signal(
    gsl::span<std::uint8_t const> const & compressed_bytes,
    std::uint32_t samples_count,
    arrow::MemoryPool * pool);

POD5_FORMAT_EXPORT arrow::Status decompress_signal(
    gsl::span<std::uint8_t const> const & compressed_bytes,
    arrow::MemoryPool * pool,
    gsl::span<std::int16_t> const & destination);

}  // namespace pod5
