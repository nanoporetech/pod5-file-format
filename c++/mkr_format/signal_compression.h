#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/result.h"

#include <gsl/gsl-lite.hpp>

namespace arrow {
class MemoryPool;
class Buffer;
}  // namespace arrow

namespace mkr {

using SampleType = std::int16_t;

MKR_FORMAT_EXPORT std::size_t compressed_signal_max_size(std::size_t sample_count);

MKR_FORMAT_EXPORT arrow::Result<std::shared_ptr<arrow::Buffer>> compress_signal(
        gsl::span<SampleType const> const& samples,
        arrow::MemoryPool* pool);

MKR_FORMAT_EXPORT arrow::Result<std::shared_ptr<arrow::Buffer>> decompress_signal(
        gsl::span<std::uint8_t const> const& compressed_bytes,
        std::uint32_t samples_count,
        arrow::MemoryPool* pool);

MKR_FORMAT_EXPORT arrow::Status decompress_signal(
        gsl::span<std::uint8_t const> const& compressed_bytes,
        arrow::MemoryPool* pool,
        gsl::span<std::int16_t> const& destination);

}  // namespace mkr