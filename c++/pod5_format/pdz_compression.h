#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"

#include <gsl/gsl-lite.hpp>

#include <cstdint>

namespace arrow {
class MemoryPool;
class Buffer;
}  // namespace arrow

namespace pod5 {

using SampleType = std::int16_t;  // matches signal_compression.h

// Upper bound on the number of bytes a PDZ block may occupy for the given
// number of int16 samples. Mirrors compressed_signal_max_size() for VBZ.
POD5_FORMAT_EXPORT arrow::Result<std::size_t> pdz_compressed_signal_max_size(
    std::size_t sample_count);

// Compresses `samples` into the caller-provided `destination`, which must be at
// least pdz_compressed_signal_max_size(samples.size()) bytes. Returns the number
// of bytes written.
POD5_FORMAT_EXPORT arrow::Result<std::size_t> compress_signal_pdz(
    gsl::span<SampleType const> samples,
    arrow::MemoryPool * pool,
    gsl::span<std::uint8_t> destination);

// Allocates an appropriately sized buffer from `pool`, compresses `samples` into
// it, and returns it sized to the compressed length.
POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<arrow::Buffer>> compress_signal_pdz(
    gsl::span<SampleType const> samples,
    arrow::MemoryPool * pool);

// Decompresses `compressed_bytes` (a single PDZ block) into `destination`, which
// must already be sized to the original sample count. Validates the block before
// decoding so that corrupt input is rejected rather than causing out-of-bounds
// access.
POD5_FORMAT_EXPORT arrow::Status decompress_signal_pdz(
    gsl::span<std::uint8_t const> compressed_bytes,
    arrow::MemoryPool * pool,
    gsl::span<std::int16_t> destination);

}  // namespace pod5
