#include "mkr_format/signal_compression.h"

#include "utils.h"

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <catch2/catch.hpp>
#include <gsl/gsl-lite.hpp>
#include <numeric>

SCENARIO("Signal compression Tests") {
    auto pool = arrow::system_memory_pool();

    std::vector<std::int16_t> signal(100'00);
    std::iota(signal.begin(), signal.end(), 0);

    auto compressed = mkr::compress_signal(gsl::make_span(signal), pool);
    REQUIRE(compressed.ok());
    auto compressed_span = gsl::make_span((*compressed)->data(), (*compressed)->size());

    auto decompressed = mkr::decompress_signal(compressed_span, signal.size(), pool);
    CAPTURE(decompressed);
    REQUIRE(decompressed.ok());
    auto decompressed_span = gsl::make_span((*decompressed)->data(), (*decompressed)->size())
                                     .as_span<std::int16_t const>();

    CHECK(gsl::make_span(signal) == decompressed_span);
}