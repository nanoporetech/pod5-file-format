#include "pod5_format/pdz_compression.h"

#include "pod5_format/pdz/pdz.hpp"  // PdzBlockHeader::size() for the corrupt-payload case
#include "test_utils.h"

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <catch2/catch.hpp>
#include <gsl/gsl-lite.hpp>

#include <cstdint>
#include <limits>
#include <random>
#include <vector>

namespace {

std::vector<std::int16_t> ramp(std::size_t n)
{
    std::vector<std::int16_t> v(n);
    for (std::size_t i = 0; i < n; ++i) {
        v[i] = static_cast<std::int16_t>(i * 7 - 1000);
    }
    return v;
}

std::vector<std::int16_t> random_noise(std::size_t n, unsigned seed)
{
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> dist(
        std::numeric_limits<std::int16_t>::min(), std::numeric_limits<std::int16_t>::max());
    std::vector<std::int16_t> v(n);
    for (auto & x : v) {
        x = static_cast<std::int16_t>(dist(rng));
    }
    return v;
}

// Deterministic pseudo-realistic nanopore read: piecewise-constant baseline
// ("events") plus Gaussian measurement noise.
std::vector<std::int16_t> nanopore_like(std::size_t n, unsigned seed)
{
    std::mt19937 rng(seed);
    std::normal_distribution<double> noise(0.0, 12.0);
    std::vector<std::int16_t> v(n);
    double baseline = 500.0;
    for (std::size_t i = 0; i < n; ++i) {
        if (i % 250 == 0) {
            baseline = 80.0 + (rng() % 900);
        }
        v[i] = static_cast<std::int16_t>(baseline + noise(rng));
    }
    return v;
}

std::shared_ptr<arrow::Buffer> compress(
    gsl::span<std::int16_t const> samples,
    arrow::MemoryPool * pool)
{
    auto compressed = pod5::compress_signal_pdz(samples, pool);
    REQUIRE_ARROW_STATUS_OK(compressed);
    return *compressed;
}

void check_round_trip(std::vector<std::int16_t> const & samples)
{
    auto pool = arrow::system_memory_pool();

    auto const max_size = pod5::pdz_compressed_signal_max_size(samples.size());
    REQUIRE_ARROW_STATUS_OK(max_size);

    // Allocating overload.
    auto const compressed = compress(gsl::make_span(samples), pool);
    CHECK(static_cast<std::size_t>(compressed->size()) <= *max_size);

    auto const compressed_span = gsl::make_span(compressed->data(), compressed->size());
    std::vector<std::int16_t> decoded(samples.size(), 0);
    REQUIRE_ARROW_STATUS_OK(
        pod5::decompress_signal_pdz(compressed_span, pool, gsl::make_span(decoded)));
    CHECK(decoded == samples);

    // In-place overload writes the same bytes into a caller-sized buffer.
    std::vector<std::uint8_t> destination(*max_size);
    auto const written =
        pod5::compress_signal_pdz(gsl::make_span(samples), pool, gsl::make_span(destination));
    REQUIRE_ARROW_STATUS_OK(written);
    CHECK(*written <= destination.size());
    CHECK(*written == static_cast<std::size_t>(compressed->size()));

    std::vector<std::int16_t> decoded_in_place(samples.size(), 0);
    REQUIRE_ARROW_STATUS_OK(pod5::decompress_signal_pdz(
        gsl::make_span(destination.data(), *written), pool, gsl::make_span(decoded_in_place)));
    CHECK(decoded_in_place == samples);
}

}  // namespace

TEST_CASE("PDZ adapter round-trips representative signals", "[pdz][adapter]")
{
    SECTION("empty") { check_round_trip(std::vector<std::int16_t>{}); }
    SECTION("length 1") { check_round_trip({1234}); }
    SECTION("length 2") { check_round_trip({-5, 5}); }
    SECTION("max amplitude")
    {
        constexpr auto lo = std::numeric_limits<std::int16_t>::min();
        constexpr auto hi = std::numeric_limits<std::int16_t>::max();
        check_round_trip({lo, hi, lo, hi, 0, hi, lo, 0, hi});
    }
    SECTION("monotonic ramp") { check_round_trip(ramp(5000)); }
    SECTION("random noise") { check_round_trip(random_noise(20000, 42)); }
    SECTION("nanopore-like read") { check_round_trip(nanopore_like(123457, 7)); }
    SECTION("just over the vectorised-block boundary") { check_round_trip(random_noise(129, 9)); }
}

TEST_CASE("PDZ adapter in-place compress rejects an undersized destination", "[pdz][adapter]")
{
    auto pool = arrow::system_memory_pool();
    auto const samples = nanopore_like(2048, 11);

    auto const max_size = pod5::pdz_compressed_signal_max_size(samples.size());
    REQUIRE_ARROW_STATUS_OK(max_size);

    std::vector<std::uint8_t> too_small(*max_size / 2);
    CHECK_ARROW_STATUS_NOT_OK(
        pod5::compress_signal_pdz(gsl::make_span(samples), pool, gsl::make_span(too_small)));
}

// All cases below must not crash or write out of bounds (run under ASan); a
// well-formed Status::Invalid is the expected, observable result.
TEST_CASE("PDZ adapter rejects corrupt input without crashing", "[pdz][adapter]")
{
    auto pool = arrow::system_memory_pool();
    auto const samples = nanopore_like(4096, 3);
    auto const compressed = compress(gsl::make_span(samples), pool);
    auto const good = gsl::make_span(compressed->data(), compressed->size());

    SECTION("truncated buffer")
    {
        for (std::size_t kept : {std::size_t{0},
                                 std::size_t{5},
                                 std::size_t{23},
                                 static_cast<std::size_t>(compressed->size()) / 2,
                                 static_cast<std::size_t>(compressed->size()) - 1})
        {
            std::vector<std::int16_t> out(samples.size(), 0);
            CHECK_ARROW_STATUS_NOT_OK(
                pod5::decompress_signal_pdz(good.subspan(0, kept), pool, gsl::make_span(out)));
        }
    }

    SECTION("header sample count does not match destination size")
    {
        std::vector<std::int16_t> out(samples.size() + 1, 0);
        CHECK_ARROW_STATUS_NOT_OK(
            pod5::decompress_signal_pdz(good, pool, gsl::make_span(out)));
    }

    SECTION("input larger than the encode bound")
    {
        auto const max_size = pod5::pdz_compressed_signal_max_size(samples.size());
        REQUIRE_ARROW_STATUS_OK(max_size);
        std::vector<std::uint8_t> oversized(*max_size + 1024, 0);
        std::vector<std::int16_t> out(samples.size(), 0);
        CHECK_ARROW_STATUS_NOT_OK(
            pod5::decompress_signal_pdz(gsl::make_span(oversized), pool, gsl::make_span(out)));
    }

    SECTION("random garbage of a plausible size")
    {
        std::mt19937 rng(99);
        for (int trial = 0; trial < 64; ++trial) {
            std::vector<std::uint8_t> garbage(static_cast<std::size_t>(compressed->size()));
            for (auto & b : garbage) {
                b = static_cast<std::uint8_t>(rng());
            }
            std::vector<std::int16_t> out(samples.size(), 0);
            CHECK_ARROW_STATUS_NOT_OK(
                pod5::decompress_signal_pdz(gsl::make_span(garbage), pool, gsl::make_span(out)));
        }
    }

    SECTION("valid header but corrupt ZSTD payload")
    {
        std::vector<std::uint8_t> corrupt(
            good.data(), good.data() + static_cast<std::size_t>(compressed->size()));
        // Keep the self-describing header intact (so the layout checks pass) but
        // clobber the B_u / Z_l / Z_h payload so the ZSTD sub-blocks are garbage.
        for (std::size_t i = pod5::pdz::PdzBlockHeader::size(); i < corrupt.size(); ++i) {
            corrupt[i] = 0xFF;
        }
        std::vector<std::int16_t> out(samples.size(), 0);
        CHECK_ARROW_STATUS_NOT_OK(
            pod5::decompress_signal_pdz(gsl::make_span(corrupt), pool, gsl::make_span(out)));
    }
}
