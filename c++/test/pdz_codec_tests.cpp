#include "pod5_format/pdz/pdz.hpp"

#include <catch2/catch.hpp>

#include <cstdint>
#include <limits>
#include <random>
#include <vector>

using pod5::pdz::PdzCodec;
using pod5::pdz::SimdBackend;

namespace {

std::vector<std::uint8_t> encode(std::vector<std::int16_t> const & samples, SimdBackend backend)
{
    std::vector<std::uint8_t> out(PdzCodec::encode_bound(samples.size()));
    auto const written = PdzCodec::encode(samples.data(), samples.size(), out.data(), backend);
    REQUIRE(written != PdzCodec::encode_error);
    // encode_bound must be a true upper bound on the encoded size.
    CHECK(written <= out.size());
    out.resize(written);
    return out;
}

std::vector<std::int16_t> decode(
    std::vector<std::uint8_t> const & encoded,
    std::size_t sample_count,
    SimdBackend backend)
{
    std::vector<std::int16_t> out(sample_count, 0);
    PdzCodec::decode(encoded.data(), out.data(), backend);
    return out;
}

void check_round_trip(std::vector<std::int16_t> const & samples)
{
    auto const encoded = encode(samples, SimdBackend::Native);
    auto const decoded = decode(encoded, samples.size(), SimdBackend::Native);
    CHECK(decoded == samples);

    // The block header records the sample count, so decode is fully
    // self-describing from the encoded bytes alone.
    if (!samples.empty()) {
        auto const header = PdzCodec::get_header(const_cast<std::uint8_t *>(encoded.data()));
        CHECK(header.sample_count() == samples.size());
    }
}

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
// (pore "events") plus Gaussian measurement noise.
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

}  // namespace

TEST_CASE("PDZ encode_bound covers the actual encoded size", "[pdz]")
{
    for (std::size_t n : {std::size_t{0}, std::size_t{1}, std::size_t{1000}, std::size_t{50000}}) {
        auto const samples = random_noise(n, 1234);
        std::vector<std::uint8_t> out(PdzCodec::encode_bound(n));
        auto const written = PdzCodec::encode(samples.data(), n, out.data(), SimdBackend::Native);
        REQUIRE(written != PdzCodec::encode_error);
        CHECK(written <= PdzCodec::encode_bound(n));
    }
}

TEST_CASE("PDZ round-trips representative signals bit-exactly", "[pdz]")
{
    SECTION("empty") { check_round_trip({}); }
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
}

TEST_CASE("PDZ round-trips across the vectorised-block boundary", "[pdz]")
{
    // The codec processes whole 128-sample blocks with SIMD and the remainder
    // with a scalar tail; exercise sizes either side of that boundary.
    for (std::size_t n : {std::size_t{127},
                          std::size_t{128},
                          std::size_t{129},
                          std::size_t{255},
                          std::size_t{256},
                          std::size_t{257}})
    {
        auto const samples = random_noise(n, static_cast<unsigned>(n));
        check_round_trip(samples);
    }
}
