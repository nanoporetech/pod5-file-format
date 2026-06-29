// POD5 files must be portable, so bytes written by any SIMD backend must decode
// bit-exactly under every other backend. The scalar path is the golden reference
// oracle.
//
// On a given build only the backends compiled for that architecture can run
// (SSE on x86, NEON on ARM); scalar is always available. The CI matrix covers
// x86_64, aarch64 and forced-scalar so every pair is exercised across runners.
#include "pod5_format/pdz/pdz.hpp"

#include <catch2/catch.hpp>

#include <cstdint>
#include <limits>
#include <random>
#include <vector>

using pod5::pdz::PdzCodec;
using pod5::pdz::SimdBackend;

namespace {

std::vector<SimdBackend> runnable_backends()
{
    // Scalar is the oracle; Native is whatever this arch picks by default.
    std::vector<SimdBackend> backends{SimdBackend::Scalar, SimdBackend::Native};
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
    backends.push_back(SimdBackend::Sse);
#elif defined(__ARM_NEON) || defined(__aarch64__)
    backends.push_back(SimdBackend::Neon);
#endif
    return backends;
}

std::vector<std::uint8_t> encode(std::vector<std::int16_t> const & samples, SimdBackend backend)
{
    std::vector<std::uint8_t> out(PdzCodec::encode_bound(samples.size()));
    auto const written = PdzCodec::encode(samples.data(), samples.size(), out.data(), backend);
    REQUIRE(written != PdzCodec::encode_error);
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

std::vector<std::int16_t> make_signal(std::size_t n, unsigned seed)
{
    std::mt19937 rng(seed);
    std::normal_distribution<double> noise(0.0, 20.0);
    std::vector<std::int16_t> v(n);
    double baseline = 0.0;
    for (std::size_t i = 0; i < n; ++i) {
        if (i % 300 == 0) {
            baseline = static_cast<double>(static_cast<int>(rng() % 60000)
                                           - std::numeric_limits<std::int16_t>::max());
        }
        v[i] = static_cast<std::int16_t>(baseline + noise(rng));
    }
    return v;
}

}  // namespace

TEST_CASE("PDZ encoded bytes are identical across backends", "[pdz][cross-arch]")
{
    for (std::size_t n : {std::size_t{0},
                          std::size_t{1},
                          std::size_t{129},
                          std::size_t{4096},
                          std::size_t{65537}})
    {
        auto const samples = make_signal(n, static_cast<unsigned>(n + 1));
        auto const reference = encode(samples, SimdBackend::Scalar);
        for (auto backend : runnable_backends()) {
            INFO("n=" << n << " backend=" << static_cast<int>(backend));
            CHECK(encode(samples, backend) == reference);
        }
    }
}

TEST_CASE("PDZ: any backend decodes any other backend's output", "[pdz][cross-arch]")
{
    auto const backends = runnable_backends();
    for (std::size_t n : {std::size_t{2},
                          std::size_t{128},
                          std::size_t{257},
                          std::size_t{10000},
                          std::size_t{65537}})
    {
        auto const samples = make_signal(n, static_cast<unsigned>(n * 3 + 5));
        for (auto encode_backend : backends) {
            auto const encoded = encode(samples, encode_backend);
            for (auto decode_backend : backends) {
                INFO(
                    "n=" << n << " encode=" << static_cast<int>(encode_backend)
                         << " decode=" << static_cast<int>(decode_backend));
                CHECK(decode(encoded, n, decode_backend) == samples);
            }
        }
    }
}

TEST_CASE("PDZ scalar oracle round-trips at max amplitude", "[pdz][cross-arch]")
{
    constexpr auto lo = std::numeric_limits<std::int16_t>::min();
    constexpr auto hi = std::numeric_limits<std::int16_t>::max();
    std::vector<std::int16_t> samples;
    samples.reserve(1024);
    for (int i = 0; i < 1024; ++i) {
        samples.push_back((i % 2 == 0) ? lo : hi);
    }
    auto const encoded = encode(samples, SimdBackend::Scalar);
    CHECK(decode(encoded, samples.size(), SimdBackend::Scalar) == samples);
}
