#include "pod5_format/svb16/decode.hpp"
#include "pod5_format/svb16/encode.hpp"

#include <catch2/catch.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <random>
#include <vector>

#ifdef SVB16_X64

using Catch::Matchers::Equals;

template <typename Int16T, bool UseDelta, bool UseZigzag>
void test_sse_encode_scalar_decode()
{
    uint32_t const DATA_COUNT = GENERATE(
        1000,
        20000);  // Deliberately not aligned to 64 so we test the scalar tidy up code at the end.
    std::minstd_rand rng;
    std::vector<Int16T> data(DATA_COUNT);
    std::uniform_int_distribution<Int16T> dist{
        std::numeric_limits<Int16T>::min(), std::numeric_limits<Int16T>::max()};
    std::generate(data.begin(), data.end(), [&] { return dist(rng); });

    std::vector<uint8_t> encoded(svb16_max_encoded_length(data.size()));
    auto const encoded_count =
        svb16::encode_sse<Int16T, UseDelta, UseZigzag>(
            data.data(), encoded.data(), encoded.data() + svb16_key_length(data.size()), DATA_COUNT)
        - encoded.data();

    CHECK(encoded_count <= svb16_max_encoded_length(data.size()));

    std::vector<uint8_t> encoded_scalar(svb16_max_encoded_length(data.size()));
    auto const scalar_encoded_count = svb16::encode_scalar<Int16T, UseDelta, UseZigzag>(
                                          data.data(),
                                          encoded_scalar.data(),
                                          encoded_scalar.data() + svb16_key_length(data.size()),
                                          DATA_COUNT)
                                      - encoded_scalar.data();
    CHECK(scalar_encoded_count == encoded_count);
    CHECK(encoded == encoded_scalar);

    std::vector<Int16T> decoded(DATA_COUNT);
    auto const encoded_span = gsl::make_span(encoded);
    auto const key_length = svb16_key_length(data.size());
    auto const consumed = svb16::decode_sse<Int16T, UseDelta, UseZigzag>(
                              gsl::make_span(decoded),
                              encoded_span.subspan(0, key_length),
                              encoded_span.subspan(key_length))
                          - encoded.data();

    CHECK(consumed == encoded_count);

    CHECK_THAT(decoded, Equals(data));
}

TEST_CASE("SSE decode is inverse of scalar encode", "[scalar]")
{
    SECTION("Unsigned, no delta, no zig-zag")
    {
        test_sse_encode_scalar_decode<uint16_t, false, false>();
    }
    SECTION("Signed, no delta, no zig-zag")
    {
        test_sse_encode_scalar_decode<int16_t, false, false>();
    }
    SECTION("Unsigned, delta, no zig-zag")
    {
        test_sse_encode_scalar_decode<uint16_t, true, false>();
    }
    SECTION("Signed, delta, no zig-zag") { test_sse_encode_scalar_decode<int16_t, true, false>(); }
    SECTION("Unsigned, delta, zig-zag") { test_sse_encode_scalar_decode<uint16_t, true, true>(); }
    SECTION("Signed, delta, zig-zag") { test_sse_encode_scalar_decode<int16_t, true, true>(); }
    SECTION("Unsigned, no delta, zig-zag")
    {
        // this scenario doesn't really make sense, but it's possible, so let's test it
        test_sse_encode_scalar_decode<uint16_t, false, true>();
    }
    SECTION("Signed, no delta, zig-zag") { test_sse_encode_scalar_decode<int16_t, false, true>(); }
}

#endif
