// This file contains code from https://github.com/mariusbancila/stduuid/ which has the following
// license:
//
//   MIT License
//
//   Copyright (c) 2017
//
//   Permission is hereby granted, free of charge, to any person obtaining a copy
//   of this software and associated documentation files (the "Software"), to deal
//   in the Software without restriction, including without limitation the rights
//   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//   copies of the Software, and to permit persons to whom the Software is
//   furnished to do so, subject to the following conditions:
//
//   The above copyright notice and this permission notice shall be included in all
//   copies or substantial portions of the Software.
//
//   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//   SOFTWARE.

#include "pod5_format/uuid.h"

#include <catch2/catch.hpp>

#include <set>
#include <unordered_set>

TEST_CASE("Default constructor returns nil UUID", "[pod5::Uuid]")
{
    pod5::Uuid nil;
    REQUIRE(nil.is_nil());
}

TEST_CASE("Default constructor produces all-zero string", "[pod5::Uuid]")
{
    pod5::Uuid nil;
    REQUIRE(to_string(nil) == "00000000-0000-0000-0000-000000000000");
    REQUIRE(pod5::to_string<wchar_t>(nil) == L"00000000-0000-0000-0000-000000000000");
}

TEST_CASE("Parsing the nil UUID is nil", "[pod5::Uuid]")
{
    auto const no_braces = pod5::Uuid::from_string("00000000-0000-0000-0000-000000000000");
    auto const braces = pod5::Uuid::from_string("{00000000-0000-0000-0000-000000000000}");
    auto const no_braces_w = pod5::Uuid::from_string(L"00000000-0000-0000-0000-000000000000");
    auto const braces_w = pod5::Uuid::from_string(L"{00000000-0000-0000-0000-000000000000}");

    REQUIRE(no_braces);
    REQUIRE(no_braces->is_nil());
    REQUIRE(braces);
    REQUIRE(braces->is_nil());
    REQUIRE(no_braces_w);
    REQUIRE(no_braces_w->is_nil());
    REQUIRE(braces_w);
    REQUIRE(braces_w->is_nil());
}

TEST_CASE("Parsing produces the same value with or without braces", "[pod5::Uuid]")
{
    auto const no_braces = pod5::Uuid::from_string("1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7");
    auto const braces = pod5::Uuid::from_string("{1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7}");
    auto const no_braces_w = pod5::Uuid::from_string(L"1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7");
    auto const braces_w = pod5::Uuid::from_string(L"{1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7}");

    REQUIRE(no_braces == braces);
    REQUIRE(no_braces_w == braces_w);
}

TEST_CASE("Parsing produces the same value from char or wchar_t", "[pod5::Uuid]")
{
    auto const no_braces = pod5::Uuid::from_string("1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7");
    auto const braces = pod5::Uuid::from_string("{1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7}");
    auto const no_braces_w = pod5::Uuid::from_string(L"1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7");
    auto const braces_w = pod5::Uuid::from_string(L"{1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7}");

    REQUIRE(no_braces == no_braces_w);
    REQUIRE(braces == braces_w);
}

TEST_CASE("A parsed UUID prints the same value", "[pod5::Uuid]")
{
    auto const guid = pod5::Uuid::from_string("1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7");
    REQUIRE(guid);
    REQUIRE(to_string(*guid) == "1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7");
    REQUIRE(pod5::to_string<wchar_t>(*guid) == L"1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7");
}

TEST_CASE("Invalid UUIDs cannot be parsed", "[pod5::Uuid]")
{
    REQUIRE_FALSE(pod5::Uuid::from_string(""));
    REQUIRE_FALSE(pod5::Uuid::from_string("{}"));
    // mismatched braces
    REQUIRE_FALSE(pod5::Uuid::from_string("{1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7"));
    REQUIRE_FALSE(pod5::Uuid::from_string("1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c7}"));
    // missing a char
    REQUIRE_FALSE(pod5::Uuid::from_string("1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c"));
    // too many chars
    REQUIRE_FALSE(pod5::Uuid::from_string("1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96c77"));
    // invalid characters
    REQUIRE_FALSE(pod5::Uuid::from_string("1d5a3dd9-2d50-4f2b-a0fb-a3a749eb96cg"));
}

TEST_CASE("Construction from iterators", "[pod5::Uuid]")
{
    using namespace std::string_literals;

    {
        std::array<uint8_t, 16> const arr{
            {0x47,
             0x18,
             0x38,
             0x23,
             0x25,
             0x74,
             0x4b,
             0xfd,
             0xb4,
             0x11,
             0x99,
             0xed,
             0x17,
             0x7d,
             0x3e,
             0x43}};

        pod5::Uuid guid(arr.begin(), arr.end());
        REQUIRE(to_string(guid) == "47183823-2574-4bfd-b411-99ed177d3e43"s);
    }

    {
        uint8_t const arr[16] = {
            0x47,
            0x18,
            0x38,
            0x23,
            0x25,
            0x74,
            0x4b,
            0xfd,
            0xb4,
            0x11,
            0x99,
            0xed,
            0x17,
            0x7d,
            0x3e,
            0x43};

        pod5::Uuid guid(std::begin(arr), std::end(arr));
        REQUIRE(to_string(guid) == "47183823-2574-4bfd-b411-99ed177d3e43"s);
    }
}

TEST_CASE("Construction from arrays", "[pod5::Uuid]")
{
    using namespace std::string_literals;

    {
        pod5::Uuid guid{
            {0x47,
             0x18,
             0x38,
             0x23,
             0x25,
             0x74,
             0x4b,
             0xfd,
             0xb4,
             0x11,
             0x99,
             0xed,
             0x17,
             0x7d,
             0x3e,
             0x43}};

        REQUIRE(to_string(guid) == "47183823-2574-4bfd-b411-99ed177d3e43"s);
    }

    {
        std::array<uint8_t, 16> const arr{
            {0x47,
             0x18,
             0x38,
             0x23,
             0x25,
             0x74,
             0x4b,
             0xfd,
             0xb4,
             0x11,
             0x99,
             0xed,
             0x17,
             0x7d,
             0x3e,
             0x43}};

        pod5::Uuid guid(arr);
        REQUIRE(to_string(guid) == "47183823-2574-4bfd-b411-99ed177d3e43"s);
    }

    {
        uint8_t const arr[16] = {
            0x47,
            0x18,
            0x38,
            0x23,
            0x25,
            0x74,
            0x4b,
            0xfd,
            0xb4,
            0x11,
            0x99,
            0xed,
            0x17,
            0x7d,
            0x3e,
            0x43};

        pod5::Uuid guid(arr);
        REQUIRE(to_string(guid) == "47183823-2574-4bfd-b411-99ed177d3e43"s);
    }
}

TEST_CASE("Test equality", "[operators]")
{
    pod5::Uuid empty;

    auto engine = pod5::UuidRandomGenerator::engine_type{Catch::rngSeed()};
    pod5::Uuid guid = pod5::UuidRandomGenerator{engine}();

    REQUIRE(empty == empty);
    REQUIRE(guid == guid);
    REQUIRE(empty != guid);
}

TEST_CASE("Test comparison", "[operators]")
{
    auto empty = pod5::Uuid{};

    auto engine = pod5::UuidRandomGenerator::engine_type{Catch::rngSeed()};

    pod5::UuidRandomGenerator gen{engine};
    auto id = gen();

    REQUIRE(empty < id);

    std::set<pod5::Uuid> ids{pod5::Uuid{}, gen(), gen(), gen(), gen()};

    REQUIRE(ids.size() == 5);
    REQUIRE(ids.find(pod5::Uuid{}) != ids.end());
}

TEST_CASE("Test hashing", "[ops]")
{
    using namespace std::string_literals;
    auto str = "47183823-2574-4bfd-b411-99ed177d3e43"s;
    auto guid = pod5::Uuid::from_string(str).value();

    auto h1 = std::hash<std::string>{};
    auto h2 = std::hash<pod5::Uuid>{};
#ifdef UUID_HASH_STRING_BASED
    REQUIRE(h1(str) == h2(guid));
#else
    REQUIRE(h1(str) != h2(guid));
#endif

    auto engine = pod5::UuidRandomGenerator::engine_type{Catch::rngSeed()};
    pod5::UuidRandomGenerator gen{engine};

    std::unordered_set<pod5::Uuid> ids{pod5::Uuid{}, gen(), gen(), gen(), gen()};

    REQUIRE(ids.size() == 5);
    REQUIRE(ids.find(pod5::Uuid{}) != ids.end());
}

TEST_CASE("Test swap", "[ops]")
{
    pod5::Uuid empty;

    auto engine = pod5::UuidRandomGenerator::engine_type{Catch::rngSeed()};
    pod5::Uuid guid = pod5::UuidRandomGenerator{engine}();

    REQUIRE(empty.is_nil());
    REQUIRE(!guid.is_nil());

    std::swap(empty, guid);

    REQUIRE(!empty.is_nil());
    REQUIRE(guid.is_nil());

    empty.swap(guid);

    REQUIRE(empty.is_nil());
    REQUIRE(!guid.is_nil());
}

TEST_CASE("Test constexpr", "[const]")
{
    constexpr pod5::Uuid empty;
    static_assert(empty.is_nil());
}

TEST_CASE("Test size", "[operators]") { REQUIRE(sizeof(pod5::Uuid) == 16); }
