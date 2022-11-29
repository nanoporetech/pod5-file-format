#include "pod5_format/schema_metadata.h"
#include "test_utils.h"

#include <catch2/catch.hpp>

SCENARIO("Version Tests")
{
    using namespace pod5;

    CHECK(Version(1, 2, 3) < Version(3, 2, 1));
    CHECK(Version(1, 2, 3) < Version(1, 3, 3));
    CHECK(Version(1, 2, 3) < Version(1, 2, 4));

    CHECK(Version(3, 2, 1) > Version(1, 2, 3));
    CHECK(Version(1, 3, 3) > Version(1, 2, 3));
    CHECK(Version(1, 2, 4) > Version(1, 2, 3));

    CHECK(Version(1, 2, 3) == Version(1, 2, 3));

    CHECK(Version(1, 2, 3) != Version(2, 2, 3));
    CHECK(Version(1, 2, 3) != Version(1, 3, 3));
    CHECK(Version(1, 2, 3) != Version(1, 2, 4));

    CHECK_ARROW_STATUS_NOT_OK(parse_version_number("1.2.3.4"));
    CHECK_ARROW_STATUS_NOT_OK(parse_version_number("1.2.3-pre"));

    auto const parsed_version = parse_version_number("10.200.3");
    REQUIRE_ARROW_STATUS_OK(parsed_version);
    CHECK(Version(10, 200, 3) == *parsed_version);
    CHECK(parsed_version->major_version() == 10);
    CHECK(parsed_version->minor_version() == 200);
    CHECK(parsed_version->revision_version() == 3);

    CHECK(Version(1, 200, 30).to_string() == "1.200.30");
}
