#include "mkr_format/c_api.h"

#include <catch2/catch.hpp>

SCENARIO("C API") {
    static constexpr char const* combined_filename = "./foo.mkr";

    CHECK(mkr_get_error_no() == MKR_OK);
    CHECK(!mkr_create_combined_file(NULL, "c_software", NULL));
    CHECK(mkr_get_error_no() == MKR_ERROR_INVALID);
    CHECK(!mkr_create_combined_file("", "c_software", NULL));
    CHECK(mkr_get_error_no() == MKR_ERROR_INVALID);
    CHECK(!mkr_create_combined_file("", NULL, NULL));
    CHECK(mkr_get_error_no() == MKR_ERROR_INVALID);

    auto combined_file = mkr_create_combined_file(combined_filename, "c_software", NULL);
    REQUIRE(combined_file);
    CHECK(mkr_get_error_no() == MKR_OK);

    std::int16_t pore_id = -1;
    CHECK(mkr_add_pore(&pore_id, combined_file, 43, 2, "pore_type") == MKR_OK);
    CHECK(pore_id == 0);

    std::int16_t end_reason_id = -1;
    CHECK(mkr_add_end_reason(&end_reason_id, combined_file, MKR_END_REASON_MUX_CHANGE, false) ==
          MKR_OK);
    CHECK(end_reason_id == 0);

    mkr_close_and_free_writer(combined_file);
    CHECK(mkr_get_error_no() == MKR_OK);
}