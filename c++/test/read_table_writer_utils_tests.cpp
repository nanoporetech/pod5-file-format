#include "pod5_format/read_table_writer_utils.h"

#include "test_utils.h"
#include "utils.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/memory_pool.h>
#include <catch2/catch.hpp>

TEST_CASE("Run Info Writer Tests")
{
    auto pool = arrow::system_memory_pool();
    auto run_info_writer = pod5::make_run_info_writer(pool);
    REQUIRE_ARROW_STATUS_OK(run_info_writer);

    auto index = (*run_info_writer)->add("acq_id_1");
    CHECK(*index == 0);
    CHECK((*run_info_writer)->item_count() == 1);

    // Important to always call this so we test calling it twice
    auto const value_array = (*run_info_writer)->get_value_array();

    WHEN("Checking the first row")
    {
        REQUIRE_ARROW_STATUS_OK(value_array);

        auto string_value_array = std::dynamic_pointer_cast<arrow::StringArray>(*value_array);
        REQUIRE(!!string_value_array);

        CHECK(string_value_array->length() == 1);
        CHECK(string_value_array->Value(0) == "acq_id_1");
    }

    index = (*run_info_writer)->add("acq_id_2");
    CHECK(*index == 1);
    CHECK((*run_info_writer)->item_count() == 2);

    WHEN("Checking the rows after a second append")
    {
        auto value_array = (*run_info_writer)->get_value_array();
        REQUIRE_ARROW_STATUS_OK(value_array);

        auto string_value_array = std::dynamic_pointer_cast<arrow::StringArray>(*value_array);
        REQUIRE(!!string_value_array);

        CHECK(string_value_array->length() == 2);
        CHECK(string_value_array->Value(0) == "acq_id_1");
        CHECK(string_value_array->Value(1) == "acq_id_2");
    }
}
