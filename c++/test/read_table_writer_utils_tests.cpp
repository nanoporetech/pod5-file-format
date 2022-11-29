#include "pod5_format/read_table_writer_utils.h"

#include "test_utils.h"
#include "utils.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/memory_pool.h>
#include <catch2/catch.hpp>

template <typename T>
std::shared_ptr<T> get_field(arrow::StructArray & struct_array, char const * name)
{
    CAPTURE(name);
    auto field = struct_array.GetFieldByName(name);
    REQUIRE(field);
    auto typed_field = std::dynamic_pointer_cast<T>(field);
    REQUIRE(typed_field);
    return typed_field;
}

void check_field(
    std::size_t index,
    arrow::StructArray & struct_array,
    char const * name,
    std::uint16_t data)
{
    INFO("name " << name << ", index " << index);
    auto field = get_field<arrow::UInt16Array>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(
    std::size_t index,
    arrow::StructArray & struct_array,
    char const * name,
    std::int16_t data)
{
    INFO("name " << name << ", index " << index);
    auto field = get_field<arrow::Int16Array>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(
    std::size_t index,
    arrow::StructArray & struct_array,
    char const * name,
    std::uint8_t data)
{
    INFO("name " << name << ", index " << index);
    auto field = get_field<arrow::UInt8Array>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(
    std::size_t index,
    arrow::StructArray & struct_array,
    char const * name,
    float data)
{
    INFO("name " << name << ", index " << index);
    auto field = get_field<arrow::FloatArray>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(std::size_t index, arrow::StructArray & struct_array, char const * name, bool data)
{
    INFO("name " << name << ", index " << index);
    auto field = get_field<arrow::BooleanArray>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(
    std::size_t index,
    arrow::StructArray & struct_array,
    char const * name,
    std::string const & data)
{
    INFO("name " << name << ", index " << index);
    auto field = get_field<arrow::StringArray>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_timestamp_field(
    std::size_t index,
    arrow::StructArray & struct_array,
    char const * name,
    std::int64_t milliseconds_since_epoch)
{
    INFO("name " << name << ", index " << index);
    auto field = get_field<arrow::TimestampArray>(struct_array, name);
    CHECK((*field)[index] == milliseconds_since_epoch);
}

void check_field(
    std::size_t index,
    arrow::StructArray & struct_array,
    char const * name,
    pod5::RunInfoData::MapType const & data)
{
    auto field = get_field<arrow::MapArray>(struct_array, name);

    auto offsets = std::dynamic_pointer_cast<arrow::Int32Array>(field->offsets());
    auto start_data = *(*offsets)[index];
    auto end_data = *(*offsets)[index + 1];

    auto keys = std::dynamic_pointer_cast<arrow::StringArray>(field->keys());
    auto items = std::dynamic_pointer_cast<arrow::StringArray>(field->items());

    pod5::RunInfoData::MapType extracted_data;
    for (std::size_t i = start_data; i < end_data; ++i) {
        std::string key = nonstd::sv_lite::to_string(*((*keys)[i]));
        std::string item = nonstd::sv_lite::to_string(*((*items)[i]));
        extracted_data.emplace_back(key, item);
    }

    CHECK(extracted_data == data);
}

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
