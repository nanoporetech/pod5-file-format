#include "pod5_format/read_table_writer_utils.h"

#include "utils.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/memory_pool.h>
#include <catch2/catch.hpp>

template <typename T>
std::shared_ptr<T> get_field(arrow::StructArray& struct_array, char const* name) {
    CAPTURE(name);
    auto field = struct_array.GetFieldByName(name);
    REQUIRE(field);
    auto typed_field = std::dynamic_pointer_cast<T>(field);
    REQUIRE(typed_field);
    return typed_field;
}

void check_field(std::size_t index,
                 arrow::StructArray& struct_array,
                 char const* name,
                 std::uint16_t data) {
    auto field = get_field<arrow::UInt16Array>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(std::size_t index,
                 arrow::StructArray& struct_array,
                 char const* name,
                 std::int16_t data) {
    auto field = get_field<arrow::Int16Array>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(std::size_t index,
                 arrow::StructArray& struct_array,
                 char const* name,
                 std::uint8_t data) {
    auto field = get_field<arrow::UInt8Array>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(std::size_t index,
                 arrow::StructArray& struct_array,
                 char const* name,
                 float data) {
    auto field = get_field<arrow::FloatArray>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(std::size_t index, arrow::StructArray& struct_array, char const* name, bool data) {
    auto field = get_field<arrow::BooleanArray>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_field(std::size_t index,
                 arrow::StructArray& struct_array,
                 char const* name,
                 std::string const& data) {
    auto field = get_field<arrow::StringArray>(struct_array, name);
    CHECK((*field)[index] == data);
}

void check_timestamp_field(std::size_t index,
                           arrow::StructArray& struct_array,
                           char const* name,
                           std::int64_t milliseconds_since_epoch) {
    auto field = get_field<arrow::TimestampArray>(struct_array, name);
    CHECK((*field)[index] == milliseconds_since_epoch);
}

void check_field(std::size_t index,
                 arrow::StructArray& struct_array,
                 char const* name,
                 pod5::RunInfoData::MapType const& data) {
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

TEST_CASE("Pore Writer Tests") {
    auto check_values = [](std::size_t index, arrow::StructArray& struct_array,
                           std::uint16_t channel, std::uint8_t well, std::string const& pore_type) {
        CHECK(struct_array.fields().size() == 3);
        check_field(index, struct_array, "channel", channel);
        check_field(index, struct_array, "well", well);
        check_field(index, struct_array, "pore_type", pore_type);
    };

    auto pool = arrow::system_memory_pool();
    auto pore_writer = pod5::make_pore_writer(pool);
    REQUIRE(pore_writer.ok());

    std::uint16_t channel_1 = 511;
    std::uint8_t well_1 = 2;
    std::string pore_type_1 = "pore_type";
    auto index = (*pore_writer)->add({channel_1, well_1, pore_type_1.c_str()});
    CHECK(*index == 0);
    CHECK((*pore_writer)->item_count() == 1);

    // Important to always call this so we test calling it twice
    auto const value_array = (*pore_writer)->get_value_array();

    WHEN("Checking the first row") {
        REQUIRE(value_array.ok());

        auto struct_value_array = std::dynamic_pointer_cast<arrow::StructArray>(*value_array);
        REQUIRE(!!struct_value_array);

        CHECK(struct_value_array->length() == 1);
        check_values(0, *struct_value_array, channel_1, well_1, pore_type_1);
    }

    std::uint16_t channel_2 = 513;
    std::uint8_t well_2 = 1;
    std::string pore_type_2 = "other_pore";
    index = (*pore_writer)->add({channel_2, well_2, pore_type_2.c_str()});
    CHECK(*index == 1);
    CHECK((*pore_writer)->item_count() == 2);

    WHEN("Checking the rows after a second append") {
        auto value_array = (*pore_writer)->get_value_array();
        REQUIRE(value_array.ok());

        auto struct_value_array = std::dynamic_pointer_cast<arrow::StructArray>(*value_array);
        REQUIRE(!!struct_value_array);

        CHECK(struct_value_array->length() == 2);
        check_values(0, *struct_value_array, channel_1, well_1, pore_type_1);
        check_values(1, *struct_value_array, channel_2, well_2, pore_type_2);
    }
}

TEST_CASE("End Reason Writer Tests") {
    auto check_values = [](std::size_t index, arrow::StructArray& struct_array,
                           std::string const& end_reason, bool forced) {
        CHECK(struct_array.fields().size() == 2);
        check_field(index, struct_array, "name", end_reason);
        check_field(index, struct_array, "forced", forced);
    };

    auto pool = arrow::system_memory_pool();
    auto end_reason_writer = pod5::make_end_reason_writer(pool);
    REQUIRE(end_reason_writer.ok());

    auto end_reason_1 = pod5::EndReasonData::ReadEndReason::signal_negative;
    auto forced_1 = false;
    auto index = (*end_reason_writer)->add({end_reason_1, forced_1});
    CHECK(*index == 0);
    CHECK((*end_reason_writer)->item_count() == 1);

    // Important to always call this so we test calling it twice
    auto const value_array = (*end_reason_writer)->get_value_array();

    WHEN("Checking the first row") {
        REQUIRE(value_array.ok());

        auto struct_value_array = std::dynamic_pointer_cast<arrow::StructArray>(*value_array);
        REQUIRE(!!struct_value_array);

        CHECK(struct_value_array->length() == 1);
        check_values(0, *struct_value_array, "signal_negative", forced_1);
    }

    auto end_reason_2 = pod5::EndReasonData::ReadEndReason::signal_positive;
    auto forced_2 = true;
    index = (*end_reason_writer)->add({end_reason_2, forced_2});
    CHECK(*index == 1);
    CHECK((*end_reason_writer)->item_count() == 2);

    WHEN("Checking the rows after a second append") {
        auto value_array = (*end_reason_writer)->get_value_array();
        REQUIRE(value_array.ok());

        auto struct_value_array = std::dynamic_pointer_cast<arrow::StructArray>(*value_array);
        REQUIRE(!!struct_value_array);

        CHECK(struct_value_array->length() == 2);
        check_values(0, *struct_value_array, "signal_negative", forced_1);
        check_values(1, *struct_value_array, "signal_positive", forced_2);
    }
}

TEST_CASE("Calibration Writer Tests") {
    auto check_values = [](std::size_t index, arrow::StructArray& struct_array, float offset,
                           float scale) {
        CHECK(struct_array.fields().size() == 2);
        check_field(index, struct_array, "offset", offset);
        check_field(index, struct_array, "scale", scale);
    };

    auto pool = arrow::system_memory_pool();
    auto calibration_writer = pod5::make_calibration_writer(pool);
    REQUIRE(calibration_writer.ok());

    float offset_1 = 100;
    float scale_1 = 2;
    auto index = (*calibration_writer)->add({offset_1, scale_1});
    CHECK(*index == 0);
    CHECK((*calibration_writer)->item_count() == 1);

    // Important to always call this so we test calling it twice
    auto const value_array = (*calibration_writer)->get_value_array();

    WHEN("Checking the first row") {
        REQUIRE(value_array.ok());

        auto struct_value_array = std::dynamic_pointer_cast<arrow::StructArray>(*value_array);
        REQUIRE(!!struct_value_array);

        CHECK(struct_value_array->length() == 1);
        check_values(0, *struct_value_array, offset_1, scale_1);
    }

    float offset_2 = 200;
    float scale_2 = 0.5f;
    index = (*calibration_writer)->add({offset_2, scale_2});
    CHECK(*index == 1);
    CHECK((*calibration_writer)->item_count() == 2);

    WHEN("Checking the rows after a second append") {
        auto value_array = (*calibration_writer)->get_value_array();
        REQUIRE(value_array.ok());

        auto struct_value_array = std::dynamic_pointer_cast<arrow::StructArray>(*value_array);
        REQUIRE(!!struct_value_array);

        CHECK(struct_value_array->length() == 2);
        check_values(0, *struct_value_array, offset_1, scale_1);
        check_values(1, *struct_value_array, offset_2, scale_2);
    }
}

TEST_CASE("Run Info Writer Tests") {
    auto check_values = [](std::size_t index, arrow::StructArray& struct_array,
                           pod5::RunInfoData const& data) {
        CHECK(struct_array.fields().size() == 20);

        check_field(index, struct_array, "acquisition_id", data.acquisition_id);
        check_timestamp_field(index, struct_array, "acquisition_start_time",
                              data.acquisition_start_time);
        check_field(index, struct_array, "adc_max", data.adc_max);
        check_field(index, struct_array, "adc_min", data.adc_min);
        check_field(index, struct_array, "context_tags", data.context_tags);
        check_field(index, struct_array, "experiment_name", data.experiment_name);
        check_field(index, struct_array, "flow_cell_id", data.flow_cell_id);
        check_field(index, struct_array, "flow_cell_product_code", data.flow_cell_product_code);
        check_field(index, struct_array, "protocol_name", data.protocol_name);
        check_field(index, struct_array, "protocol_run_id", data.protocol_run_id);
        check_timestamp_field(index, struct_array, "protocol_start_time", data.protocol_start_time);
        check_field(index, struct_array, "sample_id", data.sample_id);
        check_field(index, struct_array, "sample_rate", data.sample_rate);
        check_field(index, struct_array, "sequencing_kit", data.sequencing_kit);
        check_field(index, struct_array, "sequencer_position", data.sequencer_position);
        check_field(index, struct_array, "sequencer_position_type", data.sequencer_position_type);
        check_field(index, struct_array, "software", data.software);
        check_field(index, struct_array, "system_name", data.system_name);
        check_field(index, struct_array, "system_type", data.system_type);
        check_field(index, struct_array, "tracking_id", data.tracking_id);
    };

    auto pool = arrow::system_memory_pool();
    auto run_info_writer = pod5::make_run_info_writer(pool);
    REQUIRE(run_info_writer.ok());

    auto data = get_test_run_info_data("_1", 0, 4000);
    auto index = (*run_info_writer)->add(data);
    CHECK(*index == 0);
    CHECK((*run_info_writer)->item_count() == 1);

    // Important to always call this so we test calling it twice
    auto const value_array = (*run_info_writer)->get_value_array();

    WHEN("Checking the first row") {
        REQUIRE(value_array.ok());

        auto struct_value_array = std::dynamic_pointer_cast<arrow::StructArray>(*value_array);
        REQUIRE(!!struct_value_array);

        CHECK(struct_value_array->length() == 1);
        check_values(0, *struct_value_array, data);
    }

    auto data_2 = get_test_run_info_data("_2", 1000, 3012);
    index = (*run_info_writer)->add(data_2);
    CHECK(*index == 1);
    CHECK((*run_info_writer)->item_count() == 2);

    WHEN("Checking the rows after a second append") {
        auto value_array = (*run_info_writer)->get_value_array();
        REQUIRE(value_array.ok());

        auto struct_value_array = std::dynamic_pointer_cast<arrow::StructArray>(*value_array);
        REQUIRE(!!struct_value_array);

        CHECK(struct_value_array->length() == 2);
        check_values(0, *struct_value_array, data);
        check_values(1, *struct_value_array, data_2);
    }
}