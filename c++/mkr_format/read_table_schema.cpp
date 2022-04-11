#include "mkr_format/read_table_schema.h"

#include "mkr_format/schema_utils.h"
#include "mkr_format/types.h"

#include <arrow/type.h>

#include <iostream>

namespace mkr {

std::shared_ptr<arrow::StructType> make_pore_struct_type() {
    return std::static_pointer_cast<arrow::StructType>(arrow::struct_({
            arrow::field("channel", arrow::uint16()),
            arrow::field("well", arrow::uint8()),
            arrow::field("pore_type", arrow::utf8()),
    }));
}

std::shared_ptr<arrow::StructType> make_calibration_struct_type() {
    return std::static_pointer_cast<arrow::StructType>(arrow::struct_({
            arrow::field("offset", arrow::float32()),
            arrow::field("scale", arrow::float32()),
    }));
}

std::shared_ptr<arrow::StructType> make_end_reason_struct_type() {
    return std::static_pointer_cast<arrow::StructType>(arrow::struct_({
            arrow::field("name", arrow::utf8()),
            arrow::field("forced", arrow::boolean()),
    }));
}

std::shared_ptr<arrow::StructType> make_run_info_struct_type() {
    return std::static_pointer_cast<arrow::StructType>(arrow::struct_({
            arrow::field("acquisition_id", arrow::utf8()),
            arrow::field("acquisition_start_time", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("adc_max", arrow::int16()),
            arrow::field("adc_min", arrow::int16()),
            arrow::field("context_tags", arrow::map(arrow::utf8(), arrow::utf8())),
            arrow::field("experiment_name", arrow::utf8()),
            arrow::field("flow_cell_id", arrow::utf8()),
            arrow::field("flow_cell_product_code", arrow::utf8()),
            arrow::field("protocol_name", arrow::utf8()),
            arrow::field("protocol_run_id", arrow::utf8()),
            arrow::field("protocol_start_time", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("sample_id", arrow::utf8()),
            arrow::field("sample_rate", arrow::uint16()),
            arrow::field("sequencing_kit", arrow::utf8()),
            arrow::field("sequencer_position", arrow::utf8()),
            arrow::field("sequencer_position_type", arrow::utf8()),
            arrow::field("software", arrow::utf8()),
            arrow::field("system_name", arrow::utf8()),
            arrow::field("system_type", arrow::utf8()),
            arrow::field("tracking_id", arrow::map(arrow::utf8(), arrow::utf8())),
    }));
}

std::shared_ptr<arrow::Schema> make_read_table_schema(
        std::shared_ptr<const arrow::KeyValueMetadata> const& metadata,
        ReadTableSchemaDescription* field_locations) {
    if (field_locations) {
        *field_locations = {};
    }

    return arrow::schema(
            {
                    arrow::field("read_id", uuid()),
                    arrow::field("signal", arrow::list(arrow::uint64())),
                    arrow::field("pore",
                                 arrow::dictionary(arrow::int16(), make_pore_struct_type())),
                    arrow::field("calibration",
                                 arrow::dictionary(arrow::int16(), make_calibration_struct_type())),
                    arrow::field("read_number", arrow::uint32()),
                    arrow::field("start", arrow::uint64()),
                    arrow::field("median_before", arrow::float32()),
                    arrow::field("end_reason",
                                 arrow::dictionary(arrow::int16(), make_end_reason_struct_type())),
                    arrow::field("run_info",
                                 arrow::dictionary(arrow::int16(), make_run_info_struct_type())),
            },
            metadata);
}

Result<ReadTableSchemaDescription> read_read_table_schema(
        std::shared_ptr<arrow::Schema> const& schema) {
    ARROW_ASSIGN_OR_RAISE(auto read_id_field_idx, find_field(schema, "read_id", uuid()));
    ARROW_ASSIGN_OR_RAISE(auto signal_field_idx,
                          find_field(schema, "signal", arrow::list(arrow::uint64())));
    ARROW_ASSIGN_OR_RAISE(
            auto pore_field_idx,
            find_field(schema, "pore", arrow::dictionary(arrow::int16(), make_pore_struct_type())));
    ARROW_ASSIGN_OR_RAISE(
            auto calibration_field_idx,
            find_field(schema, "calibration",
                       arrow::dictionary(arrow::int16(), make_calibration_struct_type())));
    ARROW_ASSIGN_OR_RAISE(auto read_number_field_idx,
                          find_field(schema, "read_number", arrow::uint32()));
    ARROW_ASSIGN_OR_RAISE(auto start_field_idx, find_field(schema, "start", arrow::uint64()));
    ARROW_ASSIGN_OR_RAISE(auto median_before_field_idx,
                          find_field(schema, "median_before", arrow::float32()));
    ARROW_ASSIGN_OR_RAISE(
            auto end_reason_field_idx,
            find_field(schema, "end_reason",
                       arrow::dictionary(arrow::int16(), make_end_reason_struct_type())));
    ARROW_ASSIGN_OR_RAISE(
            auto run_info_field_idx,
            find_field(schema, "run_info",
                       arrow::dictionary(arrow::int16(), make_run_info_struct_type())));

    return ReadTableSchemaDescription{
            read_id_field_idx,       signal_field_idx,      pore_field_idx,
            calibration_field_idx,   read_number_field_idx, start_field_idx,
            median_before_field_idx, end_reason_field_idx,  run_info_field_idx};
}

}  // namespace mkr