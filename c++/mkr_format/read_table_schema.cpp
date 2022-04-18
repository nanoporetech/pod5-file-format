#include "mkr_format/read_table_schema.h"

#include "mkr_format/internal/schema_utils.h"
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

Result<std::shared_ptr<ReadTableSchemaDescription>> read_read_table_schema(
        std::shared_ptr<arrow::Schema> const& schema) {
    ARROW_ASSIGN_OR_RAISE(auto read_id_field_idx, find_field(schema, "read_id", uuid()));
    ARROW_ASSIGN_OR_RAISE(auto signal_field_idx,
                          find_field(schema, "signal", arrow::list(arrow::uint64())));
    std::shared_ptr<arrow::StructType> pore_type;
    ARROW_ASSIGN_OR_RAISE(auto pore_field_idx,
                          find_dict_struct_field(schema, "pore", arrow::int16(), &pore_type));
    std::shared_ptr<arrow::StructType> calibration_type;
    ARROW_ASSIGN_OR_RAISE(
            auto calibration_field_idx,
            find_dict_struct_field(schema, "calibration", arrow::int16(), &calibration_type));
    ARROW_ASSIGN_OR_RAISE(auto read_number_field_idx,
                          find_field(schema, "read_number", arrow::uint32()));
    ARROW_ASSIGN_OR_RAISE(auto start_field_idx, find_field(schema, "start", arrow::uint64()));
    ARROW_ASSIGN_OR_RAISE(auto median_before_field_idx,
                          find_field(schema, "median_before", arrow::float32()));
    std::shared_ptr<arrow::StructType> end_reason_type;
    ARROW_ASSIGN_OR_RAISE(
            auto end_reason_field_idx,
            find_dict_struct_field(schema, "end_reason", arrow::int16(), &end_reason_type));
    std::shared_ptr<arrow::StructType> run_info_type;
    ARROW_ASSIGN_OR_RAISE(
            auto run_info_field_idx,
            find_dict_struct_field(schema, "run_info", arrow::int16(), &run_info_type));

    ARROW_ASSIGN_OR_RAISE(auto pore_fields, read_pore_struct_schema(pore_type));
    ARROW_ASSIGN_OR_RAISE(auto calibration_fields,
                          read_calibration_struct_schema(calibration_type));
    ARROW_ASSIGN_OR_RAISE(auto end_reason_fields, read_end_reason_struct_schema(end_reason_type));
    ARROW_ASSIGN_OR_RAISE(auto run_info_fields, read_run_info_struct_schema(run_info_type));

    return std::make_shared<ReadTableSchemaDescription>(ReadTableSchemaDescription{
            read_id_field_idx, signal_field_idx, pore_field_idx, calibration_field_idx,
            read_number_field_idx, start_field_idx, median_before_field_idx, end_reason_field_idx,
            run_info_field_idx, pore_fields, calibration_fields, end_reason_fields,
            run_info_fields});
}

Result<int> find_struct_field(std::shared_ptr<arrow::StructType> const& type, char const* name) {
    int field_idx = type->GetFieldIndex(name);
    if (field_idx == -1) {
        return mkr::Status::Invalid("Missing ", name, " field in struct");
    }

    return field_idx;
}

Result<PoreStructSchemaDescription> read_pore_struct_schema(
        std::shared_ptr<arrow::StructType> const& type) {
    ARROW_ASSIGN_OR_RAISE(auto channel, find_struct_field(type, "channel"));
    ARROW_ASSIGN_OR_RAISE(auto well, find_struct_field(type, "well"));
    ARROW_ASSIGN_OR_RAISE(auto pore_type, find_struct_field(type, "pore_type"));

    return PoreStructSchemaDescription{channel, well, pore_type};
}

MKR_FORMAT_EXPORT Result<CalibrationStructSchemaDescription> read_calibration_struct_schema(
        std::shared_ptr<arrow::StructType> const& type) {
    ARROW_ASSIGN_OR_RAISE(auto offset, find_struct_field(type, "offset"));
    ARROW_ASSIGN_OR_RAISE(auto scale, find_struct_field(type, "scale"));

    return CalibrationStructSchemaDescription{offset, scale};
}

MKR_FORMAT_EXPORT Result<EndReasonStructSchemaDescription> read_end_reason_struct_schema(
        std::shared_ptr<arrow::StructType> const& type) {
    ARROW_ASSIGN_OR_RAISE(auto end_reason, find_struct_field(type, "name"));
    ARROW_ASSIGN_OR_RAISE(auto forced, find_struct_field(type, "forced"));

    return EndReasonStructSchemaDescription{end_reason, forced};
}

MKR_FORMAT_EXPORT Result<RunInfoStructSchemaDescription> read_run_info_struct_schema(
        std::shared_ptr<arrow::StructType> const& type) {
    ARROW_ASSIGN_OR_RAISE(auto acquisition_id, find_struct_field(type, "acquisition_id"));
    ARROW_ASSIGN_OR_RAISE(auto acquisition_start_time,
                          find_struct_field(type, "acquisition_start_time"));
    ARROW_ASSIGN_OR_RAISE(auto adc_max, find_struct_field(type, "adc_max"));
    ARROW_ASSIGN_OR_RAISE(auto adc_min, find_struct_field(type, "adc_min"));
    ARROW_ASSIGN_OR_RAISE(auto context_tags, find_struct_field(type, "context_tags"));
    ARROW_ASSIGN_OR_RAISE(auto experiment_name, find_struct_field(type, "experiment_name"));
    ARROW_ASSIGN_OR_RAISE(auto flow_cell_id, find_struct_field(type, "flow_cell_id"));
    ARROW_ASSIGN_OR_RAISE(auto flow_cell_product_code,
                          find_struct_field(type, "flow_cell_product_code"));
    ARROW_ASSIGN_OR_RAISE(auto protocol_name, find_struct_field(type, "protocol_name"));
    ARROW_ASSIGN_OR_RAISE(auto protocol_run_id, find_struct_field(type, "protocol_run_id"));
    ARROW_ASSIGN_OR_RAISE(auto protocol_start_time, find_struct_field(type, "protocol_start_time"));
    ARROW_ASSIGN_OR_RAISE(auto sample_id, find_struct_field(type, "sample_id"));
    ARROW_ASSIGN_OR_RAISE(auto sample_rate, find_struct_field(type, "sample_rate"));
    ARROW_ASSIGN_OR_RAISE(auto sequencing_kit, find_struct_field(type, "sequencing_kit"));
    ARROW_ASSIGN_OR_RAISE(auto sequencer_position, find_struct_field(type, "sequencer_position"));
    ARROW_ASSIGN_OR_RAISE(auto sequencer_position_type,
                          find_struct_field(type, "sequencer_position_type"));
    ARROW_ASSIGN_OR_RAISE(auto software, find_struct_field(type, "software"));
    ARROW_ASSIGN_OR_RAISE(auto system_name, find_struct_field(type, "system_name"));
    ARROW_ASSIGN_OR_RAISE(auto system_type, find_struct_field(type, "system_type"));
    ARROW_ASSIGN_OR_RAISE(auto tracking_id, find_struct_field(type, "tracking_id"));

    return RunInfoStructSchemaDescription{
            acquisition_id,
            acquisition_start_time,
            adc_max,
            adc_min,
            context_tags,
            experiment_name,
            flow_cell_id,
            flow_cell_product_code,
            protocol_name,
            protocol_run_id,
            protocol_start_time,
            sample_id,
            sample_rate,
            sequencing_kit,
            sequencer_position,
            sequencer_position_type,
            software,
            system_name,
            system_type,
            tracking_id,
    };
}

}  // namespace mkr