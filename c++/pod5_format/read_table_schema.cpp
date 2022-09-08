#include "pod5_format/read_table_schema.h"

#include "pod5_format/internal/schema_utils.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/types.h"

namespace pod5 {

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
            arrow::field("acquisition_start_time", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
            arrow::field("adc_max", arrow::int16()),
            arrow::field("adc_min", arrow::int16()),
            arrow::field("context_tags", arrow::map(arrow::utf8(), arrow::utf8())),
            arrow::field("experiment_name", arrow::utf8()),
            arrow::field("flow_cell_id", arrow::utf8()),
            arrow::field("flow_cell_product_code", arrow::utf8()),
            arrow::field("protocol_name", arrow::utf8()),
            arrow::field("protocol_run_id", arrow::utf8()),
            arrow::field("protocol_start_time", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
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

ReadTableSchemaDescription::ReadTableSchemaDescription()
        // V0 Fields
        : read_id(this, "read_id", uuid(), ReadTableSpecVersion::TableV0Version),
          signal(this,
                 "signal",
                 arrow::list(arrow::uint64()),
                 ReadTableSpecVersion::TableV0Version),
          pore(this,
               "pore",
               arrow::dictionary(arrow::int16(), make_pore_struct_type()),
               ReadTableSpecVersion::TableV0Version),
          calibration(this,
                      "calibration",
                      arrow::dictionary(arrow::int16(), make_calibration_struct_type()),
                      ReadTableSpecVersion::TableV0Version),
          read_number(this, "read_number", arrow::uint32(), ReadTableSpecVersion::TableV0Version),
          start(this, "start", arrow::uint64(), ReadTableSpecVersion::TableV0Version),
          median_before(this,
                        "median_before",
                        arrow::float32(),
                        ReadTableSpecVersion::TableV0Version),
          end_reason(this,
                     "end_reason",
                     arrow::dictionary(arrow::int16(), make_end_reason_struct_type()),
                     ReadTableSpecVersion::TableV0Version),
          run_info(this,
                   "run_info",
                   arrow::dictionary(arrow::int16(), make_run_info_struct_type()),
                   ReadTableSpecVersion::TableV0Version)
          // V1 Fields
          ,
          num_minknow_events(this,
                             "num_minknow_events",
                             arrow::uint64(),
                             ReadTableSpecVersion::TableV1Version),
          tracked_scaling_scale(this,
                                "tracked_scaling_scale",
                                arrow::float32(),
                                ReadTableSpecVersion::TableV1Version),
          tracked_scaling_shift(this,
                                "tracked_scaling_shift",
                                arrow::float32(),
                                ReadTableSpecVersion::TableV1Version),
          predicted_scaling_scale(this,
                                  "predicted_scaling_scale",
                                  arrow::float32(),
                                  ReadTableSpecVersion::TableV1Version),
          predicted_scaling_shift(this,
                                  "predicted_scaling_shift",
                                  arrow::float32(),
                                  ReadTableSpecVersion::TableV1Version),
          num_reads_since_mux_change(this,
                                     "num_reads_since_mux_change",
                                     arrow::uint32(),
                                     ReadTableSpecVersion::TableV1Version),
          time_since_mux_change(this,
                                "time_since_mux_change",
                                arrow::float32(),
                                ReadTableSpecVersion::TableV1Version) {}

std::shared_ptr<arrow::Schema> ReadTableSchemaDescription::make_schema(
        std::shared_ptr<const arrow::KeyValueMetadata> const& metadata) const {
    arrow::FieldVector arrow_fields;
    for (auto& field : fields()) {
        arrow_fields.emplace_back(arrow::field(field->name(), field->datatype()));
    }

    return arrow::schema(arrow_fields, metadata);
}

Result<std::shared_ptr<arrow::StructType>> read_dict_value_struct_type(
        std::shared_ptr<arrow::DataType> const& datatype) {
    if (datatype->id() != arrow::Type::DICTIONARY) {
        return arrow::Status::Invalid("Dictionary type is not a dictionary");
    }

    auto const dict_type = std::static_pointer_cast<arrow::DictionaryType>(datatype);
    auto const value_type = std::dynamic_pointer_cast<arrow::StructType>(dict_type->value_type());
    if (!value_type) {
        return arrow::Status::Invalid("Dictionary value type is not a struct");
    }
    return value_type;
}

Result<std::shared_ptr<ReadTableSchemaDescription const>> read_read_table_schema(
        SchemaMetadataDescription const& schema_metadata,
        std::shared_ptr<arrow::Schema> const& schema) {
    auto result = std::make_shared<ReadTableSchemaDescription>();
    if (schema_metadata.writing_pod5_version < Version(0, 0, 24)) {
        result->table_spec_version = ReadTableSpecVersion::TableV0Version;
    }

    for (auto& field : result->fields()) {
        if (result->table_spec_version < field->added_table_spec_version()) {
            continue;
        }
        auto const& datatype = field->datatype();
        int field_index = 0;
        if (datatype->id() == arrow::Type::DICTIONARY) {
            std::shared_ptr<arrow::StructType> value_type;
            ARROW_ASSIGN_OR_RAISE(field_index, find_dict_struct_field(schema, field->name().c_str(),
                                                                      arrow::int16(), &value_type));
        } else {
            ARROW_ASSIGN_OR_RAISE(field_index, find_field(schema, field->name().c_str(), datatype));
        }
        field->set_field_index(field_index);
    }

    ARROW_ASSIGN_OR_RAISE(auto pore_dict_value_type,
                          read_dict_value_struct_type(result->pore.datatype()));
    ARROW_ASSIGN_OR_RAISE(result->pore_fields, read_pore_struct_schema(pore_dict_value_type));

    ARROW_ASSIGN_OR_RAISE(auto calib_dict_value_type,
                          read_dict_value_struct_type(result->calibration.datatype()));
    ARROW_ASSIGN_OR_RAISE(result->calibration_fields,
                          read_calibration_struct_schema(calib_dict_value_type));

    ARROW_ASSIGN_OR_RAISE(auto end_reason_dict_value_type,
                          read_dict_value_struct_type(result->end_reason.datatype()));
    ARROW_ASSIGN_OR_RAISE(result->end_reason_fields,
                          read_end_reason_struct_schema(end_reason_dict_value_type));

    ARROW_ASSIGN_OR_RAISE(auto run_info_dict_value_type,
                          read_dict_value_struct_type(result->run_info.datatype()));
    ARROW_ASSIGN_OR_RAISE(result->run_info_fields,
                          read_run_info_struct_schema(run_info_dict_value_type));

    return result;
}

Result<int> find_struct_field(std::shared_ptr<arrow::StructType> const& type, char const* name) {
    int field_idx = type->GetFieldIndex(name);
    if (field_idx == -1) {
        return pod5::Status::Invalid("Missing ", name, " field in struct");
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

POD5_FORMAT_EXPORT Result<CalibrationStructSchemaDescription> read_calibration_struct_schema(
        std::shared_ptr<arrow::StructType> const& type) {
    ARROW_ASSIGN_OR_RAISE(auto offset, find_struct_field(type, "offset"));
    ARROW_ASSIGN_OR_RAISE(auto scale, find_struct_field(type, "scale"));

    return CalibrationStructSchemaDescription{offset, scale};
}

POD5_FORMAT_EXPORT Result<EndReasonStructSchemaDescription> read_end_reason_struct_schema(
        std::shared_ptr<arrow::StructType> const& type) {
    ARROW_ASSIGN_OR_RAISE(auto end_reason, find_struct_field(type, "name"));
    ARROW_ASSIGN_OR_RAISE(auto forced, find_struct_field(type, "forced"));

    return EndReasonStructSchemaDescription{end_reason, forced};
}

POD5_FORMAT_EXPORT Result<RunInfoStructSchemaDescription> read_run_info_struct_schema(
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

}  // namespace pod5