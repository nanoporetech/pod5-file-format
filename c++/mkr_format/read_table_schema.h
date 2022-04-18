#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/result.h"

#include <memory>

namespace arrow {
class KeyValueMetadata;
class Schema;
class DataType;
class StructType;
}  // namespace arrow

namespace mkr {

struct PoreStructSchemaDescription {
    int channel = 0;
    int well = 1;
    int pore_type = 2;
};

struct CalibrationStructSchemaDescription {
    int offset = 0;
    int scale = 1;
};

struct EndReasonStructSchemaDescription {
    int end_reason = 0;
    int forced = 1;
};

struct RunInfoStructSchemaDescription {
    int acquisition_id;
    int acquisition_start_time;
    int adc_max;
    int adc_min;
    int context_tags;
    int experiment_name;
    int flow_cell_id;
    int flow_cell_product_code;
    int protocol_name;
    int protocol_run_id;
    int protocol_start_time;
    int sample_id;
    int sample_rate;
    int sequencing_kit;
    int sequencer_position;
    int sequencer_position_type;
    int software;
    int system_name;
    int system_type;
    int tracking_id;
};

struct ReadTableSchemaDescription {
    int read_id = 0;
    int signal = 1;
    int pore = 2;
    int calibration = 3;
    int read_number = 4;
    int start_sample = 5;
    int median_before = 6;
    int end_reason = 7;
    int run_info = 8;

    PoreStructSchemaDescription pore_fields;
    CalibrationStructSchemaDescription calibration_fields;
    EndReasonStructSchemaDescription end_reason_fields;
    RunInfoStructSchemaDescription run_info_fields;
};

MKR_FORMAT_EXPORT std::shared_ptr<arrow::StructType> make_pore_struct_type();
MKR_FORMAT_EXPORT std::shared_ptr<arrow::StructType> make_calibration_struct_type();
MKR_FORMAT_EXPORT std::shared_ptr<arrow::StructType> make_end_reason_struct_type();
MKR_FORMAT_EXPORT std::shared_ptr<arrow::StructType> make_run_info_struct_type();

/// \brief Make a new schema for a read table.
/// \param metadata Metadata to be applied to the schema.
/// \param field_locations [optional] The read table field locations, for use when writing to the table.
/// \returns The schema for a read table.
MKR_FORMAT_EXPORT std::shared_ptr<arrow::Schema> make_read_table_schema(
        std::shared_ptr<const arrow::KeyValueMetadata> const& metadata,
        ReadTableSchemaDescription* field_locations);

MKR_FORMAT_EXPORT Result<std::shared_ptr<ReadTableSchemaDescription>> read_read_table_schema(
        std::shared_ptr<arrow::Schema> const&);

MKR_FORMAT_EXPORT Result<PoreStructSchemaDescription> read_pore_struct_schema(
        std::shared_ptr<arrow::StructType> const&);
MKR_FORMAT_EXPORT Result<CalibrationStructSchemaDescription> read_calibration_struct_schema(
        std::shared_ptr<arrow::StructType> const&);
MKR_FORMAT_EXPORT Result<EndReasonStructSchemaDescription> read_end_reason_struct_schema(
        std::shared_ptr<arrow::StructType> const&);
MKR_FORMAT_EXPORT Result<RunInfoStructSchemaDescription> read_run_info_struct_schema(
        std::shared_ptr<arrow::StructType> const&);

}  // namespace mkr