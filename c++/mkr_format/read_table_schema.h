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

MKR_FORMAT_EXPORT Result<ReadTableSchemaDescription> read_read_table_schema(
        std::shared_ptr<arrow::Schema> const&);

}  // namespace mkr