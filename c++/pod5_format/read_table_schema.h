#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"
#include "pod5_format/schema_utils.h"
#include "pod5_format/tuple_utils.h"
#include "pod5_format/types.h"

#include <memory>
#include <tuple>
#include <vector>

namespace arrow {
class KeyValueMetadata;
class Schema;
class DataType;
class StructType;
}  // namespace arrow

namespace pod5 {

struct SchemaMetadataDescription;

class ReadTableSpecVersion {
public:
    static TableSpecVersion v0() { return TableSpecVersion::first_version(); }

    static TableSpecVersion v1()
    {
        // Addition of num_minknow_events and scaling parameters
        return TableSpecVersion::at_version(1);
    }

    static TableSpecVersion v2()
    {
        // Addition of num_samples parameters
        return TableSpecVersion::at_version(2);
    }

    static TableSpecVersion v3()
    {
        // Flattening of dictionaries into separate table.
        return TableSpecVersion::at_version(3);
    }

    static TableSpecVersion latest() { return v3(); }
};

class ReadTableSchemaDescription : public SchemaDescriptionBase {
public:
    ReadTableSchemaDescription();

    ReadTableSchemaDescription(ReadTableSchemaDescription const &) = delete;
    ReadTableSchemaDescription & operator=(ReadTableSchemaDescription const &) = delete;

    TableSpecVersion table_version_from_file_version(Version file_version) const override;

    // V0 fields
    Field<0, UuidArray> read_id;
    ListField<1, arrow::ListArray, arrow::UInt64Array> signal;
    Field<2, arrow::UInt32Array> read_number;
    Field<3, arrow::UInt64Array> start;
    Field<4, arrow::FloatArray> median_before;

    // V1 fields
    Field<5, arrow::UInt64Array> num_minknow_events;
    Field<6, arrow::FloatArray> tracked_scaling_scale;
    Field<7, arrow::FloatArray> tracked_scaling_shift;
    Field<8, arrow::FloatArray> predicted_scaling_scale;
    Field<9, arrow::FloatArray> predicted_scaling_shift;
    Field<10, arrow::UInt32Array> num_reads_since_mux_change;
    Field<11, arrow::FloatArray> time_since_mux_change;

    // V2 fields
    Field<12, arrow::UInt64Array> num_samples;

    // V3 fields
    Field<13, arrow::UInt16Array> channel;
    Field<14, arrow::UInt8Array> well;
    Field<15, arrow::DictionaryArray> pore_type;
    Field<16, arrow::FloatArray> calibration_offset;
    Field<17, arrow::FloatArray> calibration_scale;
    Field<18, arrow::DictionaryArray> end_reason;
    Field<19, arrow::BooleanArray> end_reason_forced;
    Field<20, arrow::DictionaryArray> run_info;

    // Field Builders only for fields we write in newly generated files.
    // Should not include fields which are removed in the latest version:
    using FieldBuilders = FieldBuilder<
        // V0 fields
        decltype(read_id),
        decltype(signal),
        decltype(read_number),
        decltype(start),
        decltype(median_before),

        // V1 fields
        decltype(num_minknow_events),
        decltype(tracked_scaling_scale),
        decltype(tracked_scaling_shift),
        decltype(predicted_scaling_scale),
        decltype(predicted_scaling_shift),
        decltype(num_reads_since_mux_change),
        decltype(time_since_mux_change),

        // V2 fields
        decltype(num_samples),

        // V3 fields
        decltype(channel),
        decltype(well),
        decltype(pore_type),
        decltype(calibration_offset),
        decltype(calibration_scale),
        decltype(end_reason),
        decltype(end_reason_forced),
        decltype(run_info)>;
};

POD5_FORMAT_EXPORT Result<std::shared_ptr<ReadTableSchemaDescription const>> read_read_table_schema(
    SchemaMetadataDescription const & schema_metadata,
    std::shared_ptr<arrow::Schema> const &);

}  // namespace pod5
