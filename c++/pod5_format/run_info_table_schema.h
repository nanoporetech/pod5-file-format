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

class RunInfoTableSpecVersion {
public:
    static TableSpecVersion v0() { return TableSpecVersion::first_version(); }

    static TableSpecVersion latest() { return v0(); }
};

class RunInfoTableSchemaDescription : public SchemaDescriptionBase {
public:
    RunInfoTableSchemaDescription();

    RunInfoTableSchemaDescription(RunInfoTableSchemaDescription const &) = delete;
    RunInfoTableSchemaDescription & operator=(RunInfoTableSchemaDescription const &) = delete;

    TableSpecVersion table_version_from_file_version(Version file_version) const override;

    Field<0, arrow::StringArray> acquisition_id;
    Field<1, arrow::TimestampArray> acquisition_start_time;
    Field<2, arrow::Int16Array> adc_max;
    Field<3, arrow::Int16Array> adc_min;
    Field<4, arrow::MapArray> context_tags;
    Field<5, arrow::StringArray> experiment_name;
    Field<6, arrow::StringArray> flow_cell_id;
    Field<7, arrow::StringArray> flow_cell_product_code;
    Field<8, arrow::StringArray> protocol_name;
    Field<9, arrow::StringArray> protocol_run_id;
    Field<10, arrow::TimestampArray> protocol_start_time;
    Field<11, arrow::StringArray> sample_id;
    Field<12, arrow::UInt16Array> sample_rate;
    Field<13, arrow::StringArray> sequencing_kit;
    Field<14, arrow::StringArray> sequencer_position;
    Field<15, arrow::StringArray> sequencer_position_type;
    Field<16, arrow::StringArray> software;
    Field<17, arrow::StringArray> system_name;
    Field<18, arrow::StringArray> system_type;
    Field<19, arrow::MapArray> tracking_id;

    // Field Builders only for fields we write in newly generated files.
    // Should not include fields which are removed in the latest version:
    using FieldBuilders = FieldBuilder<
        // V0 fields
        decltype(acquisition_id),
        decltype(acquisition_start_time),
        decltype(adc_max),
        decltype(adc_min),
        decltype(context_tags),
        decltype(experiment_name),
        decltype(flow_cell_id),
        decltype(flow_cell_product_code),
        decltype(protocol_name),
        decltype(protocol_run_id),
        decltype(protocol_start_time),
        decltype(sample_id),
        decltype(sample_rate),
        decltype(sequencing_kit),
        decltype(sequencer_position),
        decltype(sequencer_position_type),
        decltype(software),
        decltype(system_name),
        decltype(system_type),
        decltype(tracking_id)>;
};

POD5_FORMAT_EXPORT Result<std::shared_ptr<RunInfoTableSchemaDescription const>>
read_run_info_table_schema(
    SchemaMetadataDescription const & schema_metadata,
    std::shared_ptr<arrow::Schema> const &);

}  // namespace pod5
