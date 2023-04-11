#include "pod5_format/run_info_table_schema.h"

#include "pod5_format/schema_metadata.h"
#include "pod5_format/types.h"

namespace pod5 {

RunInfoTableSchemaDescription::RunInfoTableSchemaDescription()
: SchemaDescriptionBase(RunInfoTableSpecVersion::latest())
// V0 Fields
, acquisition_id(this, "acquisition_id", arrow::utf8(), RunInfoTableSpecVersion::v0())
, acquisition_start_time(
      this,
      "acquisition_start_time",
      arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"),
      RunInfoTableSpecVersion::v0())
, adc_max(this, "adc_max", arrow::int16(), RunInfoTableSpecVersion::v0())
, adc_min(this, "adc_min", arrow::int16(), RunInfoTableSpecVersion::v0())
, context_tags(
      this,
      "context_tags",
      arrow::map(arrow::utf8(), arrow::utf8()),
      RunInfoTableSpecVersion::v0())
, experiment_name(this, "experiment_name", arrow::utf8(), RunInfoTableSpecVersion::v0())
, flow_cell_id(this, "flow_cell_id", arrow::utf8(), RunInfoTableSpecVersion::v0())
, flow_cell_product_code(
      this,
      "flow_cell_product_code",
      arrow::utf8(),
      RunInfoTableSpecVersion::v0())
, protocol_name(this, "protocol_name", arrow::utf8(), RunInfoTableSpecVersion::v0())
, protocol_run_id(this, "protocol_run_id", arrow::utf8(), RunInfoTableSpecVersion::v0())
, protocol_start_time(
      this,
      "protocol_start_time",
      arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"),
      RunInfoTableSpecVersion::v0())
, sample_id(this, "sample_id", arrow::utf8(), RunInfoTableSpecVersion::v0())
, sample_rate(this, "sample_rate", arrow::uint16(), RunInfoTableSpecVersion::v0())
, sequencing_kit(this, "sequencing_kit", arrow::utf8(), RunInfoTableSpecVersion::v0())
, sequencer_position(this, "sequencer_position", arrow::utf8(), RunInfoTableSpecVersion::v0())
, sequencer_position_type(
      this,
      "sequencer_position_type",
      arrow::utf8(),
      RunInfoTableSpecVersion::v0())
, software(this, "software", arrow::utf8(), RunInfoTableSpecVersion::v0())
, system_name(this, "system_name", arrow::utf8(), RunInfoTableSpecVersion::v0())
, system_type(this, "system_type", arrow::utf8(), RunInfoTableSpecVersion::v0())
, tracking_id(
      this,
      "tracking_id",
      arrow::map(arrow::utf8(), arrow::utf8()),
      RunInfoTableSpecVersion::v0())
{
}

TableSpecVersion RunInfoTableSchemaDescription::table_version_from_file_version(
    Version file_version) const
{
    return RunInfoTableSpecVersion::latest();
}

Result<std::shared_ptr<RunInfoTableSchemaDescription const>> read_run_info_table_schema(
    SchemaMetadataDescription const & schema_metadata,
    std::shared_ptr<arrow::Schema> const & schema)
{
    auto result = std::make_shared<RunInfoTableSchemaDescription>();
    ARROW_RETURN_NOT_OK(
        RunInfoTableSchemaDescription::read_schema(result, schema_metadata, schema));

    return result;
}

}  // namespace pod5
