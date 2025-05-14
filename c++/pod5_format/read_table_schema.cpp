#include "pod5_format/read_table_schema.h"

#include "pod5_format/schema_metadata.h"
#include "pod5_format/types.h"

namespace pod5 {

ReadTableSchemaDescription::ReadTableSchemaDescription()
: SchemaDescriptionBase(ReadTableSpecVersion::latest())
// V0 Fields
, read_id(this, "read_id", uuid(), ReadTableSpecVersion::v0())
, signal(this, "signal", arrow::list(arrow::uint64()), ReadTableSpecVersion::v0())
, read_number(this, "read_number", arrow::uint32(), ReadTableSpecVersion::v0())
, start(this, "start", arrow::uint64(), ReadTableSpecVersion::v0())
, median_before(this, "median_before", arrow::float32(), ReadTableSpecVersion::v0())
,
// V1 Fields
num_minknow_events(this, "num_minknow_events", arrow::uint64(), ReadTableSpecVersion::v1())
, tracked_scaling_scale(this, "tracked_scaling_scale", arrow::float32(), ReadTableSpecVersion::v1())
, tracked_scaling_shift(this, "tracked_scaling_shift", arrow::float32(), ReadTableSpecVersion::v1())
, predicted_scaling_scale(
      this,
      "predicted_scaling_scale",
      arrow::float32(),
      ReadTableSpecVersion::v1())
, predicted_scaling_shift(
      this,
      "predicted_scaling_shift",
      arrow::float32(),
      ReadTableSpecVersion::v1())
, num_reads_since_mux_change(
      this,
      "num_reads_since_mux_change",
      arrow::uint32(),
      ReadTableSpecVersion::v1())
, time_since_mux_change(this, "time_since_mux_change", arrow::float32(), ReadTableSpecVersion::v1())
,
// V2 Fields
num_samples(this, "num_samples", arrow::uint64(), ReadTableSpecVersion::v2())
,
// V3 Fields
channel(this, "channel", arrow::uint16(), ReadTableSpecVersion::v3())
, well(this, "well", arrow::uint8(), ReadTableSpecVersion::v3())
, pore_type(
      this,
      "pore_type",
      arrow::dictionary(arrow::int16(), arrow::utf8()),
      ReadTableSpecVersion::v3())
, calibration_offset(this, "calibration_offset", arrow::float32(), ReadTableSpecVersion::v3())
, calibration_scale(this, "calibration_scale", arrow::float32(), ReadTableSpecVersion::v3())
, end_reason(
      this,
      "end_reason",
      arrow::dictionary(arrow::int16(), arrow::utf8()),
      ReadTableSpecVersion::v3())
, end_reason_forced(this, "end_reason_forced", arrow::boolean(), ReadTableSpecVersion::v3())
, run_info(
      this,
      "run_info",
      arrow::dictionary(arrow::int16(), arrow::utf8()),
      ReadTableSpecVersion::v3())
{
}

TableSpecVersion ReadTableSchemaDescription::table_version_from_file_version(
    Version file_version) const
{
    return ReadTableSpecVersion::latest();
}

Result<std::shared_ptr<ReadTableSchemaDescription const>> read_read_table_schema(
    SchemaMetadataDescription const & schema_metadata,
    std::shared_ptr<arrow::Schema> const & schema)
{
    auto result = std::make_shared<ReadTableSchemaDescription>();
    ARROW_RETURN_NOT_OK(ReadTableSchemaDescription::read_schema(result, schema_metadata, schema));

    return result;
}

}  // namespace pod5
