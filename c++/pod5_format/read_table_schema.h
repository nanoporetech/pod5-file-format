#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"
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

enum class SpecialFieldValues : int {
    InvalidField = -1,
};

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

enum class ReadTableSpecVersion {
    TableV0Version = 0,
    // Addition of num_minknow_events and scaling parameters
    TableV1Version = 1,

    TableLatestVersion = TableV1Version
};

class FieldBase;

class SchamaDescriptionBase {
public:
    void add_field(FieldBase* field) { m_fields.push_back(field); }

    std::vector<FieldBase*> const& fields() { return m_fields; }
    std::vector<FieldBase const*> const& fields() const {
        return reinterpret_cast<std::vector<FieldBase const*> const&>(m_fields);
    }

private:
    std::vector<FieldBase*> m_fields;
};

namespace detail {
template <typename ArrayType>
class BuilderHelper;
template <typename ArrayType, typename ElementArrayType>
class ListBuilderHelper;
}  // namespace detail

class FieldBase {
public:
    FieldBase(SchamaDescriptionBase* owner,
              int field_index,
              std::string name,
              std::shared_ptr<arrow::DataType> const& datatype,
              ReadTableSpecVersion added_table_spec_version = ReadTableSpecVersion::TableV0Version)
            : m_name(name),
              m_datatype(datatype),
              m_field_index(field_index),
              m_added_table_spec_version(added_table_spec_version) {
        owner->add_field(this);
    }

    std::string const& name() const { return m_name; }
    std::shared_ptr<arrow::DataType> const& datatype() const { return m_datatype; }
    int field_index() const { return m_field_index; }
    ReadTableSpecVersion added_table_spec_version() const { return m_added_table_spec_version; }

    void set_field_index(int index) { m_field_index = index; }

private:
    std::string m_name;
    std::shared_ptr<arrow::DataType> m_datatype;
    int m_field_index;
    ReadTableSpecVersion m_added_table_spec_version;
};

template <int Index_, typename ArrayType_>
struct Field : public FieldBase {
    using Index = std::integral_constant<int, Index_>;
    using ArrayType = ArrayType_;
    using BuilderType = detail::BuilderHelper<ArrayType>;

    Field(SchamaDescriptionBase* owner,
          std::string name,
          std::shared_ptr<arrow::DataType> const& datatype,
          ReadTableSpecVersion added_table_spec_version = ReadTableSpecVersion::TableV0Version)
            : FieldBase(owner, Index::value, name, datatype, added_table_spec_version) {}
};

template <int Index_, typename ArrayType_, typename ElementType_>
struct ListField : public Field<Index_, ArrayType_> {
    using ElementType = ElementType_;
    using BuilderType = detail::ListBuilderHelper<ArrayType_, ElementType>;

    ListField(SchamaDescriptionBase* owner,
              std::string name,
              std::shared_ptr<arrow::DataType> const& datatype,
              ReadTableSpecVersion added_table_spec_version = ReadTableSpecVersion::TableV0Version)
            : Field<Index_, ArrayType_>(owner, name, datatype, added_table_spec_version) {}
};

template <typename... Args>
class FieldBuilder;

class ReadTableSchemaDescription : public SchamaDescriptionBase {
public:
    ReadTableSchemaDescription();

    ReadTableSchemaDescription(ReadTableSchemaDescription const&) = delete;
    ReadTableSchemaDescription& operator=(ReadTableSchemaDescription const&) = delete;

    /// \brief Make a new schema for a read table.
    /// \param metadata Metadata to be applied to the schema.
    /// \param field_locations [optional] The read table field locations, for use when writing to the table.
    /// \returns The schema for a read table.
    std::shared_ptr<arrow::Schema> make_schema(
            std::shared_ptr<const arrow::KeyValueMetadata> const& metadata) const;

    // V0 fields
    Field<0, UuidArray> read_id;
    ListField<1, arrow::ListArray, arrow::UInt64Array> signal;
    Field<2, arrow::DictionaryArray> pore;
    Field<3, arrow::DictionaryArray> calibration;
    Field<4, arrow::UInt32Array> read_number;
    Field<5, arrow::UInt64Array> start;
    Field<6, arrow::FloatArray> median_before;
    Field<7, arrow::DictionaryArray> end_reason;
    Field<8, arrow::DictionaryArray> run_info;

    // V1 fields
    Field<9, arrow::UInt64Array> num_minknow_events;
    Field<10, arrow::FloatArray> tracked_scaling_scale;
    Field<11, arrow::FloatArray> tracked_scaling_shift;
    Field<12, arrow::FloatArray> predicted_scaling_scale;
    Field<13, arrow::FloatArray> predicted_scaling_shift;
    Field<15, arrow::UInt32Array> num_reads_since_mux_change;
    Field<14, arrow::FloatArray> time_since_mux_change;

    using FieldBuilders = FieldBuilder<
            // V0 fields
            decltype(read_id),
            decltype(signal),
            decltype(pore),
            decltype(calibration),
            decltype(read_number),
            decltype(start),
            decltype(median_before),
            decltype(end_reason),
            decltype(run_info),

            // V1 fields
            decltype(num_minknow_events),
            decltype(tracked_scaling_scale),
            decltype(tracked_scaling_shift),
            decltype(predicted_scaling_scale),
            decltype(predicted_scaling_shift),
            decltype(num_reads_since_mux_change),
            decltype(time_since_mux_change)>;

    PoreStructSchemaDescription pore_fields;
    CalibrationStructSchemaDescription calibration_fields;
    EndReasonStructSchemaDescription end_reason_fields;
    RunInfoStructSchemaDescription run_info_fields;

    ReadTableSpecVersion table_spec_version = ReadTableSpecVersion::TableLatestVersion;
};

POD5_FORMAT_EXPORT std::shared_ptr<arrow::StructType> make_pore_struct_type();
POD5_FORMAT_EXPORT std::shared_ptr<arrow::StructType> make_calibration_struct_type();
POD5_FORMAT_EXPORT std::shared_ptr<arrow::StructType> make_end_reason_struct_type();
POD5_FORMAT_EXPORT std::shared_ptr<arrow::StructType> make_run_info_struct_type();

POD5_FORMAT_EXPORT Result<std::shared_ptr<ReadTableSchemaDescription const>> read_read_table_schema(
        SchemaMetadataDescription const& schema_metadata,
        std::shared_ptr<arrow::Schema> const&);

POD5_FORMAT_EXPORT Result<PoreStructSchemaDescription> read_pore_struct_schema(
        std::shared_ptr<arrow::StructType> const&);
POD5_FORMAT_EXPORT Result<CalibrationStructSchemaDescription> read_calibration_struct_schema(
        std::shared_ptr<arrow::StructType> const&);
POD5_FORMAT_EXPORT Result<EndReasonStructSchemaDescription> read_end_reason_struct_schema(
        std::shared_ptr<arrow::StructType> const&);
POD5_FORMAT_EXPORT Result<RunInfoStructSchemaDescription> read_run_info_struct_schema(
        std::shared_ptr<arrow::StructType> const&);

}  // namespace pod5