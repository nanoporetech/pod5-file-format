#pragma once

#include "pod5_format/schema_metadata.h"

#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace pod5 {

inline arrow::Result<int> find_field_untyped(
    std::shared_ptr<arrow::Schema> const & schema,
    char const * name)
{
    auto const field_idx = schema->GetFieldIndex(name);
    if (field_idx == -1) {
        return Status::TypeError("Schema missing field '", name, "'");
    }

    return field_idx;
}

inline arrow::Result<int> find_field(
    std::shared_ptr<arrow::Schema> const & schema,
    char const * name,
    std::shared_ptr<arrow::DataType> const & expected_data_type)
{
    ARROW_ASSIGN_OR_RAISE(auto field_idx, find_field_untyped(schema, name));

    auto const field = schema->field(field_idx);
    auto const type = field->type();

    if (!type->Equals(expected_data_type)) {
        return Status::TypeError(
            "Schema field '", name, "' is incorrect type: '", type->name(), "'");
    }

    return field_idx;
}

template <typename ValueType>
inline arrow::Result<int> find_dict_field(
    std::shared_ptr<arrow::Schema> const & schema,
    char const * name,
    std::shared_ptr<arrow::DataType> const & index_type,
    std::shared_ptr<ValueType> * value_type)
{
    ARROW_ASSIGN_OR_RAISE(auto field_idx, find_field_untyped(schema, name));

    auto const field = schema->field(field_idx);
    auto const type = std::dynamic_pointer_cast<arrow::DictionaryType>(field->type());
    if (!type) {
        return Status::TypeError("Dictionary field was unexpected type: ", field->type()->name());
    }

    if (!type->index_type()->Equals(index_type)) {
        return Status::TypeError(
            "Schema field '", name, "' is incorrect type: '", type->name(), "'");
    }

    *value_type = std::dynamic_pointer_cast<ValueType>(type->value_type());
    if (!*value_type) {
        return Status::TypeError(
            "Dictionary value was unexpected type: ", type->value_type()->name());
    }
    return field_idx;
}

template <typename FieldType>
std::shared_ptr<typename FieldType::ArrayType> find_column(
    std::shared_ptr<arrow::RecordBatch> const & batch,
    FieldType const & field)
{
    auto const field_base = batch->column(field.field_index());
    return std::static_pointer_cast<typename FieldType::ArrayType>(field_base);
}

class FieldBase;

enum class SpecialFieldValues : int {
    InvalidField = -1,
};

class TableSpecVersion {
public:
    using UnderlyingType = std::uint8_t;

    TableSpecVersion() : m_version(std::numeric_limits<UnderlyingType>::max()) {}

    static TableSpecVersion first_version() { return TableSpecVersion(0); }

    static TableSpecVersion unknown_version() { return TableSpecVersion(); }

    static TableSpecVersion at_version(UnderlyingType version) { return TableSpecVersion(version); }

    UnderlyingType as_int() const { return m_version; }

    bool operator<(TableSpecVersion const & other) const { return m_version < other.m_version; }

    bool operator>(TableSpecVersion const & other) const { return m_version > other.m_version; }

    bool operator<=(TableSpecVersion const & other) const { return m_version <= other.m_version; }

    bool operator>=(TableSpecVersion const & other) const { return m_version >= other.m_version; }

private:
    TableSpecVersion(UnderlyingType version) : m_version(version) {}

    UnderlyingType m_version;
};

class SchemaDescriptionBase {
public:
    SchemaDescriptionBase(TableSpecVersion version) : m_table_spec_version(version) {}

    virtual ~SchemaDescriptionBase() = default;

    void add_field(FieldBase * field) { m_fields.push_back(field); }

    std::vector<FieldBase *> const & fields() { return m_fields; }

    std::vector<FieldBase const *> const & fields() const
    {
        return reinterpret_cast<std::vector<FieldBase const *> const &>(m_fields);
    }

    TableSpecVersion latest_table_version() const
    {
        return table_version_from_file_version(current_build_version_number());
    }

    virtual TableSpecVersion table_version_from_file_version(Version file_version) const = 0;

    TableSpecVersion table_version() const { return m_table_spec_version; }

    /// \brief Make a new schema for a read table to be written (will only contain fields which are written in the latest version).
    /// \param metadata Metadata to be applied to the schema.
    /// \returns The schema for a read table.
    std::shared_ptr<arrow::Schema> make_writer_schema(
        std::shared_ptr<const arrow::KeyValueMetadata> const & metadata) const;

    static Status read_schema(
        std::shared_ptr<SchemaDescriptionBase> dest_schema,
        SchemaMetadataDescription const & schema_metadata,
        std::shared_ptr<arrow::Schema> const & schema);

private:
    std::vector<FieldBase *> m_fields;
    TableSpecVersion m_table_spec_version;
};

namespace detail {
template <typename ArrayType>
class BuilderHelper;
template <typename ArrayType, typename ElementArrayType>
class ListBuilderHelper;
}  // namespace detail

class FieldBase {
public:
    FieldBase(
        SchemaDescriptionBase * owner,
        int field_index,
        std::string name,
        std::shared_ptr<arrow::DataType> const & datatype,
        TableSpecVersion added_table_spec_version = TableSpecVersion::first_version(),
        TableSpecVersion removed_table_spec_version = TableSpecVersion::unknown_version())
    : m_name(name)
    , m_datatype(datatype)
    , m_field_index(field_index)
    , m_added_table_spec_version(added_table_spec_version)
    , m_removed_table_spec_version(removed_table_spec_version)
    {
        owner->add_field(this);
    }

    std::string const & name() const { return m_name; }

    std::shared_ptr<arrow::DataType> const & datatype() const { return m_datatype; }

    int field_index() const { return m_field_index; }

    TableSpecVersion added_table_spec_version() const { return m_added_table_spec_version; }

    TableSpecVersion removed_table_spec_version() const { return m_removed_table_spec_version; }

    void set_field_index(int index) { m_field_index = index; }

    bool found_field() const { return m_field_index != (int)SpecialFieldValues::InvalidField; }

private:
    std::string m_name;
    std::shared_ptr<arrow::DataType> m_datatype;
    int m_field_index = (int)SpecialFieldValues::InvalidField;
    TableSpecVersion m_added_table_spec_version;
    TableSpecVersion m_removed_table_spec_version;
};

template <int WriteIndex_, typename ArrayType_>
struct Field : public FieldBase {
    using WriteIndex = std::integral_constant<int, WriteIndex_>;
    using ArrayType = ArrayType_;
    using BuilderType = detail::BuilderHelper<ArrayType>;

    Field(
        SchemaDescriptionBase * owner,
        std::string name,
        std::shared_ptr<arrow::DataType> const & datatype,
        TableSpecVersion added_table_spec_version = TableSpecVersion::first_version(),
        TableSpecVersion removed_table_spec_version = TableSpecVersion::unknown_version())
    : FieldBase(
        owner,
        WriteIndex::value,
        name,
        datatype,
        added_table_spec_version,
        removed_table_spec_version)
    {
    }
};

template <int WriteIndex_, typename ArrayType_, typename ElementType_>
struct ListField : public Field<WriteIndex_, ArrayType_> {
    using ElementType = ElementType_;
    using BuilderType = detail::ListBuilderHelper<ArrayType_, ElementType>;

    ListField(
        SchemaDescriptionBase * owner,
        std::string name,
        std::shared_ptr<arrow::DataType> const & datatype,
        TableSpecVersion added_table_spec_version = TableSpecVersion::first_version(),
        TableSpecVersion removed_table_spec_version = TableSpecVersion::unknown_version())
    : Field<WriteIndex_, ArrayType_>(
        owner,
        name,
        datatype,
        added_table_spec_version,
        removed_table_spec_version)
    {
    }
};

template <typename... Args>
class FieldBuilder;

}  // namespace pod5
