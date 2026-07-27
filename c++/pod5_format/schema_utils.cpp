#include "pod5_format/schema_utils.h"

namespace pod5 {

/// \brief Make a new schema for a read table to be written (will only contain fields which are written in the latest version).
/// \param metadata Metadata to be applied to the schema.
/// \returns The schema for a read table.
std::shared_ptr<arrow::Schema> SchemaDescriptionBase::make_writer_schema(
    std::shared_ptr<arrow::KeyValueMetadata const> const & metadata) const
{
    auto const latest_version = latest_table_version();
    arrow::FieldVector writer_fields;
    for (auto & field : fields()) {
        if (field->removed_table_spec_version() > latest_version) {
            writer_fields.emplace_back(arrow::field(field->name(), field->datatype()));
        }
    }
    return arrow::schema(writer_fields, metadata);
}

// From the writer version in the schema meta-data establish the version of the schema used
// and then define which fields should be present based on that version. Update dest_schema
// by setting field indices for present fields.
Status SchemaDescriptionBase::read_schema(
    std::shared_ptr<SchemaDescriptionBase> dest_schema,
    std::shared_ptr<arrow::Schema> const & schema)
{
    // Infer the table version number from the field with the latest added
    // version by advancing the version every time there is a field from a newer
    // schema with matching type.
    dest_schema->m_table_spec_version = TableSpecVersion::first_version();
    for (auto & field : dest_schema->fields()) {
        if (auto field_index = schema->GetFieldIndex(field->name());
            field_index != -1
            && dest_schema->m_table_spec_version < field->added_table_spec_version()
            && schema->field(field_index)->type() == field->datatype())
        {
            dest_schema->m_table_spec_version = field->added_table_spec_version();
        }
    }

    for (auto & field : dest_schema->fields()) {
        if (dest_schema->table_version() < field->added_table_spec_version()
            || dest_schema->table_version() >= field->removed_table_spec_version())
        {
            continue;
        }

        auto const & datatype = field->datatype();
        int field_index = 0;
        if (datatype->id() == arrow::Type::DICTIONARY) {
            auto const & dict_type = static_cast<arrow::DictionaryType const &>(*datatype);
            if (dict_type.value_type()->id() == arrow::Type::STRUCT) {
                std::shared_ptr<arrow::StructType> value_type;
                ARROW_ASSIGN_OR_RAISE(
                    field_index,
                    find_dict_field(schema, field->name().c_str(), arrow::int16(), &value_type));
            } else {
                std::shared_ptr<arrow::StringType> value_type;
                ARROW_ASSIGN_OR_RAISE(
                    field_index,
                    find_dict_field(schema, field->name().c_str(), arrow::int16(), &value_type));
            }
        } else {
            ARROW_ASSIGN_OR_RAISE(field_index, find_field(schema, field->name().c_str(), datatype));
        }
        field->set_field_index(field_index);
    }

    return arrow::Status::OK();
}

}  // namespace pod5
