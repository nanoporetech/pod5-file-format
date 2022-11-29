#include "pod5_format/schema_utils.h"

namespace pod5 {

/// \brief Make a new schema for a read table to be written (will only contain fields which are written in the latest version).
/// \param metadata Metadata to be applied to the schema.
/// \returns The schema for a read table.
std::shared_ptr<arrow::Schema> SchemaDescriptionBase::make_writer_schema(
    std::shared_ptr<const arrow::KeyValueMetadata> const & metadata) const
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

Status SchemaDescriptionBase::read_schema(
    std::shared_ptr<SchemaDescriptionBase> dest_schema,
    SchemaMetadataDescription const & schema_metadata,
    std::shared_ptr<arrow::Schema> const & schema)
{
    dest_schema->m_table_spec_version =
        dest_schema->table_version_from_file_version(schema_metadata.writing_pod5_version);

    for (auto & field : dest_schema->fields()) {
        if (dest_schema->table_version() < field->added_table_spec_version()
            || dest_schema->table_version() >= field->removed_table_spec_version())
        {
            continue;
        }

        auto const & datatype = field->datatype();
        int field_index = 0;
        if (datatype->id() == arrow::Type::DICTIONARY) {
            auto dict_type = std::static_pointer_cast<arrow::DictionaryType>(datatype);
            if (dict_type->value_type()->id() == arrow::Type::STRUCT) {
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
