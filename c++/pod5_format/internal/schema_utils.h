#pragma once

#include <arrow/ipc/reader.h>
#include <arrow/status.h>
#include <arrow/type.h>

namespace pod5 {

inline arrow::Result<int> find_field_untyped(std::shared_ptr<arrow::Schema> const& schema,
                                             char const* name) {
    auto const field_idx = schema->GetFieldIndex(name);
    if (field_idx == -1) {
        return Status::TypeError("Schema missing field '", name, "'");
    }

    return field_idx;
}

inline arrow::Result<int> find_field(std::shared_ptr<arrow::Schema> const& schema,
                                     char const* name,
                                     std::shared_ptr<arrow::DataType> const& expected_data_type) {
    ARROW_ASSIGN_OR_RAISE(auto field_idx, find_field_untyped(schema, name));

    auto const field = schema->field(field_idx);
    auto const type = field->type();

    if (!type->Equals(expected_data_type)) {
        return Status::TypeError("Schema field '", name, "' is incorrect type: '", type->name(),
                                 "'");
    }

    return field_idx;
}

inline arrow::Result<int> find_dict_struct_field(std::shared_ptr<arrow::Schema> const& schema,
                                                 char const* name,
                                                 std::shared_ptr<arrow::DataType> const& index_type,
                                                 std::shared_ptr<arrow::StructType>* value_type) {
    ARROW_ASSIGN_OR_RAISE(auto field_idx, find_field_untyped(schema, name));

    auto const field = schema->field(field_idx);
    auto const type = std::dynamic_pointer_cast<arrow::DictionaryType>(field->type());
    if (!type) {
        return Status::TypeError("Dictionary field was unexpected type: ", field->type()->name());
    }

    if (!type->index_type()->Equals(index_type)) {
        return Status::TypeError("Schema field '", name, "' is incorrect type: '", type->name(),
                                 "'");
    }

    *value_type = std::dynamic_pointer_cast<arrow::StructType>(type->value_type());
    if (!*value_type) {
        return Status::TypeError("Dictionary Struct value was unexpected type: ",
                                 type->value_type()->name());
    }
    return field_idx;
}

template <typename FieldType>
std::shared_ptr<typename FieldType::ArrayType> find_column(
        std::shared_ptr<arrow::RecordBatch> const& batch,
        FieldType const& field) {
    auto const field_base = batch->column(field.field_index());
    return std::static_pointer_cast<typename FieldType::ArrayType>(field_base);
}

}  // namespace pod5