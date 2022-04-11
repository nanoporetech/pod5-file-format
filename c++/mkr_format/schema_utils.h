#pragma once

#include <arrow/status.h>
#include <arrow/type.h>

namespace mkr {

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

}  // namespace mkr