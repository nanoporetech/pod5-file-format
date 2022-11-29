#include "pod5_format/signal_table_schema.h"

#include "pod5_format/schema_utils.h"
#include "pod5_format/types.h"

#include <arrow/type.h>

namespace pod5 {

std::shared_ptr<arrow::Schema> make_signal_table_schema(
    SignalType signal_type,
    std::shared_ptr<const arrow::KeyValueMetadata> const & metadata,
    SignalTableSchemaDescription * field_locations)
{
    auto const uuid_type = uuid();

    if (field_locations) {
        *field_locations = {};
        field_locations->signal_type = signal_type;
    }

    std::shared_ptr<arrow::DataType> signal_schema_type;
    switch (signal_type) {
    case SignalType::UncompressedSignal:
        signal_schema_type = arrow::large_list(arrow::int16());
        break;
    case SignalType::VbzSignal:
        signal_schema_type = vbz_signal();
        break;
    }

    return arrow::schema(
        {
            arrow::field("read_id", uuid_type),
            arrow::field("signal", signal_schema_type),
            arrow::field("samples", arrow::uint32()),
        },
        metadata);
}

Result<SignalTableSchemaDescription> read_signal_table_schema(
    std::shared_ptr<arrow::Schema> const & schema)
{
    ARROW_ASSIGN_OR_RAISE(auto read_id_field_idx, find_field(schema, "read_id", uuid()));
    ARROW_ASSIGN_OR_RAISE(auto samples_field_idx, find_field(schema, "samples", arrow::uint32()));

    ARROW_ASSIGN_OR_RAISE(auto signal_field_idx, find_field_untyped(schema, "signal"));
    SignalType signal_type = SignalType::UncompressedSignal;
    {
        auto const signal_field = schema->field(signal_field_idx);

        auto const signal_arrow_type = signal_field->type();
        if (signal_arrow_type->id() == arrow::Type::LARGE_LIST) {
            auto const signal_list_field =
                std::static_pointer_cast<arrow::LargeListType>(signal_field->type());
            if (signal_list_field->value_type()->id() != arrow::Type::INT16) {
                return Status::TypeError("Schema field 'signal' list value type is incorrect type");
            }
        } else if (signal_arrow_type->Equals(vbz_signal())) {
            signal_type = SignalType::VbzSignal;
        } else {
            return Status::TypeError(
                "Schema field 'signal' is incorrect type: '", signal_arrow_type->name(), "'");
        }
    }

    return SignalTableSchemaDescription{
        signal_type, read_id_field_idx, signal_field_idx, samples_field_idx};
}

}  // namespace pod5
