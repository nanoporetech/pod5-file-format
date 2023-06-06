#include "pod5_format/read_table_writer_utils.h"

#include "pod5_format/read_table_schema.h"

#include <arrow/array/array_dict.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>

namespace pod5 {

namespace detail {

template <typename Type>
arrow::Result<std::shared_ptr<arrow::ArrayData>> get_array_data(
    std::shared_ptr<arrow::DataType> const & type,
    PrimitiveDictionaryKeyBuilder<Type> const & builder,
    std::size_t expected_length)
{
    arrow::TypedBufferBuilder<Type> buffer_builder;
    auto data = builder.get_data();
    return arrow::ArrayData::Make(type, expected_length, {nullptr, data}, 0);
}

arrow::Result<std::shared_ptr<arrow::ArrayData>> get_array_data(
    std::shared_ptr<arrow::DataType> const & type,
    StringDictionaryKeyBuilder const & builder,
    std::size_t expected_length)
{
    auto const value_data = builder.get_string_data();
    if (!value_data) {
        return Status::Invalid("Missing array value data for dictionary");
    }

    arrow::TypedBufferBuilder<std::int32_t> offset_builder;
    auto const & offset_data = builder.get_typed_offset_data();
    if (offset_data.size() != expected_length) {
        return Status::Invalid("Invalid size for field in struct");
    }
    ARROW_RETURN_NOT_OK(offset_builder.Append(offset_data.data(), offset_data.size()));
    // Append final offset - size of value data.
    ARROW_RETURN_NOT_OK(offset_builder.Append(value_data->size()));

    std::shared_ptr<arrow::Buffer> offsets;
    ARROW_RETURN_NOT_OK(offset_builder.Finish(&offsets));

    return arrow::ArrayData::Make(type, expected_length, {nullptr, offsets, value_data}, 0, 0);
}

arrow::Result<std::shared_ptr<arrow::ArrayData>> get_array_data(
    std::shared_ptr<arrow::DataType> const & type,
    StringMapDictionaryKeyBuilder const & builder,
    std::size_t expected_length)
{
    arrow::TypedBufferBuilder<std::int32_t> offset_builder;
    auto const & offset_data = builder.get_typed_offset_data();
    if (offset_data.size() != expected_length) {
        return Status::Invalid("Invalid size for field in struct");
    }
    ARROW_RETURN_NOT_OK(offset_builder.Append(offset_data.data(), offset_data.size()));
    // Append final offset - size of value data.
    auto const final_item_length = builder.key_builder().length();
    ARROW_RETURN_NOT_OK(offset_builder.Append(builder.key_builder().length()));

    std::shared_ptr<arrow::Buffer> offsets;
    ARROW_RETURN_NOT_OK(offset_builder.Finish(&offsets));

    if (type->id() != arrow::Type::MAP) {
        return Status::Invalid("Unexpected type for map");
    }

    // Extract map keys + values into their own array data:
    auto const & map_type = std::static_pointer_cast<arrow::MapType>(type);
    auto const & key_type = map_type->key_type();
    auto const & item_type = map_type->item_type();

    ARROW_ASSIGN_OR_RAISE(
        auto key_data, get_array_data(key_type, builder.key_builder(), final_item_length));
    ARROW_ASSIGN_OR_RAISE(
        auto item_data, get_array_data(item_type, builder.value_builder(), final_item_length));

    // Pack this data out as a struct:
    std::shared_ptr<arrow::ArrayData> items = arrow::ArrayData::Make(
        map_type->value_type(), final_item_length, {nullptr, offsets}, {key_data, item_data}, 0);

    // And add this struct to the map/list as the value data, along with the offsets:
    auto data =
        arrow::ArrayData::Make(type, expected_length, {nullptr, offsets}, {std::move(items)}, 0);

    arrow::MapArray array(data);
    assert(array.length() == (std::int64_t)offset_data.size());
    assert(array.keys()->length() == (std::int64_t)builder.key_builder().length());
    assert(array.items()->length() == (std::int64_t)builder.value_builder().length());

    return data;
}

template <std::size_t CurrentIndex, typename BuilderTuple>
arrow::Status do_struct_array_data_unpack(
    std::vector<std::shared_ptr<arrow::ArrayData>> & dest,
    std::shared_ptr<arrow::StructType> const & type,
    std::size_t expected_length,
    BuilderTuple const & builders)
{
    static_assert(CurrentIndex >= 0, "Current index must be greater than zero");
    auto const & field_type = type->field(CurrentIndex)->type();

    auto const & builder = std::get<CurrentIndex>(builders);

    ARROW_ASSIGN_OR_RAISE(dest[CurrentIndex], get_array_data(field_type, builder, expected_length));
    return Status::OK();
}

template <std::size_t CurrentIndex, typename BuilderTuple>
struct UnpackStructArrayData;

template <typename BuilderTuple>
struct UnpackStructArrayData<0, BuilderTuple> {
    static arrow::Status unpack(
        std::vector<std::shared_ptr<arrow::ArrayData>> & dest,
        std::shared_ptr<arrow::StructType> const & type,
        std::size_t expected_length,
        BuilderTuple const & builders)
    {
        // Dump the last item:
        return do_struct_array_data_unpack<0>(dest, type, expected_length, builders);
    }
};

template <std::size_t CurrentIndex, typename BuilderTuple>
struct UnpackStructArrayData {
    static arrow::Status unpack(
        std::vector<std::shared_ptr<arrow::ArrayData>> & dest,
        std::shared_ptr<arrow::StructType> const & type,
        std::size_t expected_length,
        BuilderTuple const & builders)
    {
        // Dump this builders first:
        RETURN_NOT_OK(
            do_struct_array_data_unpack<CurrentIndex>(dest, type, expected_length, builders));
        // Then recursively dump the other builders:
        return UnpackStructArrayData<CurrentIndex - 1, BuilderTuple>::unpack(
            dest, type, expected_length, builders);
    }
};

template <typename BuilderTuple>
arrow::Result<std::shared_ptr<arrow::StructArray>> get_struct_array(
    std::shared_ptr<arrow::StructType> const & type,
    BuilderTuple const & builders)
{
    auto const length = std::get<0>(builders).length();

    std::int64_t null_count = 0;

    if (type->num_fields() != std::tuple_size<BuilderTuple>::value) {
        return Status::Invalid("Invalid builder count for number of struct fields");
    }

    // Extract child data:
    std::vector<std::shared_ptr<arrow::ArrayData>> child_data(std::tuple_size<BuilderTuple>::value);
    using Unpacker = UnpackStructArrayData<std::tuple_size<BuilderTuple>::value - 1, BuilderTuple>;
    RETURN_NOT_OK(Unpacker::unpack(child_data, type, length, builders));

    // Compile into array data:
    auto array = arrow::ArrayData::Make(type, length, {nullptr}, null_count);
    array->child_data = std::move(child_data);
    return std::make_shared<arrow::StructArray>(array);
}

}  // namespace detail

arrow::Result<std::shared_ptr<PoreWriter>> make_pore_writer(arrow::MemoryPool * pool)
{
    return std::make_shared<PoreWriter>(pool);
}

arrow::Result<std::shared_ptr<EndReasonWriter>> make_end_reason_writer(arrow::MemoryPool * pool)
{
    std::shared_ptr<arrow::StringArray> end_reasons;
    {
        arrow::StringBuilder builder(pool);
        for (int end_reason = 0; end_reason <= (int)ReadEndReason::last_end_reason; ++end_reason) {
            ARROW_RETURN_NOT_OK(builder.Append(end_reason_as_string((ReadEndReason)end_reason)));
        }

        ARROW_RETURN_NOT_OK(builder.Finish(&end_reasons));
    }

    return std::make_shared<EndReasonWriter>(end_reasons);
}

arrow::Result<std::shared_ptr<RunInfoWriter>> make_run_info_writer(arrow::MemoryPool * pool)
{
    return std::make_shared<RunInfoWriter>(pool);
}

pod5::Result<std::shared_ptr<arrow::Array>> DictionaryWriter::build_dictionary_array(
    std::shared_ptr<arrow::Array> const & indices)
{
    ARROW_ASSIGN_OR_RAISE(auto res, get_value_array());
    return arrow::DictionaryArray::FromArrays(indices, res);
}

PoreWriter::PoreWriter(arrow::MemoryPool * pool) : m_builder(pool) {}

pod5::Result<std::shared_ptr<arrow::Array>> PoreWriter::get_value_array()
{
    ARROW_ASSIGN_OR_RAISE(auto array_data, get_array_data(arrow::utf8(), m_builder, item_count()));
    return std::make_shared<arrow::StringArray>(array_data);
}

std::size_t PoreWriter::item_count() { return m_builder.length(); }

EndReasonWriter::EndReasonWriter(std::shared_ptr<arrow::StringArray> const & end_reasons)
: m_end_reasons(end_reasons)
{
}

pod5::Result<std::shared_ptr<arrow::Array>> EndReasonWriter::get_value_array()
{
    return m_end_reasons;
}

std::size_t EndReasonWriter::item_count() { return m_end_reasons->length(); }

RunInfoWriter::RunInfoWriter(arrow::MemoryPool * pool) : m_builder(pool) {}

pod5::Result<std::shared_ptr<arrow::Array>> RunInfoWriter::get_value_array()
{
    ARROW_ASSIGN_OR_RAISE(auto array_data, get_array_data(arrow::utf8(), m_builder, item_count()));
    return std::make_shared<arrow::StringArray>(array_data);
}

std::size_t RunInfoWriter::item_count() { return m_builder.length(); }

}  // namespace pod5
