#include "mkr_format/read_table_writer_utils.h"

#include "mkr_format/read_table_schema.h"

#include <arrow/array/array_dict.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>

namespace mkr {

namespace detail {

template <typename Type>
arrow::Result<std::shared_ptr<arrow::ArrayData>> get_array_data(
        std::shared_ptr<arrow::DataType> const& type,
        PrimitiveDictionaryKeyBuilder<Type> const& builder,
        std::shared_ptr<arrow::Buffer> const& null_bitmap,
        std::size_t expected_length) {
    arrow::TypedBufferBuilder<Type> buffer_builder;
    auto data_in = builder.get_data();
    ARROW_RETURN_NOT_OK(buffer_builder.Append(data_in.data(), data_in.size()));
    ARROW_ASSIGN_OR_RAISE(auto data, buffer_builder.FinishWithLength(expected_length));

    return arrow::ArrayData::Make(type, expected_length, {null_bitmap, data}, 0);
}

arrow::Result<std::shared_ptr<arrow::ArrayData>> get_array_data(
        std::shared_ptr<arrow::DataType> const& type,
        StringDictionaryKeyBuilder const& builder,
        std::shared_ptr<arrow::Buffer> const& null_bitmap,
        std::size_t expected_length) {
    arrow::TypedBufferBuilder<std::uint8_t> value_builder;
    auto const& str_data = builder.get_string_data();
    ARROW_RETURN_NOT_OK(value_builder.Append(str_data.data(), str_data.size()));

    arrow::TypedBufferBuilder<std::int32_t> offset_builder;
    auto const& offset_data = builder.get_offset_data();
    if (offset_data.size() != expected_length) {
        return Status::Invalid("Invalid size for field in struct");
    }
    ARROW_RETURN_NOT_OK(offset_builder.Append(offset_data.data(), offset_data.size()));
    // Append final offset - size of value data.
    ARROW_RETURN_NOT_OK(offset_builder.Append(str_data.size()));

    std::shared_ptr<arrow::Buffer> offsets, value_data;
    ARROW_RETURN_NOT_OK(offset_builder.Finish(&offsets));
    ARROW_RETURN_NOT_OK(value_builder.Finish(&value_data));

    return arrow::ArrayData::Make(type, expected_length, {null_bitmap, offsets, value_data}, 0, 0);
}

arrow::Result<std::shared_ptr<arrow::ArrayData>> get_array_data(
        std::shared_ptr<arrow::DataType> const& type,
        StringMapDictionaryKeyBuilder const& builder,
        std::shared_ptr<arrow::Buffer> const& null_bitmap,
        std::size_t expected_length) {
    arrow::TypedBufferBuilder<std::int32_t> offset_builder;
    auto const& offset_data = builder.get_offset_data();
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

    // Extract map keys + values into their own array datas:
    auto const& map_type = std::static_pointer_cast<arrow::MapType>(type);
    auto const& key_type = map_type->key_type();
    auto const& item_type = map_type->item_type();

    std::shared_ptr<arrow::Buffer> map_item_null_bitmap;
    ARROW_ASSIGN_OR_RAISE(auto key_data, get_array_data(key_type, builder.key_builder(),
                                                        map_item_null_bitmap, final_item_length));
    ARROW_ASSIGN_OR_RAISE(auto item_data, get_array_data(item_type, builder.value_builder(),
                                                         map_item_null_bitmap, final_item_length));

    // Pack this data out as a struct:
    std::shared_ptr<arrow::ArrayData> items =
            arrow::ArrayData::Make(map_type->value_type(), final_item_length,
                                   {null_bitmap, offsets}, {key_data, item_data}, 0);

    // And add this struct to the map/list as the value data, along with the offsets:
    auto data = arrow::ArrayData::Make(type, expected_length, {null_bitmap, offsets},
                                       {std::move(items)}, 0);

    arrow::MapArray array(data);
    assert(array.length() == offset_data.size());
    assert(array.keys()->length() == builder.key_builder().length());
    assert(array.items()->length() == builder.value_builder().length());

    return data;
}

template <std::size_t CurrentIndex, typename BuilderTuple>
arrow::Status do_struct_array_data_unpack(std::vector<std::shared_ptr<arrow::ArrayData>>& dest,
                                          std::shared_ptr<arrow::StructType> const& type,
                                          std::shared_ptr<arrow::Buffer> const& null_bitmap,
                                          std::size_t expected_length,
                                          BuilderTuple const& builders) {
    static_assert(CurrentIndex >= 0, "Current index must be greater than zero");
    auto const& field_type = type->field(CurrentIndex)->type();

    auto const& builder = std::get<CurrentIndex>(builders);

    ARROW_ASSIGN_OR_RAISE(dest[CurrentIndex],
                          get_array_data(field_type, builder, null_bitmap, expected_length));
    return Status::OK();
}

template <std::size_t CurrentIndex, typename BuilderTuple>
struct UnpackStructArrayData;

template <typename BuilderTuple>
struct UnpackStructArrayData<0, BuilderTuple> {
    static arrow::Status unpack(std::vector<std::shared_ptr<arrow::ArrayData>>& dest,
                                std::shared_ptr<arrow::StructType> const& type,
                                std::shared_ptr<arrow::Buffer> const& null_bitmap,
                                std::size_t expected_length,
                                BuilderTuple const& builders) {
        // Dump the last item:
        return do_struct_array_data_unpack<0>(dest, type, null_bitmap, expected_length, builders);
    }
};

template <std::size_t CurrentIndex, typename BuilderTuple>
struct UnpackStructArrayData {
    static arrow::Status unpack(std::vector<std::shared_ptr<arrow::ArrayData>>& dest,
                                std::shared_ptr<arrow::StructType> const& type,
                                std::shared_ptr<arrow::Buffer> const& null_bitmap,
                                std::size_t expected_length,
                                BuilderTuple const& builders) {
        // Dump this builders first:
        RETURN_NOT_OK(do_struct_array_data_unpack<CurrentIndex>(dest, type, null_bitmap,
                                                                expected_length, builders));
        // Then recursively dump the other builders:
        return UnpackStructArrayData<CurrentIndex - 1, BuilderTuple>::unpack(
                dest, type, null_bitmap, expected_length, builders);
    }
};

template <typename BuilderTuple>
arrow::Result<std::shared_ptr<arrow::StructArray>> get_struct_array(
        std::shared_ptr<arrow::StructType> const& type,
        BuilderTuple const& builders) {
    auto const length = std::get<0>(builders).length();

    // Build null bitmap:
    arrow::TypedBufferBuilder<bool> null_bitmap_builder;
    ARROW_RETURN_NOT_OK(null_bitmap_builder.Append(length, true));
    std::int64_t null_count = 0;
    std::shared_ptr<arrow::Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder.Finish(&null_bitmap));

    if (type->num_fields() != std::tuple_size<BuilderTuple>::value) {
        return Status::Invalid("Invalid builder count for number of struct fields");
    }

    // Extract child data:
    std::vector<std::shared_ptr<arrow::ArrayData>> child_data(std::tuple_size<BuilderTuple>::value);
    using Unpacker = UnpackStructArrayData<std::tuple_size<BuilderTuple>::value - 1, BuilderTuple>;
    RETURN_NOT_OK(Unpacker::unpack(child_data, type, null_bitmap, length, builders));

    // Compile into array data:
    auto array = arrow::ArrayData::Make(type, length, {null_bitmap}, null_count);
    array->child_data = std::move(child_data);
    return std::make_shared<arrow::StructArray>(array);
}

}  // namespace detail

arrow::Result<std::shared_ptr<PoreWriter>> make_pore_writer(arrow::MemoryPool* pool) {
    return std::make_shared<PoreWriter>(pool);
}

arrow::Result<std::shared_ptr<EndReasonWriter>> make_end_reason_writer(arrow::MemoryPool* pool) {
    return std::make_shared<EndReasonWriter>(pool);
}

arrow::Result<std::shared_ptr<CalibrationWriter>> make_calibration_writer(arrow::MemoryPool* pool) {
    return std::make_shared<CalibrationWriter>(pool);
}

arrow::Result<std::shared_ptr<RunInfoWriter>> make_run_info_writer(arrow::MemoryPool* pool) {
    return std::make_shared<RunInfoWriter>(pool);
}

mkr::Result<std::shared_ptr<arrow::Array>> DictionaryWriter::build_dictionary_array(
        std::shared_ptr<arrow::Array> const& indices) {
    ARROW_ASSIGN_OR_RAISE(auto res, get_value_array());
    return arrow::DictionaryArray::FromArrays(indices, res);
}

PoreWriter::PoreWriter(arrow::MemoryPool* pool) { m_type = make_pore_struct_type(); }

mkr::Result<std::shared_ptr<arrow::Array>> PoreWriter::get_value_array() {
    ARROW_ASSIGN_OR_RAISE(auto result, detail::get_struct_array(m_type, m_builder.builders()));
    return result;
}

std::size_t PoreWriter::item_count() { return std::get<0>(m_builder.builders()).length(); }

EndReasonWriter::EndReasonWriter(arrow::MemoryPool* pool) {
    m_type = make_end_reason_struct_type();
}

mkr::Result<std::shared_ptr<arrow::Array>> EndReasonWriter::get_value_array() {
    ARROW_ASSIGN_OR_RAISE(auto result, detail::get_struct_array(m_type, m_builder.builders()));
    return result;
}

std::size_t EndReasonWriter::item_count() { return std::get<0>(m_builder.builders()).length(); }

CalibrationWriter::CalibrationWriter(arrow::MemoryPool* pool) {
    m_type = make_calibration_struct_type();
}

mkr::Result<std::shared_ptr<arrow::Array>> CalibrationWriter::get_value_array() {
    ARROW_ASSIGN_OR_RAISE(auto result, detail::get_struct_array(m_type, m_builder.builders()));
    return result;
}

std::size_t CalibrationWriter::item_count() { return std::get<0>(m_builder.builders()).length(); }

RunInfoWriter::RunInfoWriter(arrow::MemoryPool* pool) { m_type = make_run_info_struct_type(); }

mkr::Result<std::shared_ptr<arrow::Array>> RunInfoWriter::get_value_array() {
    ARROW_ASSIGN_OR_RAISE(auto result, detail::get_struct_array(m_type, m_builder.builders()));
    return result;
}

std::size_t RunInfoWriter::item_count() { return std::get<0>(m_builder.builders()).length(); }

}  // namespace mkr