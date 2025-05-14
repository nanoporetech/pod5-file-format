#include "pod5_format/read_table_writer_utils.h"

#include "pod5_format/read_table_schema.h"

#include <arrow/array/array_dict.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>

namespace pod5 {

namespace detail {

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
