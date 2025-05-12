#pragma once

#include "pod5_format/dictionary_writer.h"
#include "pod5_format/expandable_buffer.h"
#include "pod5_format/pod5_format_export.h"
#include "pod5_format/read_table_utils.h"
#include "pod5_format/result.h"
#include "pod5_format/tuple_utils.h"

#include <arrow/array/array_binary.h>
#include <arrow/io/type_fwd.h>
#include <arrow/util/bit_util.h>
#include <gsl/gsl-lite.hpp>

#include <chrono>
#include <cstdint>
#include <map>

namespace pod5 {

namespace detail {

class StringDictionaryKeyBuilder {
public:
    StringDictionaryKeyBuilder(arrow::MemoryPool * pool = nullptr)
    : m_offset_values(pool)
    , m_string_values(pool)
    {
    }

    arrow::Status init_buffer(arrow::MemoryPool * pool)
    {
        ARROW_RETURN_NOT_OK(m_offset_values.init_buffer(pool));
        return m_string_values.init_buffer(pool);
    }

    arrow::Status append(std::string const & value)
    {
        ARROW_RETURN_NOT_OK(m_offset_values.append(m_string_values.size()));
        return m_string_values.append_array(
            gsl::make_span(value.data(), value.size()).as_span<std::uint8_t const>());
    }

    std::size_t length() const { return m_offset_values.size(); }

    std::shared_ptr<arrow::Buffer> get_string_data() const { return m_string_values.get_buffer(); }

    gsl::span<std::int32_t const> get_typed_offset_data() const
    {
        return m_offset_values.get_data_span();
    }

private:
    ExpandableBuffer<std::int32_t> m_offset_values;
    ExpandableBuffer<std::uint8_t> m_string_values;
};

}  // namespace detail

class POD5_FORMAT_EXPORT PoreWriter : public DictionaryWriter {
public:
    PoreWriter(arrow::MemoryPool * pool);

    pod5::Result<PoreDictionaryIndex> add(std::string const & pore_type)
    {
        auto const index = item_count();

        if (index >= std::size_t(std::numeric_limits<std::int16_t>::max())) {
            return arrow::Status::Invalid(
                "Failed to add pore to dictionary, too many indices in file");
        }

        ARROW_RETURN_NOT_OK(m_builder.append(pore_type));
        return index;
    }

    pod5::Result<std::shared_ptr<arrow::Array>> get_value_array() override;
    std::size_t item_count() override;

private:
    detail::StringDictionaryKeyBuilder m_builder;
};

class POD5_FORMAT_EXPORT EndReasonWriter : public DictionaryWriter {
public:
    EndReasonWriter(std::shared_ptr<arrow::StringArray> const & end_reasons);

    pod5::Result<EndReasonDictionaryIndex> lookup(ReadEndReason end_reason) const
    {
        if (end_reason > ReadEndReason::last_end_reason) {
            return pod5::Status::Invalid("Invalid read end reason requested");
        }
        return EndReasonDictionaryIndex(end_reason);
    }

    pod5::Result<std::shared_ptr<arrow::Array>> get_value_array() override;
    std::size_t item_count() override;

private:
    std::shared_ptr<arrow::StringArray> m_end_reasons;
};

class POD5_FORMAT_EXPORT RunInfoWriter : public DictionaryWriter {
public:
    RunInfoWriter(arrow::MemoryPool * pool);

    pod5::Result<RunInfoDictionaryIndex> add(std::string const & acquisition_id)
    {
        auto const index = item_count();

        if (index >= std::size_t(std::numeric_limits<std::int16_t>::max())) {
            return arrow::Status::Invalid(
                "Failed to add run info to dictionary, too many indices in file");
        }

        ARROW_RETURN_NOT_OK(m_builder.append(acquisition_id));
        return index;
    }

    pod5::Result<std::shared_ptr<arrow::Array>> get_value_array() override;
    std::size_t item_count() override;

private:
    detail::StringDictionaryKeyBuilder m_builder;
};

POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<PoreWriter>> make_pore_writer(
    arrow::MemoryPool * pool);

POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<EndReasonWriter>> make_end_reason_writer(
    arrow::MemoryPool * pool);

POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<RunInfoWriter>> make_run_info_writer(
    arrow::MemoryPool * pool);

}  // namespace pod5
