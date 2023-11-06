#pragma once

#include "pod5_format/expandable_buffer.h"
#include "pod5_format/signal_compression.h"
#include "pod5_format/signal_table_utils.h"
#include "pod5_format/types.h"

#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/util.h>
#include <boost/variant.hpp>

namespace pod5 {

struct UncompressedSignalBuilder {
    std::shared_ptr<arrow::Int16Builder> signal_data_builder;
    std::unique_ptr<arrow::LargeListBuilder> signal_builder;
};

struct VbzSignalBuilder {
    ExpandableBuffer<std::int64_t> offset_values;
    ExpandableBuffer<std::uint8_t> data_values;
};

using SignalBuilderVariant = boost::variant<UncompressedSignalBuilder, VbzSignalBuilder>;

inline arrow::Result<SignalBuilderVariant> make_signal_builder(
    SignalType compression_type,
    arrow::MemoryPool * pool)
{
    if (compression_type == SignalType::UncompressedSignal) {
        auto signal_array_builder = std::make_shared<arrow::Int16Builder>(pool);
        return UncompressedSignalBuilder{
            signal_array_builder,
            std::make_unique<arrow::LargeListBuilder>(pool, signal_array_builder),
        };
    } else {
        VbzSignalBuilder vbz_builder;
        ARROW_RETURN_NOT_OK(vbz_builder.offset_values.init_buffer(pool));
        ARROW_RETURN_NOT_OK(vbz_builder.data_values.init_buffer(pool));
        return vbz_builder;
    }
}

namespace visitors {
class reserve_rows : boost::static_visitor<Status> {
public:
    reserve_rows(std::size_t row_count, std::size_t approx_read_samples)
    : m_row_count(row_count)
    , m_approx_read_samples(approx_read_samples)
    {
    }

    Status operator()(UncompressedSignalBuilder & builder) const
    {
        ARROW_RETURN_NOT_OK(builder.signal_builder->Reserve(m_row_count));
        return builder.signal_data_builder->Reserve(m_row_count * m_approx_read_samples);
    }

    Status operator()(VbzSignalBuilder & builder) const
    {
        ARROW_RETURN_NOT_OK(builder.offset_values.reserve(m_row_count + 1));
        return builder.data_values.reserve(m_row_count * m_approx_read_samples);
    }

    std::size_t m_row_count;
    std::size_t m_approx_read_samples;
};

class append_pre_compressed_signal : boost::static_visitor<Status> {
public:
    append_pre_compressed_signal(gsl::span<std::uint8_t const> const & signal) : m_signal(signal) {}

    Status operator()(UncompressedSignalBuilder & builder) const
    {
        ARROW_RETURN_NOT_OK(builder.signal_builder->Append());  // start new slot

        auto as_uncompressed = m_signal.as_span<std::int16_t const>();
        return builder.signal_data_builder->AppendValues(
            as_uncompressed.data(), as_uncompressed.size());
    }

    Status operator()(VbzSignalBuilder & builder) const
    {
        ARROW_RETURN_NOT_OK(builder.offset_values.append(builder.data_values.size()));
        return builder.data_values.append_array(m_signal);
    }

    gsl::span<std::uint8_t const> m_signal;
};

class append_signal : boost::static_visitor<Status> {
public:
    append_signal(gsl::span<std::int16_t const> const & signal, arrow::MemoryPool * pool)
    : m_signal(signal)
    , m_pool(pool)
    {
    }

    Status operator()(UncompressedSignalBuilder & builder) const
    {
        ARROW_RETURN_NOT_OK(builder.signal_builder->Append());  // start new slot
        return builder.signal_data_builder->AppendValues(m_signal.data(), m_signal.size());
    }

    Status operator()(VbzSignalBuilder & builder) const
    {
        ARROW_ASSIGN_OR_RAISE(auto compressed_signal, compress_signal(m_signal, m_pool));

        ARROW_RETURN_NOT_OK(builder.offset_values.append(builder.data_values.size()));
        return builder.data_values.append_array(
            gsl::make_span(compressed_signal->data(), compressed_signal->size()));
    }

    gsl::span<std::int16_t const> m_signal;
    arrow::MemoryPool * m_pool;
};

class finish_column : boost::static_visitor<Status> {
public:
    finish_column(std::shared_ptr<arrow::Array> * dest) : m_dest(dest) {}

    Status operator()(UncompressedSignalBuilder & builder) const
    {
        return builder.signal_builder->Finish(m_dest);
    }

    Status operator()(VbzSignalBuilder & builder) const
    {
        auto offsets_copy = builder.offset_values;
        ARROW_RETURN_NOT_OK(builder.offset_values.clear());

        auto const value_data = builder.data_values.get_buffer();
        ARROW_RETURN_NOT_OK(builder.data_values.clear());

        auto const length = offsets_copy.size();

        // Write final offset (values length)
        ARROW_RETURN_NOT_OK(offsets_copy.append(value_data->size()));
        auto const offsets = offsets_copy.get_buffer();

        std::shared_ptr<arrow::Buffer> null_bitmap;

        *m_dest = arrow::MakeArray(
            arrow::ArrayData::Make(vbz_signal(), length, {null_bitmap, offsets, value_data}, 0, 0));

        return arrow::Status::OK();
    }

    std::shared_ptr<arrow::Array> * m_dest;
};

}  // namespace visitors
}  // namespace pod5
