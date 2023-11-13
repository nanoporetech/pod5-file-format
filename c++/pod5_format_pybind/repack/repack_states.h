#pragma once

#include "pod5_format/file_reader.h"
#include "pod5_format/signal_builder.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <boost/asio/io_context.hpp>
#include <boost/variant.hpp>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace repack { namespace states {

class unread_read_table_rows {
public:
    unread_read_table_rows(
        std::shared_ptr<pod5::FileReader> const & _input,
        std::size_t _batch_index,
        std::vector<std::uint32_t> && _batch_rows)
    : input(_input)
    , batch_index(_batch_index)
    , batch_rows(std::move(_batch_rows))
    {
    }

    std::shared_ptr<pod5::FileReader> input;
    std::size_t batch_index;
    std::vector<std::uint32_t> batch_rows;
};

class read_read_table_rows_no_signal {
public:
    std::vector<pod5::ReadData> reads;
    std::vector<std::size_t> signal_durations;
    std::vector<std::size_t> signal_row_sizes;

    std::atomic<std::size_t> written_row_indices{0};
    std::vector<pod5::SignalTableRowIndex> signal_row_indices;
};

class read_split_signal_table_batch_rows {
public:
    struct PatchRecord {
        PatchRecord(
            std::shared_ptr<states::read_read_table_rows_no_signal> dest_read_table,
            std::uint64_t dest_batch_row_index)
        : dest_read_table(dest_read_table)
        , dest_batch_row_index(dest_batch_row_index)
        {
        }

        std::shared_ptr<states::read_read_table_rows_no_signal> dest_read_table;
        std::uint64_t dest_batch_row_index;
    };

    read_split_signal_table_batch_rows(
        pod5::SignalBuilderVariant && signal_builder,
        arrow::MemoryPool * pool)
    : read_id_builder(pod5::make_read_id_builder(pool))
    , signal_builder(std::move(signal_builder))
    , samples_builder(pool)
    {
    }

    std::unique_ptr<arrow::FixedSizeBinaryBuilder> read_id_builder;
    pod5::SignalBuilderVariant signal_builder;
    arrow::UInt32Builder samples_builder;

    std::vector<PatchRecord> patch_rows;
    bool final_batch = false;

    std::size_t row_count() const { return patch_rows.size(); }
};

struct finished {};

using shared_variant = boost::variant<
    std::shared_ptr<unread_read_table_rows>,
    std::shared_ptr<read_split_signal_table_batch_rows>,
    std::shared_ptr<read_read_table_rows_no_signal>,
    std::shared_ptr<finished>>;

}}  // namespace repack::states
