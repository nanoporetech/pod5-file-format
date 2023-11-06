#pragma once

#include "pod5_format/file_reader.h"

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

class unread_split_signal_table_batch_rows {
public:
    struct RowToRead {
        RowToRead(
            std::shared_ptr<pod5::FileReader> const & _source_file,
            std::shared_ptr<states::read_read_table_rows_no_signal> const & _dest_read_table,
            boost::uuids::uuid _read_id,
            std::uint64_t _source_signal_row_index,
            std::uint64_t _batch_row_index)
        : read_id(_read_id)
        , source_file(_source_file)
        , dest_read_table(_dest_read_table)
        , source_signal_row_index(_source_signal_row_index)
        , batch_row_index(_batch_row_index)
        {
        }

        boost::uuids::uuid read_id;
        std::shared_ptr<pod5::FileReader> source_file;
        std::shared_ptr<states::read_read_table_rows_no_signal> dest_read_table;
        std::uint64_t source_signal_row_index;
        std::uint64_t batch_row_index;
    };

    std::vector<RowToRead> rows;
    bool final_batch = false;
};

struct finished {};

using shared_variant = boost::variant<
    std::shared_ptr<unread_read_table_rows>,
    std::shared_ptr<unread_split_signal_table_batch_rows>,
    std::shared_ptr<read_read_table_rows_no_signal>,
    std::shared_ptr<finished>>;

}}  // namespace repack::states
