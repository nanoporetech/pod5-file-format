#pragma once

#include "pod5_format/expandable_buffer.h"
#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"
#include "pod5_format/signal_table_schema.h"

#include <arrow/io/type_fwd.h>
#include <boost/uuid/uuid.hpp>
#include <boost/variant/variant.hpp>
#include <gsl/gsl-lite.hpp>

namespace arrow {
class Schema;

namespace io {
class OutputStream;
}

namespace ipc {
class RecordBatchWriter;
}
}  // namespace arrow

namespace pod5 {

struct UncompressedSignalBuilder {
    std::shared_ptr<arrow::Int16Builder> signal_data_builder;
    std::unique_ptr<arrow::LargeListBuilder> signal_builder;
};

struct VbzSignalBuilder {
    ExpandableBuffer<std::int64_t> offset_values;
    ExpandableBuffer<std::uint8_t> data_values;
};

class POD5_FORMAT_EXPORT SignalTableWriter {
public:
    using SignalBuilderVariant = boost::variant<UncompressedSignalBuilder, VbzSignalBuilder>;

    SignalTableWriter(
        std::shared_ptr<arrow::ipc::RecordBatchWriter> && writer,
        std::shared_ptr<arrow::Schema> && schema,
        SignalBuilderVariant && signal_builder,
        SignalTableSchemaDescription const & field_locations,
        std::size_t table_batch_size,
        arrow::MemoryPool * pool);
    SignalTableWriter(SignalTableWriter &&);
    SignalTableWriter & operator=(SignalTableWriter &&);
    SignalTableWriter(SignalTableWriter const &) = delete;
    SignalTableWriter & operator=(SignalTableWriter const &) = delete;
    ~SignalTableWriter();

    /// \brief Add a read to the signal table, adding to the current batch.
    /// \param read_id The read id for the read entry
    /// \param signal The signal for the read entry
    /// \returns The row index of the inserted signal, or a status on failure.
    Result<std::size_t> add_signal(
        boost::uuids::uuid const & read_id,
        gsl::span<std::int16_t const> const & signal);

    /// \brief Add a pre-compressed read to the signal table, adding to the current batch.
    ///        The batch is not flushed to disk until #flush is called.
    ///
    ///        The user should call #compress_signal on *this* writer to compress the signal prior
    ///        to calling this method, to ensure the signal is compressed correctly for the table.
    ///
    /// \param read_id The read id for the read entry
    /// \param signal The signal for the read entry
    /// \returns The row index of the inserted signal, or a status on failure.
    Result<std::size_t> add_pre_compressed_signal(
        boost::uuids::uuid const & read_id,
        gsl::span<std::uint8_t const> const & signal,
        std::uint32_t sample_count);

    /// \brief Close this writer, signaling no further data will be written to the writer.
    Status close();

    /// \brief Find the signal type of this writer
    SignalType signal_type() const;

    /// \brief Reserve space for future row writes, called automatically when a flush occurs.
    Status reserve_rows();

    /// \brief Find the schema for the signal table
    std::shared_ptr<arrow::Schema> const & schema() const { return m_schema; }

    /// \brief Flush passed data into the writer as a record batch.
    Status write_batch(arrow::RecordBatch const &);

private:
    /// \brief Flush buffered data into the writer as a record batch.
    Status write_batch();

    arrow::MemoryPool * m_pool = nullptr;
    std::shared_ptr<arrow::Schema> m_schema;
    SignalTableSchemaDescription m_field_locations;
    std::size_t m_table_batch_size;

    std::shared_ptr<arrow::ipc::RecordBatchWriter> m_writer;

    std::unique_ptr<arrow::FixedSizeBinaryBuilder> m_read_id_builder;
    SignalBuilderVariant m_signal_builder;
    std::unique_ptr<arrow::UInt32Builder> m_samples_builder;

    std::size_t m_written_batched_row_count = 0;
    std::size_t m_current_batch_row_count = 0;
};

/// \brief Make a new writer for a signal table.
/// \param sink Sink to be used for output of the table.
/// \param metadata Metadata to be applied to the table schema.
/// \param table_batch_size The size of each batch written for the table.
/// \param pool Pool to be used for building table in memory.
/// \returns The writer for the new table.
POD5_FORMAT_EXPORT Result<SignalTableWriter> make_signal_table_writer(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    std::shared_ptr<const arrow::KeyValueMetadata> const & metadata,
    std::size_t table_batch_size,
    SignalType compression_type,
    arrow::MemoryPool * pool);

}  // namespace pod5
