#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/result.h"
#include "mkr_format/signal_table_schema.h"

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

namespace mkr {

struct UncompressedSignalBuilder {
    std::shared_ptr<arrow::Int16Builder> signal_data_builder;
    std::unique_ptr<arrow::LargeListBuilder> signal_builder;
};

struct VbzSignalBuilder {
    std::shared_ptr<arrow::LargeBinaryBuilder> signal_builder;
};

class MKR_FORMAT_EXPORT SignalTableWriter {
public:
    using SignalBuilderVariant = boost::variant<UncompressedSignalBuilder, VbzSignalBuilder>;

    SignalTableWriter(std::shared_ptr<arrow::ipc::RecordBatchWriter>&& writer,
                      std::shared_ptr<arrow::Schema>&& schema,
                      SignalBuilderVariant&& signal_builder,
                      SignalTableSchemaDescription const& field_locations,
                      arrow::MemoryPool* pool);
    SignalTableWriter(SignalTableWriter&&);
    SignalTableWriter& operator=(SignalTableWriter&&);
    SignalTableWriter(SignalTableWriter const&) = delete;
    SignalTableWriter& operator=(SignalTableWriter const&) = delete;
    ~SignalTableWriter();

    /// \brief Add a read to the signal table, adding to the current batch.
    ///        The batch is not flushed to disk until #flush is called.
    /// \param read_id The read id for the read entry
    /// \param signal The signal for the read entry
    /// \returns The row index of the inserted signal, or a status on failure.
    Result<std::size_t> add_signal(boost::uuids::uuid const& read_id,
                                   gsl::span<std::int16_t const> const& signal);

    /// \brief Add a pre-compressed read to the signal table, adding to the current batch.
    ///        The batch is not flushed to disk until #flush is called.
    ///
    ///        The user should call #compress_signal on *this* writer to compress the signal prior
    ///        to calling this method, to ensure the signal is compressed correctly for the table.
    ///
    /// \param read_id The read id for the read entry
    /// \param signal The signal for the read entry
    /// \returns The row index of the inserted signal, or a status on failure.
    Result<std::size_t> add_pre_compressed_signal(boost::uuids::uuid const& read_id,
                                                  gsl::span<std::uint8_t const> const& signal,
                                                  std::uint32_t sample_count);

    /// \brief Flush buffered data into the writer as a record batch.
    Status flush();

    /// \brief Close this writer, signaling no further data will be written to the writer.
    Status close();

    /// \brief Compress the signal, intended for use with #add_pre_compressed_read
    /// \param signal The input signal to be compressed.
    /// \param[out] compressed_signal The compressed signal
    Status compress_signal(gsl::span<std::int16_t> const& signal,
                           std::vector<std::uint8_t> const& compressed_signal);

private:
    arrow::MemoryPool* m_pool = nullptr;
    std::shared_ptr<arrow::Schema> m_schema;
    SignalTableSchemaDescription m_field_locations;

    std::shared_ptr<arrow::ipc::RecordBatchWriter> m_writer;

    std::unique_ptr<arrow::FixedSizeBinaryBuilder> m_read_id_builder;
    SignalBuilderVariant m_signal_builder;
    std::unique_ptr<arrow::UInt32Builder> m_samples_builder;

    std::size_t m_flushed_row_count = 0;
    std::size_t m_current_batch_row_count = 0;
};

/// \brief Make a new writer for a signal table.
/// \param sink Sink to be used for output of the table.
/// \param metadata Metadata to be applied to the table schema.
/// \param pool Pool to be used for building table in memory.
/// \returns The writer for the new table.
Result<SignalTableWriter> make_signal_table_writer(
        std::shared_ptr<arrow::io::OutputStream> const& sink,
        std::shared_ptr<const arrow::KeyValueMetadata> const& metadata,
        SignalType compression_type,
        arrow::MemoryPool* pool);

}  // namespace mkr