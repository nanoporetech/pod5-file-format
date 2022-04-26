#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/read_table_schema.h"
#include "mkr_format/read_table_writer_utils.h"
#include "mkr_format/result.h"

#include <arrow/array/builder_dict.h>
#include <arrow/io/type_fwd.h>
#include <boost/variant/variant.hpp>

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

class MKR_FORMAT_EXPORT ReadTableWriter {
public:
    ReadTableWriter(std::shared_ptr<arrow::ipc::RecordBatchWriter>&& writer,
                    std::shared_ptr<arrow::Schema>&& schema,
                    ReadTableSchemaDescription const& field_locations,
                    std::shared_ptr<PoreWriter> const& pore_writer,
                    std::shared_ptr<CalibrationWriter> const& calibration_writer,
                    std::shared_ptr<EndReasonWriter> const& end_reason_writer,
                    std::shared_ptr<RunInfoWriter> const& run_info_writer,
                    arrow::MemoryPool* pool);
    ReadTableWriter(ReadTableWriter&&);
    ReadTableWriter& operator=(ReadTableWriter&&);
    ReadTableWriter(ReadTableWriter const&) = delete;
    ReadTableWriter& operator=(ReadTableWriter const&) = delete;
    ~ReadTableWriter();

    /// \brief Add a read to the read table, adding to the current batch.
    ///        The batch is not flushed to disk until #flush is called.
    /// \param read_data The data to add as a read.
    /// \param signal List of signal table row indices that belong to this read.
    /// \returns The row index of the inserted read, or a status on failure.
    Result<std::size_t> add_read(ReadData const& read_data,
                                 gsl::span<SignalTableRowIndex const> const& signal);

    /// \brief Flush buffered data into the writer as a record batch.
    Status flush();

    /// \brief Close this writer, signaling no further data will be written to the writer.
    Status close();

private:
    arrow::MemoryPool* m_pool = nullptr;
    std::shared_ptr<arrow::Schema> m_schema;
    ReadTableSchemaDescription m_field_locations;

    std::shared_ptr<arrow::ipc::RecordBatchWriter> m_writer;

    std::unique_ptr<arrow::FixedSizeBinaryBuilder> m_read_id_builder;

    std::shared_ptr<arrow::UInt64Builder> m_signal_array_builder;
    std::unique_ptr<arrow::ListBuilder> m_signal_builder;

    std::unique_ptr<arrow::UInt32Builder> m_read_number_builder;
    std::unique_ptr<arrow::UInt64Builder> m_start_sample_builder;
    std::unique_ptr<arrow::FloatBuilder> m_median_before_builder;

    std::unique_ptr<arrow::Int16Builder> m_pore_builder;
    std::unique_ptr<arrow::Int16Builder> m_calibration_builder;
    std::unique_ptr<arrow::Int16Builder> m_end_reason_builder;
    std::unique_ptr<arrow::Int16Builder> m_run_info_builder;

    std::shared_ptr<PoreWriter> m_pore_writer;
    std::shared_ptr<CalibrationWriter> m_calibration_writer;
    std::shared_ptr<EndReasonWriter> m_end_reason_writer;
    std::shared_ptr<RunInfoWriter> m_run_info_writer;

    std::size_t m_flushed_row_count = 0;
    std::size_t m_current_batch_row_count = 0;
};

/// \brief Make a new writer for a read table.
/// \param sink Sink to be used for output of the table.
/// \param metadata Metadata to be applied to the table schema.
/// \param pool Pool to be used for building table in memory.
/// \returns The writer for the new table.
Result<ReadTableWriter> make_read_table_writer(
        std::shared_ptr<arrow::io::OutputStream> const& sink,
        std::shared_ptr<const arrow::KeyValueMetadata> const& metadata,
        std::shared_ptr<PoreWriter> const& pore_writer,
        std::shared_ptr<CalibrationWriter> const& calibration_writer,
        std::shared_ptr<EndReasonWriter> const& end_reason_writer,
        std::shared_ptr<RunInfoWriter> const& run_info_writer,
        arrow::MemoryPool* pool);

}  // namespace mkr