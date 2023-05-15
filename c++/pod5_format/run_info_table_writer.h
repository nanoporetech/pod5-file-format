#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"
#include "pod5_format/run_info_table_schema.h"
#include "pod5_format/schema_field_builder.h"

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

namespace pod5 {

class RunInfoData;

class POD5_FORMAT_EXPORT RunInfoTableWriter {
public:
    RunInfoTableWriter(
        std::shared_ptr<arrow::ipc::RecordBatchWriter> && writer,
        std::shared_ptr<arrow::Schema> && schema,
        std::shared_ptr<RunInfoTableSchemaDescription> const & field_locations,
        std::size_t table_batch_size,
        arrow::MemoryPool * pool);
    RunInfoTableWriter(RunInfoTableWriter &&);
    RunInfoTableWriter & operator=(RunInfoTableWriter &&);
    RunInfoTableWriter(RunInfoTableWriter const &) = delete;
    RunInfoTableWriter & operator=(RunInfoTableWriter const &) = delete;
    ~RunInfoTableWriter();

    /// \brief Add a run info to the table, adding to the current batch.
    /// \param run_info_data The run info data to add.
    /// \returns The row index of the inserted read, or a status on failure.
    Result<std::size_t> add_run_info(RunInfoData const & run_info_data);

    /// \brief Close this writer, signaling no further data will be written to the writer.
    Status close();

    /// \brief Reserve space for future row writes, called automatically when a flush occurs.
    Status reserve_rows();

    /// \brief Find the schema for the table
    std::shared_ptr<arrow::Schema> const & schema() const { return m_schema; }

    /// \brief Flush passed data into the writer as a record batch.
    Status write_batch(arrow::RecordBatch const &);

private:
    /// \brief Flush buffered data into the writer as a record batch.
    Status write_batch();

    std::shared_ptr<arrow::Schema> m_schema;
    std::shared_ptr<RunInfoTableSchemaDescription> m_field_locations;
    std::size_t m_table_batch_size;

    std::shared_ptr<arrow::ipc::RecordBatchWriter> m_writer;

    RunInfoTableSchemaDescription::FieldBuilders m_field_builders;

    std::size_t m_written_batched_row_count = 0;
    std::size_t m_current_batch_row_count = 0;
};

/// \brief Make a new writer for a read table.
/// \param sink Sink to be used for output of the table.
/// \param metadata Metadata to be applied to the table schema.
/// \param table_batch_size The size of each batch written for the table.
/// \param pool Pool to be used for building table in memory.
/// \returns The writer for the new table.
POD5_FORMAT_EXPORT Result<RunInfoTableWriter> make_run_info_table_writer(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    std::shared_ptr<const arrow::KeyValueMetadata> const & metadata,
    std::size_t table_batch_size,
    arrow::MemoryPool * pool);

}  // namespace pod5
