#include "pod5_format/run_info_table_writer.h"

#include "pod5_format/errors.h"
#include "pod5_format/internal/tracing/tracing.h"
#include "pod5_format/read_table_utils.h"

#include <arrow/extension_type.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>

namespace pod5 {

RunInfoTableWriter::RunInfoTableWriter(
    std::shared_ptr<arrow::ipc::RecordBatchWriter> && writer,
    std::shared_ptr<arrow::Schema> && schema,
    std::shared_ptr<RunInfoTableSchemaDescription> const & field_locations,
    std::shared_ptr<arrow::io::OutputStream> const & output_stream,
    std::size_t table_batch_size,
    arrow::MemoryPool * pool)
: m_schema(schema)
, m_field_locations(field_locations)
, m_output_stream{output_stream}
, m_table_batch_size(table_batch_size)
, m_writer(std::move(writer))
, m_field_builders(m_field_locations, pool)
{
}

RunInfoTableWriter::RunInfoTableWriter(RunInfoTableWriter && other) = default;
RunInfoTableWriter & RunInfoTableWriter::operator=(RunInfoTableWriter &&) = default;

RunInfoTableWriter::~RunInfoTableWriter()
{
    if (m_writer) {
        (void)close();
    }
}

Result<std::size_t> RunInfoTableWriter::add_run_info(RunInfoData const & run_info_data)
{
    POD5_TRACE_FUNCTION();
    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    auto row_id = m_written_batched_row_count + m_current_batch_row_count;
    ARROW_RETURN_NOT_OK(m_field_builders.append(
        // V0 Fields
        run_info_data.acquisition_id,
        run_info_data.acquisition_start_time,
        run_info_data.adc_max,
        run_info_data.adc_min,
        run_info_data.context_tags,
        run_info_data.experiment_name,
        run_info_data.flow_cell_id,
        run_info_data.flow_cell_product_code,
        run_info_data.protocol_name,
        run_info_data.protocol_run_id,
        run_info_data.protocol_start_time,
        run_info_data.sample_id,
        run_info_data.sample_rate,
        run_info_data.sequencing_kit,
        run_info_data.sequencer_position,
        run_info_data.sequencer_position_type,
        run_info_data.software,
        run_info_data.system_name,
        run_info_data.system_type,
        run_info_data.tracking_id));

    ++m_current_batch_row_count;

    if (m_current_batch_row_count >= m_table_batch_size) {
        ARROW_RETURN_NOT_OK(write_batch());
    }
    return row_id;
}

Status RunInfoTableWriter::close()
{
    // Check for already closed
    if (!m_writer) {
        return Status::OK();
    }

    ARROW_RETURN_NOT_OK(write_batch());
    ARROW_RETURN_NOT_OK(m_writer->Close());
    m_writer = nullptr;
    return Status::OK();
}

Status RunInfoTableWriter::write_batch(arrow::RecordBatch const & record_batch)
{
    ARROW_RETURN_NOT_OK(m_writer->WriteRecordBatch(record_batch));
    return m_output_stream->Flush();
}

Status RunInfoTableWriter::write_batch()
{
    POD5_TRACE_FUNCTION();
    if (m_current_batch_row_count == 0) {
        return Status::OK();
    }

    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    ARROW_ASSIGN_OR_RAISE(auto columns, m_field_builders.finish_columns());

    auto const record_batch =
        arrow::RecordBatch::Make(m_schema, m_current_batch_row_count, std::move(columns));

    m_written_batched_row_count += m_current_batch_row_count;
    m_current_batch_row_count = 0;

    ARROW_RETURN_NOT_OK(m_writer->WriteRecordBatch(*record_batch));
    ARROW_RETURN_NOT_OK(m_output_stream->Flush());

    return reserve_rows();
}

Status RunInfoTableWriter::reserve_rows() { return m_field_builders.reserve(m_table_batch_size); }

Result<RunInfoTableWriter> make_run_info_table_writer(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    std::shared_ptr<const arrow::KeyValueMetadata> const & metadata,
    std::size_t table_batch_size,
    arrow::MemoryPool * pool)
{
    auto field_locations = std::make_shared<RunInfoTableSchemaDescription>();
    auto schema = field_locations->make_writer_schema(metadata);

    arrow::ipc::IpcWriteOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(sink, schema, options, metadata));

    auto run_info_table_writer = RunInfoTableWriter(
        std::move(writer), std::move(schema), field_locations, sink, table_batch_size, pool);

    ARROW_RETURN_NOT_OK(run_info_table_writer.reserve_rows());

    return run_info_table_writer;
}

}  // namespace pod5
