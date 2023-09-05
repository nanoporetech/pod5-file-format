#include "pod5_format/read_table_writer.h"

#include "pod5_format/errors.h"
#include "pod5_format/internal/tracing/tracing.h"

#include <arrow/extension_type.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>

namespace pod5 {

ReadTableWriter::ReadTableWriter(
    std::shared_ptr<arrow::ipc::RecordBatchWriter> && writer,
    std::shared_ptr<arrow::Schema> && schema,
    std::shared_ptr<ReadTableSchemaDescription> const & field_locations,
    std::size_t table_batch_size,
    std::shared_ptr<PoreWriter> const & pore_writer,
    std::shared_ptr<EndReasonWriter> const & end_reason_writer,
    std::shared_ptr<RunInfoWriter> const & run_info_writer,
    std::shared_ptr<arrow::io::OutputStream> const & output_stream,
    arrow::MemoryPool * pool)
: m_schema(schema)
, m_field_locations(field_locations)
, m_table_batch_size(table_batch_size)
, m_writer(std::move(writer))
, m_field_builders(m_field_locations, pool)
, m_output_stream{output_stream}
{
    m_field_builders.get_builder(m_field_locations->pore_type).set_dict_writer(pore_writer);
    m_field_builders.get_builder(m_field_locations->end_reason).set_dict_writer(end_reason_writer);
    m_field_builders.get_builder(m_field_locations->run_info).set_dict_writer(run_info_writer);
}

ReadTableWriter::ReadTableWriter(ReadTableWriter && other) = default;
ReadTableWriter & ReadTableWriter::operator=(ReadTableWriter &&) = default;

ReadTableWriter::~ReadTableWriter()
{
    if (m_writer) {
        (void)close();
    }
}

Result<std::size_t> ReadTableWriter::add_read(
    ReadData const & read_data,
    gsl::span<SignalTableRowIndex const> const & signal,
    std::uint64_t signal_duration)
{
    POD5_TRACE_FUNCTION();
    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    auto row_id = m_written_batched_row_count + m_current_batch_row_count;
    ARROW_RETURN_NOT_OK(m_field_builders.append(
        // V0 Fields
        read_data.read_id,
        signal,
        read_data.read_number,
        read_data.start_sample,
        read_data.median_before,

        // V1 Fields
        read_data.num_minknow_events,
        read_data.tracked_scaling_scale,
        read_data.tracked_scaling_shift,
        read_data.predicted_scaling_scale,
        read_data.predicted_scaling_shift,
        read_data.num_reads_since_mux_change,
        read_data.time_since_mux_change,

        // V2 Fields
        signal_duration,

        // V3 Fields
        read_data.channel,
        read_data.well,
        read_data.pore_type,
        read_data.calibration_offset,
        read_data.calibration_scale,
        read_data.end_reason,
        read_data.end_reason_forced,
        read_data.run_info));

    ++m_current_batch_row_count;

    if (m_current_batch_row_count >= m_table_batch_size) {
        ARROW_RETURN_NOT_OK(write_batch());
    }
    return row_id;
}

Status ReadTableWriter::close()
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

Status ReadTableWriter::write_batch(arrow::RecordBatch const & record_batch)
{
    ARROW_RETURN_NOT_OK(m_writer->WriteRecordBatch(record_batch));
    return m_output_stream->Flush();
}

Status ReadTableWriter::write_batch()
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

Status ReadTableWriter::reserve_rows() { return m_field_builders.reserve(m_table_batch_size); }

Result<ReadTableWriter> make_read_table_writer(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    std::shared_ptr<const arrow::KeyValueMetadata> const & metadata,
    std::size_t table_batch_size,
    std::shared_ptr<PoreWriter> const & pore_writer,
    std::shared_ptr<EndReasonWriter> const & end_reason_writer,
    std::shared_ptr<RunInfoWriter> const & run_info_writer,
    arrow::MemoryPool * pool)
{
    auto field_locations = std::make_shared<ReadTableSchemaDescription>();
    auto schema = field_locations->make_writer_schema(metadata);

    arrow::ipc::IpcWriteOptions options;
    options.memory_pool = pool;
    options.emit_dictionary_deltas = true;
    // todo... consider:
    //ARROW_ASSIGN_OR_RAISE(options.codec, arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));

    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(sink, schema, options, metadata));

    auto read_table_writer = ReadTableWriter(
        std::move(writer),
        std::move(schema),
        field_locations,
        table_batch_size,
        pore_writer,
        end_reason_writer,
        run_info_writer,
        sink,
        pool);

    ARROW_RETURN_NOT_OK(read_table_writer.reserve_rows());

    return read_table_writer;
}

}  // namespace pod5
