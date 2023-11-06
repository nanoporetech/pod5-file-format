#include "pod5_format/signal_table_writer.h"

#include "pod5_format/errors.h"
#include "pod5_format/internal/tracing/tracing.h"
#include "pod5_format/types.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/util.h>
#include <arrow/extension_type.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace pod5 {

SignalTableWriter::SignalTableWriter(
    std::shared_ptr<arrow::ipc::RecordBatchWriter> && writer,
    std::shared_ptr<arrow::Schema> && schema,
    SignalBuilderVariant && signal_builder,
    SignalTableSchemaDescription const & field_locations,
    std::shared_ptr<arrow::io::OutputStream> const & output_stream,
    std::size_t table_batch_size,
    arrow::MemoryPool * pool)
: m_pool(pool)
, m_schema(schema)
, m_field_locations(field_locations)
, m_output_stream{output_stream}
, m_table_batch_size(table_batch_size)
, m_writer(std::move(writer))
, m_signal_builder(std::move(signal_builder))
{
    m_read_id_builder = make_read_id_builder(m_pool);
    m_samples_builder = std::make_unique<arrow::UInt32Builder>(m_pool);
}

SignalTableWriter::SignalTableWriter(SignalTableWriter && other) = default;
SignalTableWriter & SignalTableWriter::operator=(SignalTableWriter &&) = default;

SignalTableWriter::~SignalTableWriter()
{
    if (m_writer) {
        (void)close();
    }
}

Result<SignalTableRowIndex> SignalTableWriter::add_signal(
    boost::uuids::uuid const & read_id,
    gsl::span<std::int16_t const> const & signal)
{
    POD5_TRACE_FUNCTION();
    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    auto row_id = m_written_batched_row_count + m_current_batch_row_count;
    ARROW_RETURN_NOT_OK(m_read_id_builder->Append(read_id.begin()));

    ARROW_RETURN_NOT_OK(
        boost::apply_visitor(visitors::append_signal{signal, m_pool}, m_signal_builder));

    ARROW_RETURN_NOT_OK(m_samples_builder->Append(signal.size()));
    ++m_current_batch_row_count;

    if (m_current_batch_row_count >= m_table_batch_size) {
        ARROW_RETURN_NOT_OK(write_batch());
    }

    return row_id;
}

Result<SignalTableRowIndex> SignalTableWriter::add_pre_compressed_signal(
    boost::uuids::uuid const & read_id,
    gsl::span<std::uint8_t const> const & signal,
    std::uint32_t sample_count)
{
    POD5_TRACE_FUNCTION();
    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    auto row_id = m_written_batched_row_count + m_current_batch_row_count;
    ARROW_RETURN_NOT_OK(m_read_id_builder->Append(read_id.begin()));

    ARROW_RETURN_NOT_OK(
        boost::apply_visitor(visitors::append_pre_compressed_signal{signal}, m_signal_builder));

    ARROW_RETURN_NOT_OK(m_samples_builder->Append(sample_count));
    ++m_current_batch_row_count;

    if (m_current_batch_row_count >= m_table_batch_size) {
        ARROW_RETURN_NOT_OK(write_batch());
    }

    return row_id;
}

pod5::Result<std::pair<SignalTableRowIndex, SignalTableRowIndex>>
SignalTableWriter::add_signal_batch(
    std::size_t row_count,
    std::vector<std::shared_ptr<arrow::Array>> && columns,
    bool final_batch)
{
    POD5_TRACE_FUNCTION();
    if (!m_writer) {
        return Status::Invalid("Unable to write batches, writer is closed.");
    }

    if (m_current_batch_row_count != 0) {
        return Status::Invalid("Unable to write batches directly and using per read methods");
    }

    if (!final_batch && row_count != m_table_batch_size) {
        return Status::Invalid("Unable to write invalid sized signal batch to signal table");
    }

    auto const record_batch = arrow::RecordBatch::Make(m_schema, row_count, std::move(columns));
    ARROW_RETURN_NOT_OK(m_writer->WriteRecordBatch(*record_batch));
    if (final_batch) {
        ARROW_RETURN_NOT_OK(close());
    }

    auto first_row_id = m_written_batched_row_count;
    m_written_batched_row_count += row_count;
    return std::make_pair(first_row_id, m_written_batched_row_count);
}

Status SignalTableWriter::close()
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

SignalType SignalTableWriter::signal_type() const { return m_field_locations.signal_type; }

Status SignalTableWriter::write_batch(arrow::RecordBatch const & record_batch)
{
    ARROW_RETURN_NOT_OK(m_writer->WriteRecordBatch(record_batch));
    return m_output_stream->Flush();
}

Status SignalTableWriter::write_batch()
{
    POD5_TRACE_FUNCTION();
    if (m_current_batch_row_count == 0) {
        return Status::OK();
    }

    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    std::vector<std::shared_ptr<arrow::Array>> columns{nullptr, nullptr, nullptr};
    ARROW_RETURN_NOT_OK(m_read_id_builder->Finish(&columns[m_field_locations.read_id]));

    ARROW_RETURN_NOT_OK(boost::apply_visitor(
        visitors::finish_column{&columns[m_field_locations.signal]}, m_signal_builder));

    ARROW_RETURN_NOT_OK(m_samples_builder->Finish(&columns[m_field_locations.samples]));

    auto const record_batch =
        arrow::RecordBatch::Make(m_schema, m_current_batch_row_count, std::move(columns));
    m_written_batched_row_count += m_current_batch_row_count;
    m_current_batch_row_count = 0;

    ARROW_RETURN_NOT_OK(m_writer->WriteRecordBatch(*record_batch));
    ARROW_RETURN_NOT_OK(m_output_stream->Flush());

    // Reserve space for next batch:
    return reserve_rows();
}

Status SignalTableWriter::reserve_rows()
{
    ARROW_RETURN_NOT_OK(m_read_id_builder->Reserve(m_table_batch_size));
    ARROW_RETURN_NOT_OK(m_samples_builder->Reserve(m_table_batch_size));

    static constexpr std::uint32_t APPROX_READ_SIZE = 102'400;

    return boost::apply_visitor(
        visitors::reserve_rows{m_table_batch_size, APPROX_READ_SIZE}, m_signal_builder);
}

Result<SignalTableWriter> make_signal_table_writer(
    std::shared_ptr<arrow::io::OutputStream> const & sink,
    std::shared_ptr<const arrow::KeyValueMetadata> const & metadata,
    std::size_t table_batch_size,
    SignalType compression_type,
    arrow::MemoryPool * pool)
{
    SignalTableSchemaDescription field_locations;
    auto schema = make_signal_table_schema(compression_type, metadata, &field_locations);

    arrow::ipc::IpcWriteOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(sink, schema, options, metadata));

    ARROW_ASSIGN_OR_RAISE(auto signal_builder, make_signal_builder(compression_type, pool));

    auto signal_table_writer = SignalTableWriter(
        std::move(writer),
        std::move(schema),
        std::move(signal_builder),
        field_locations,
        sink,
        table_batch_size,
        pool);

    ARROW_RETURN_NOT_OK(signal_table_writer.reserve_rows());
    return signal_table_writer;
}

}  // namespace pod5
