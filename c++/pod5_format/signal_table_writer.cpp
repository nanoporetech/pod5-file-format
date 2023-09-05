#include "pod5_format/signal_table_writer.h"

#include "pod5_format/errors.h"
#include "pod5_format/internal/tracing/tracing.h"
#include "pod5_format/signal_compression.h"
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

namespace { namespace visitors {
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

}}  // namespace ::visitors

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
    auto uuid_type = m_schema->field(m_field_locations.read_id)->type();
    assert(uuid_type->id() == arrow::Type::EXTENSION);
    auto uuid_extension = std::static_pointer_cast<arrow::ExtensionType>(uuid_type);
    m_read_id_builder =
        std::make_unique<arrow::FixedSizeBinaryBuilder>(uuid_extension->storage_type(), m_pool);
    assert(m_read_id_builder->byte_width() == 16);

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

Result<std::size_t> SignalTableWriter::add_signal(
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

Result<std::size_t> SignalTableWriter::add_pre_compressed_signal(
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

    SignalTableWriter::SignalBuilderVariant signal_builder;
    if (compression_type == SignalType::UncompressedSignal) {
        auto signal_array_builder = std::make_shared<arrow::Int16Builder>(pool);
        signal_builder = UncompressedSignalBuilder{
            signal_array_builder,
            std::make_unique<arrow::LargeListBuilder>(pool, signal_array_builder),
        };
    } else {
        VbzSignalBuilder vbz_builder;
        ARROW_RETURN_NOT_OK(vbz_builder.offset_values.init_buffer(pool));
        ARROW_RETURN_NOT_OK(vbz_builder.data_values.init_buffer(pool));
        signal_builder = vbz_builder;
    }

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
