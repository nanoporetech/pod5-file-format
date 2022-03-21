#include "mkr_format/signal_table_writer.h"

#include "mkr_format/errors.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/extension_type.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace mkr {

SignalTableWriter::SignalTableWriter(std::shared_ptr<arrow::ipc::RecordBatchWriter>&& writer,
                                     std::shared_ptr<arrow::Schema>&& schema,
                                     SignalTableSchemaDescription const& field_locations,
                                     arrow::MemoryPool* pool) :
        m_pool(pool),
        m_schema(schema),
        m_field_locations(field_locations),
        m_writer(std::move(writer)) {
    auto uuid_type = m_schema->field(m_field_locations.read_id)->type();
    assert(uuid_type->id() == arrow::Type::EXTENSION);
    auto uuid_extension = static_cast<arrow::ExtensionType*>(uuid_type.get());
    m_read_id_builder =
            std::make_unique<arrow::FixedSizeBinaryBuilder>(uuid_extension->storage_type(), m_pool);
    assert(m_read_id_builder->byte_width() == 16);

    m_signal_data_builder = std::make_shared<arrow::Int16Builder>(m_pool);
    m_signal_builder = std::make_unique<arrow::LargeListBuilder>(m_pool, m_signal_data_builder);

    m_samples_builder = std::make_unique<arrow::UInt32Builder>(m_pool);
}

SignalTableWriter::SignalTableWriter(SignalTableWriter&& other) = default;
SignalTableWriter& SignalTableWriter::operator=(SignalTableWriter&&) = default;
SignalTableWriter::~SignalTableWriter() {
    flush();
    close();
}

Result<std::size_t> SignalTableWriter::add_signal(boost::uuids::uuid const& read_id,
                                                  gsl::span<std::int16_t> const& signal) {
    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    auto row_id = m_flushed_row_count + m_current_batch_row_count;
    ARROW_RETURN_NOT_OK(m_read_id_builder->Append(read_id.begin()));
    ARROW_RETURN_NOT_OK(m_signal_builder->Append());  // start new slot
    ARROW_RETURN_NOT_OK(m_signal_data_builder->AppendValues(signal.data(), signal.size()));
    ARROW_RETURN_NOT_OK(m_samples_builder->Append(signal.size()));
    ++m_current_batch_row_count;
    return row_id;
}

Status SignalTableWriter::flush() {
    if (m_current_batch_row_count == 0) {
        return Status::OK();
    }

    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    std::vector<std::shared_ptr<arrow::Array>> columns{nullptr, nullptr, nullptr};
    ARROW_RETURN_NOT_OK(m_read_id_builder->Finish(&columns[m_field_locations.read_id]));
    ARROW_RETURN_NOT_OK(m_signal_builder->Finish(&columns[m_field_locations.signal]));
    ARROW_RETURN_NOT_OK(m_samples_builder->Finish(&columns[m_field_locations.samples]));

    auto const record_batch =
            arrow::RecordBatch::Make(m_schema, m_current_batch_row_count, std::move(columns));
    m_flushed_row_count += m_current_batch_row_count;
    m_current_batch_row_count = 0;
    ARROW_RETURN_NOT_OK(m_writer->WriteRecordBatch(*record_batch));
    return Status();
}

Status SignalTableWriter::close() {
    // Check for already closed
    if (!m_writer) {
        return Status::OK();
    }

    ARROW_RETURN_NOT_OK(m_writer->Close());
    m_writer = nullptr;
    return Status::OK();
}

Result<SignalTableWriter> make_signal_table_writer(
        std::shared_ptr<arrow::io::OutputStream> const& sink,
        std::shared_ptr<const arrow::KeyValueMetadata> const& metadata,
        arrow::MemoryPool* pool) {
    SignalTableSchemaDescription field_locations;
    auto schema = make_signal_table_schema(metadata, &field_locations);

    arrow::ipc::IpcWriteOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(sink, schema, options, metadata));
    return SignalTableWriter(std::move(writer), std::move(schema), field_locations, pool);
}

}  // namespace mkr