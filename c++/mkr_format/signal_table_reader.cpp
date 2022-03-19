#include "mkr_format/signal_table_reader.h"

#include "mkr_format/schema_metadata.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

namespace mkr {

SignalTableRecordBatch::SignalTableRecordBatch(std::shared_ptr<arrow::RecordBatch>&& batch,
                                               SignalTableSchemaDescription field_locations) :
        m_batch(batch), m_field_locations(field_locations) {}

SignalTableRecordBatch::SignalTableRecordBatch(SignalTableRecordBatch&&) = default;
SignalTableRecordBatch& SignalTableRecordBatch::operator=(SignalTableRecordBatch&&) = default;
SignalTableRecordBatch::~SignalTableRecordBatch() = default;

std::size_t SignalTableRecordBatch::num_rows() const { return m_batch->num_rows(); }

std::shared_ptr<UuidArray> SignalTableRecordBatch::read_id_column() const {
    return std::static_pointer_cast<UuidArray>(m_batch->column(m_field_locations.read_id));
}

std::shared_ptr<arrow::LargeListArray> SignalTableRecordBatch::signal_column() const {
    return std::static_pointer_cast<arrow::LargeListArray>(
            m_batch->column(m_field_locations.signal));
}

std::shared_ptr<arrow::UInt32Array> SignalTableRecordBatch::samples_column() const {
    return std::static_pointer_cast<arrow::UInt32Array>(m_batch->column(m_field_locations.samples));
}

SignalTableReader::SignalTableReader(std::shared_ptr<void>&& input_source,
                                     std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                                     SignalTableSchemaDescription field_locations,
                                     SchemaMetadataDescription&& schema_metadata,
                                     arrow::MemoryPool* pool) :
        m_pool(pool),
        m_input_source(std::move(input_source)),
        m_reader(std::move(reader)),
        m_field_locations(field_locations),
        m_schema_metadata(std::move(schema_metadata)) {}

SignalTableReader::SignalTableReader(SignalTableReader&&) = default;
SignalTableReader& SignalTableReader::operator=(SignalTableReader&&) = default;
SignalTableReader::~SignalTableReader() = default;

std::size_t SignalTableReader::num_record_batches() const { return m_reader->num_record_batches(); }

Result<SignalTableRecordBatch> SignalTableReader::read_record_batch(std::size_t i) const {
    auto record_batch = m_reader->ReadRecordBatch(i);
    if (!record_batch.ok()) {
        return record_batch.status();
    }
    return SignalTableRecordBatch{std::move(*record_batch), m_field_locations};
}

Result<SignalTableReader> make_signal_table_reader(
        std::shared_ptr<arrow::io::RandomAccessFile> const& input,
        arrow::MemoryPool* pool) {
    arrow::ipc::IpcReadOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::ipc::RecordBatchFileReader::Open(input.get(), options));

    auto read_metadata_key_values = reader->schema()->metadata();
    if (!read_metadata_key_values) {
        return Status::IOError("Missing metadata on read signal table schema");
    }
    ARROW_ASSIGN_OR_RAISE(auto read_metadata,
                          read_schema_key_value_metadata(read_metadata_key_values));
    ARROW_ASSIGN_OR_RAISE(auto field_locations, read_signal_table_schema(reader->schema()));

    return SignalTableReader({input}, std::move(reader), field_locations, std::move(read_metadata),
                             pool);
}

}  // namespace mkr