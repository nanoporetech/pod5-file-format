#include "mkr_format/signal_table_reader.h"

#include "mkr_format/schema_metadata.h"
#include "mkr_format/signal_compression.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

namespace mkr {

SignalTableRecordBatch::SignalTableRecordBatch(std::shared_ptr<arrow::RecordBatch>&& batch,
                                               SignalTableSchemaDescription field_locations,
                                               arrow::MemoryPool* pool)
        : TableRecordBatch(std::move(batch)), m_field_locations(field_locations), m_pool(pool) {}

std::shared_ptr<UuidArray> SignalTableRecordBatch::read_id_column() const {
    return std::static_pointer_cast<UuidArray>(batch()->column(m_field_locations.read_id));
}

std::shared_ptr<arrow::LargeListArray> SignalTableRecordBatch::uncompressed_signal_column() const {
    return std::static_pointer_cast<arrow::LargeListArray>(
            batch()->column(m_field_locations.signal));
}

std::shared_ptr<VbzSignalArray> SignalTableRecordBatch::vbz_signal_column() const {
    return std::static_pointer_cast<VbzSignalArray>(batch()->column(m_field_locations.signal));
}

std::shared_ptr<arrow::UInt32Array> SignalTableRecordBatch::samples_column() const {
    return std::static_pointer_cast<arrow::UInt32Array>(batch()->column(m_field_locations.samples));
}

Result<std::size_t> SignalTableRecordBatch::samples_byte_count(std::size_t row_index) const {
    switch (m_field_locations.signal_type) {
    case SignalType::UncompressedSignal: {
        auto signal_column = uncompressed_signal_column();
        auto signal = signal_column->value_slice(row_index);
        return signal->length() * sizeof(std::int16_t);
    }
    case SignalType::VbzSignal: {
        auto signal_column = vbz_signal_column();
        auto signal_compressed = signal_column->Value(row_index);
        return signal_compressed.size();
    }
    }

    return mkr::Status::Invalid("Unknown signal type");
}

Status SignalTableRecordBatch::extract_signal_row(std::size_t row_index,
                                                  gsl::span<std::int16_t> samples) const {
    if (row_index >= num_rows()) {
        return mkr::Status::Invalid("Queried signal row ", row_index,
                                    " is outside the available rows (", num_rows(), "in batch)");
    }

    auto sample_count = samples_column();
    auto samples_in_row = sample_count->Value(row_index);
    if (samples_in_row != samples.size()) {
        return mkr::Status::Invalid("Unexpected size for sample array ", samples.size(),
                                    " expected ", samples_in_row);
    }

    switch (m_field_locations.signal_type) {
    case SignalType::UncompressedSignal: {
        auto signal_column = uncompressed_signal_column();
        auto signal =
                std::static_pointer_cast<arrow::Int16Array>(signal_column->value_slice(row_index));
        std::copy(signal->raw_values(), signal->raw_values() + signal->length(), samples.begin());
        return Status::OK();
    }
    case SignalType::VbzSignal: {
        auto signal_column = vbz_signal_column();
        auto signal_compressed = signal_column->Value(row_index);
        return mkr::decompress_signal(signal_compressed, m_pool, samples);
    }
    }

    return mkr::Status::Invalid("Unknown signal type");
}

//---------------------------------------------------------------------------------------------------------------------

SignalTableReader::SignalTableReader(std::shared_ptr<void>&& input_source,
                                     std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                                     SignalTableSchemaDescription field_locations,
                                     SchemaMetadataDescription&& schema_metadata,
                                     arrow::MemoryPool* pool)
        : TableReader(std::move(input_source), std::move(reader), std::move(schema_metadata), pool),
          m_field_locations(field_locations),
          m_pool(pool) {}

Result<SignalTableRecordBatch> SignalTableReader::read_record_batch(std::size_t i) const {
    if (m_last_batch && m_last_batch->first == i) {
        return m_last_batch->second;
    }

    auto record_batch = reader()->ReadRecordBatch(i);
    if (!record_batch.ok()) {
        return record_batch.status();
    }
    auto batch = SignalTableRecordBatch{std::move(*record_batch), m_field_locations, m_pool};
    m_last_batch.emplace(i, batch);
    return batch;
}

Result<std::size_t> SignalTableReader::signal_batch_for_row_id(std::size_t row,
                                                               std::size_t* batch_start_row) const {
    if (!m_last_batch) {
        ARROW_RETURN_NOT_OK(read_record_batch(0));
    }
    assert(!!m_last_batch);
    auto const& batch_size = m_last_batch->second.num_rows();

    auto batch = row / batch_size;

    if (batch_start_row) {
        *batch_start_row = batch * batch_size;
    }

    if (batch >= num_record_batches()) {
        return Status::Invalid("Row outside batch bounds");
    }

    return batch;
}
//---------------------------------------------------------------------------------------------------------------------

Result<SignalTableReader> make_signal_table_reader(
        std::shared_ptr<arrow::io::RandomAccessFile> const& input,
        arrow::MemoryPool* pool) {
    arrow::ipc::IpcReadOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::ipc::RecordBatchFileReader::Open(input.get(), options));

    auto read_metadata_key_values = reader->schema()->metadata();
    if (!read_metadata_key_values) {
        return Status::IOError("Missing metadata on signal table schema");
    }
    ARROW_ASSIGN_OR_RAISE(auto read_metadata,
                          read_schema_key_value_metadata(read_metadata_key_values));
    ARROW_ASSIGN_OR_RAISE(auto field_locations, read_signal_table_schema(reader->schema()));

    return SignalTableReader({input}, std::move(reader), field_locations, std::move(read_metadata),
                             pool);
}

}  // namespace mkr