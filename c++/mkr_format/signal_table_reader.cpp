#include "mkr_format/signal_table_reader.h"

#include "mkr_format/schema_metadata.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

namespace mkr {

SignalTableRecordBatch::SignalTableRecordBatch(std::shared_ptr<arrow::RecordBatch>&& batch,
                                               SignalTableSchemaDescription field_locations)
        : TableRecordBatch(std::move(batch)), m_field_locations(field_locations) {}

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

//---------------------------------------------------------------------------------------------------------------------

SignalTableReader::SignalTableReader(std::shared_ptr<void>&& input_source,
                                     std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                                     SignalTableSchemaDescription field_locations,
                                     SchemaMetadataDescription&& schema_metadata,
                                     arrow::MemoryPool* pool)
        : TableReader(std::move(input_source), std::move(reader), std::move(schema_metadata), pool),
          m_field_locations(field_locations) {}

Result<SignalTableRecordBatch> SignalTableReader::read_record_batch(std::size_t i) const {
    auto record_batch = reader()->ReadRecordBatch(i);
    if (!record_batch.ok()) {
        return record_batch.status();
    }
    return SignalTableRecordBatch{std::move(*record_batch), m_field_locations};
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