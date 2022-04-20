#include "mkr_format/read_table_reader.h"

#include "mkr_format/schema_metadata.h"

#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

namespace mkr {

ReadTableRecordBatch::ReadTableRecordBatch(std::shared_ptr<arrow::RecordBatch>&& batch,
                                           ReadTableSchemaDescription field_locations)
        : TableRecordBatch(std::move(batch)), m_field_locations(field_locations) {}

std::shared_ptr<UuidArray> ReadTableRecordBatch::read_id_column() const {
    return std::static_pointer_cast<UuidArray>(batch()->column(m_field_locations.read_id));
}
std::shared_ptr<arrow::ListArray> ReadTableRecordBatch::signal_column() const {
    return std::static_pointer_cast<arrow::ListArray>(batch()->column(m_field_locations.signal));
}
std::shared_ptr<arrow::DictionaryArray> ReadTableRecordBatch::pore_column() const {
    return std::static_pointer_cast<arrow::DictionaryArray>(
            batch()->column(m_field_locations.pore));
}
std::shared_ptr<arrow::DictionaryArray> ReadTableRecordBatch::calibration_column() const {
    return std::static_pointer_cast<arrow::DictionaryArray>(
            batch()->column(m_field_locations.calibration));
}
std::shared_ptr<arrow::UInt32Array> ReadTableRecordBatch::read_number_column() const {
    return std::static_pointer_cast<arrow::UInt32Array>(
            batch()->column(m_field_locations.read_number));
}
std::shared_ptr<arrow::UInt64Array> ReadTableRecordBatch::start_sample_column() const {
    return std::static_pointer_cast<arrow::UInt64Array>(
            batch()->column(m_field_locations.start_sample));
}
std::shared_ptr<arrow::FloatArray> ReadTableRecordBatch::median_before_column() const {
    return std::static_pointer_cast<arrow::FloatArray>(
            batch()->column(m_field_locations.median_before));
}
std::shared_ptr<arrow::DictionaryArray> ReadTableRecordBatch::end_reason_column() const {
    return std::static_pointer_cast<arrow::DictionaryArray>(
            batch()->column(m_field_locations.end_reason));
}
std::shared_ptr<arrow::DictionaryArray> ReadTableRecordBatch::run_info_column() const {
    return std::static_pointer_cast<arrow::DictionaryArray>(
            batch()->column(m_field_locations.run_info));
}

//---------------------------------------------------------------------------------------------------------------------

ReadTableReader::ReadTableReader(std::shared_ptr<void>&& input_source,
                                 std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                                 ReadTableSchemaDescription field_locations,
                                 SchemaMetadataDescription&& schema_metadata,
                                 arrow::MemoryPool* pool)
        : TableReader(std::move(input_source), std::move(reader), std::move(schema_metadata), pool),
          m_field_locations(field_locations) {}

Result<ReadTableRecordBatch> ReadTableReader::read_record_batch(std::size_t i) const {
    auto record_batch = reader()->ReadRecordBatch(i);
    if (!record_batch.ok()) {
        return record_batch.status();
    }
    return ReadTableRecordBatch{std::move(*record_batch), m_field_locations};
}

//---------------------------------------------------------------------------------------------------------------------

Result<ReadTableReader> make_read_table_reader(
        std::shared_ptr<arrow::io::RandomAccessFile> const& input,
        arrow::MemoryPool* pool) {
    arrow::ipc::IpcReadOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::ipc::RecordBatchFileReader::Open(input.get(), options));

    auto read_metadata_key_values = reader->schema()->metadata();
    if (!read_metadata_key_values) {
        return Status::IOError("Missing metadata on read table schema");
    }
    ARROW_ASSIGN_OR_RAISE(auto read_metadata,
                          read_schema_key_value_metadata(read_metadata_key_values));
    ARROW_ASSIGN_OR_RAISE(auto field_locations, read_read_table_schema(reader->schema()));

    return ReadTableReader({input}, std::move(reader), field_locations, std::move(read_metadata),
                           pool);
}

}  // namespace mkr