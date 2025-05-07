#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/schema_metadata.h"

#include <memory>

namespace arrow {
class MemoryPool;
class RecordBatch;

namespace ipc {
class RecordBatchFileReader;
}
}  // namespace arrow

namespace pod5 {

class POD5_FORMAT_EXPORT TableRecordBatch {
public:
    TableRecordBatch(std::shared_ptr<arrow::RecordBatch> const & batch);
    TableRecordBatch(std::shared_ptr<arrow::RecordBatch> && batch);

    TableRecordBatch(TableRecordBatch &&);
    TableRecordBatch & operator=(TableRecordBatch &&);
    TableRecordBatch(TableRecordBatch const &);
    TableRecordBatch & operator=(TableRecordBatch const &);
    ~TableRecordBatch();

    std::size_t num_rows() const;

    std::shared_ptr<arrow::RecordBatch> const & batch() const { return m_batch; }

private:
    std::shared_ptr<arrow::RecordBatch> m_batch;
};

class POD5_FORMAT_EXPORT TableReader {
public:
    TableReader(
        std::shared_ptr<void> && input_source,
        std::shared_ptr<arrow::ipc::RecordBatchFileReader> && reader,
        SchemaMetadataDescription && schema_metadata,
        arrow::MemoryPool * pool);
    TableReader(TableReader &&);
    TableReader & operator=(TableReader &&);
    TableReader(TableReader const &) = delete;
    TableReader & operator=(TableReader const &) = delete;
    ~TableReader();

    SchemaMetadataDescription const & schema_metadata() const { return m_schema_metadata; }

    std::size_t num_record_batches() const;

    Result<int64_t> CountRows() const;

    Result<std::shared_ptr<arrow::RecordBatch>> ReadRecordBatch(int i) const;

private:
    std::shared_ptr<void> m_input_source;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> m_reader;
    SchemaMetadataDescription m_schema_metadata;
};

// Same as RecordBatchFileReader::ReadRecordBatch() but validates the contents.
Result<std::shared_ptr<arrow::RecordBatch>> ReadRecordBatchAndValidate(
    arrow::ipc::RecordBatchFileReader & reader,
    int i);

}  // namespace pod5
