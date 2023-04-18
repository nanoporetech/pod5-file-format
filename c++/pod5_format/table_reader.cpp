#include "pod5_format/table_reader.h"

#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>

namespace pod5 {

TableRecordBatch::TableRecordBatch(std::shared_ptr<arrow::RecordBatch> const & batch)
: m_batch(batch)
{
}

TableRecordBatch::TableRecordBatch(std::shared_ptr<arrow::RecordBatch> && batch)
: m_batch(std::move(batch))
{
}

TableRecordBatch::TableRecordBatch(TableRecordBatch const &) = default;
TableRecordBatch & TableRecordBatch::operator=(TableRecordBatch const &) = default;
TableRecordBatch::TableRecordBatch(TableRecordBatch &&) = default;
TableRecordBatch & TableRecordBatch::operator=(TableRecordBatch &&) = default;
TableRecordBatch::~TableRecordBatch() = default;

std::size_t TableRecordBatch::num_rows() const { return m_batch->num_rows(); }

//---------------------------------------------------------------------------------------------------------------------

TableReader::TableReader(
    std::shared_ptr<void> && input_source,
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> && reader,
    SchemaMetadataDescription && schema_metadata,
    arrow::MemoryPool * pool)
: m_input_source(std::move(input_source))
, m_reader(std::move(reader))
, m_schema_metadata(std::move(schema_metadata))
{
}

TableReader::TableReader(TableReader &&) = default;
TableReader & TableReader::operator=(TableReader &&) = default;
TableReader::~TableReader() = default;

std::size_t TableReader::num_record_batches() const { return m_reader->num_record_batches(); }

}  // namespace pod5
