#include "pod5_format/table_reader.h"

#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/util/align_util.h>

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

Result<int64_t> TableReader::CountRows() const { return m_reader->CountRows(); }

Result<std::shared_ptr<arrow::RecordBatch>> TableReader::ReadRecordBatch(int i) const
{
    return ReadRecordBatchAndValidate(*m_reader, i);
}

Result<std::shared_ptr<arrow::RecordBatch>> ReadRecordBatchAndValidate(
    arrow::ipc::RecordBatchFileReader & reader,
    int i)
{
    ARROW_ASSIGN_OR_RAISE(auto batch, reader.ReadRecordBatch(i));
    ARROW_RETURN_NOT_OK(batch->ValidateFull());

    // Check that the data buffers are aligned.
    std::vector<bool> unaligned_columns;
    unaligned_columns.reserve(batch->num_columns());
    if (!arrow::util::CheckAlignment(*batch, arrow::util::kValueAlignment, &unaligned_columns)) {
        return Status::Invalid("Column data alignment check failed");
    }

    return batch;
}

}  // namespace pod5
