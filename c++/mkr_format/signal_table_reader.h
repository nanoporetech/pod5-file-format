#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/result.h"
#include "mkr_format/schema_metadata.h"
#include "mkr_format/signal_table_schema.h"
#include "mkr_format/table_reader.h"
#include "mkr_format/types.h"

#include <arrow/io/type_fwd.h>
#include <boost/optional.hpp>
#include <boost/uuid/uuid.hpp>
#include <gsl/gsl-lite.hpp>

namespace arrow {
class Schema;
namespace io {
class RandomAccessFile;
}
namespace ipc {
class RecordBatchFileReader;
}
}  // namespace arrow

namespace mkr {

class MKR_FORMAT_EXPORT SignalTableRecordBatch : public TableRecordBatch {
public:
    SignalTableRecordBatch(std::shared_ptr<arrow::RecordBatch>&& batch,
                           SignalTableSchemaDescription field_locations,
                           arrow::MemoryPool* pool);

    std::shared_ptr<UuidArray> read_id_column() const;
    std::shared_ptr<arrow::LargeListArray> uncompressed_signal_column() const;
    std::shared_ptr<VbzSignalArray> vbz_signal_column() const;
    std::shared_ptr<arrow::UInt32Array> samples_column() const;

    Result<std::size_t> samples_byte_count(std::size_t row_index) const;

    /// \brief Extract a row of sample data into [samples], decompressing if required.
    Status extract_signal_row(std::size_t row_index, gsl::span<std::int16_t> samples) const;

private:
    SignalTableSchemaDescription m_field_locations;
    arrow::MemoryPool* m_pool;
};

class MKR_FORMAT_EXPORT SignalTableReader : public TableReader {
public:
    SignalTableReader(std::shared_ptr<void>&& input_source,
                      std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                      SignalTableSchemaDescription field_locations,
                      SchemaMetadataDescription&& schema_metadata,
                      arrow::MemoryPool* pool);

    Result<SignalTableRecordBatch> read_record_batch(std::size_t i) const;

    Result<std::size_t> signal_batch_for_row_id(std::size_t row,
                                                std::size_t* batch_start_row) const;

private:
    SignalTableSchemaDescription m_field_locations;
    arrow::MemoryPool* m_pool;
    mutable boost::optional<std::pair<std::size_t, SignalTableRecordBatch>> m_last_batch;
};

MKR_FORMAT_EXPORT Result<SignalTableReader> make_signal_table_reader(
        std::shared_ptr<arrow::io::RandomAccessFile> const& sink,
        arrow::MemoryPool* pool);

}  // namespace mkr