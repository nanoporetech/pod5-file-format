#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/result.h"
#include "mkr_format/schema_metadata.h"
#include "mkr_format/signal_table_schema.h"
#include "mkr_format/types.h"

#include <arrow/io/type_fwd.h>
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

class MKR_FORMAT_EXPORT SignalTableRecordBatch {
public:
    SignalTableRecordBatch(std::shared_ptr<arrow::RecordBatch>&& batch,
                           SignalTableSchemaDescription field_locations);

    SignalTableRecordBatch(SignalTableRecordBatch&&);
    SignalTableRecordBatch& operator=(SignalTableRecordBatch&&);
    SignalTableRecordBatch(SignalTableRecordBatch const&) = delete;
    SignalTableRecordBatch& operator=(SignalTableRecordBatch const&) = delete;
    ~SignalTableRecordBatch();

    std::size_t num_rows() const;

    std::shared_ptr<UuidArray> read_id_column() const;
    std::shared_ptr<arrow::LargeListArray> uncompressed_signal_column() const;
    std::shared_ptr<VbzSignalArray> vbz_signal_column() const;
    std::shared_ptr<arrow::UInt32Array> samples_column() const;

private:
    std::shared_ptr<arrow::RecordBatch> m_batch;
    SignalTableSchemaDescription m_field_locations;
};

class MKR_FORMAT_EXPORT SignalTableReader {
public:
    enum class FieldNumbers : int {
        ReadId = 0,
        Signal = 1,
        Samples = 2,
    };

    SignalTableReader(std::shared_ptr<void>&& input_source,
                      std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                      SignalTableSchemaDescription field_locations,
                      SchemaMetadataDescription&& schema_metadata,
                      arrow::MemoryPool* pool);
    SignalTableReader(SignalTableReader&&);
    SignalTableReader& operator=(SignalTableReader&&);
    SignalTableReader(SignalTableReader const&) = delete;
    SignalTableReader& operator=(SignalTableReader const&) = delete;
    ~SignalTableReader();

    SchemaMetadataDescription const& schema_metadata() const { return m_schema_metadata; }

    std::size_t num_record_batches() const;
    Result<SignalTableRecordBatch> read_record_batch(std::size_t i) const;

private:
    arrow::MemoryPool* m_pool = nullptr;
    std::shared_ptr<void> m_input_source;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> m_reader;
    SignalTableSchemaDescription m_field_locations;
    SchemaMetadataDescription m_schema_metadata;
};

Result<SignalTableReader> make_signal_table_reader(
        std::shared_ptr<arrow::io::RandomAccessFile> const& sink,
        arrow::MemoryPool* pool);

}  // namespace mkr