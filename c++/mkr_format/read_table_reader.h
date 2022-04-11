#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/read_table_schema.h"
#include "mkr_format/result.h"
#include "mkr_format/schema_metadata.h"
#include "mkr_format/table_reader.h"
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

class MKR_FORMAT_EXPORT ReadTableRecordBatch : public TableRecordBatch {
public:
    ReadTableRecordBatch(std::shared_ptr<arrow::RecordBatch>&& batch,
                         ReadTableSchemaDescription field_locations);

    std::shared_ptr<UuidArray> read_id_column() const;
    std::shared_ptr<arrow::ListArray> signal_column() const;
    std::shared_ptr<arrow::DictionaryArray> pore_column() const;
    std::shared_ptr<arrow::DictionaryArray> calibration_column() const;
    std::shared_ptr<arrow::UInt32Array> read_number_column() const;
    std::shared_ptr<arrow::UInt64Array> start_sample_column() const;
    std::shared_ptr<arrow::FloatArray> median_before_column() const;
    std::shared_ptr<arrow::DictionaryArray> end_reason_column() const;
    std::shared_ptr<arrow::DictionaryArray> run_info_column() const;

private:
    ReadTableSchemaDescription m_field_locations;
};

class MKR_FORMAT_EXPORT ReadTableReader : public TableReader {
public:
    ReadTableReader(std::shared_ptr<void>&& input_source,
                    std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                    ReadTableSchemaDescription field_locations,
                    SchemaMetadataDescription&& schema_metadata,
                    arrow::MemoryPool* pool);

    Result<ReadTableRecordBatch> read_record_batch(std::size_t i) const;

private:
    ReadTableSchemaDescription m_field_locations;
};

Result<ReadTableReader> make_read_table_reader(
        std::shared_ptr<arrow::io::RandomAccessFile> const& sink,
        arrow::MemoryPool* pool);

}  // namespace mkr