#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/read_table_schema.h"
#include "pod5_format/read_table_utils.h"
#include "pod5_format/result.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/table_reader.h"
#include "pod5_format/types.h"

#include <arrow/io/type_fwd.h>
#include <boost/uuid/uuid.hpp>
#include <gsl/gsl-lite.hpp>

#include <mutex>

namespace arrow {
class Schema;

namespace io {
class RandomAccessFile;
}

namespace ipc {
class RecordBatchFileReader;
}
}  // namespace arrow

namespace pod5 {

class CalibrationData;
class EndReasonData;
class PoreData;
class RunInfoData;
class ReadIdSearchInput;

struct ReadTableRecordColumns {
    std::shared_ptr<UuidArray> read_id;
    std::shared_ptr<arrow::ListArray> signal;
    std::shared_ptr<arrow::UInt32Array> read_number;
    std::shared_ptr<arrow::UInt64Array> start_sample;
    std::shared_ptr<arrow::FloatArray> median_before;

    std::shared_ptr<arrow::UInt64Array> num_minknow_events;

    std::shared_ptr<arrow::FloatArray> tracked_scaling_scale;
    std::shared_ptr<arrow::FloatArray> tracked_scaling_shift;
    std::shared_ptr<arrow::FloatArray> predicted_scaling_scale;
    std::shared_ptr<arrow::FloatArray> predicted_scaling_shift;
    std::shared_ptr<arrow::UInt32Array> num_reads_since_mux_change;
    std::shared_ptr<arrow::FloatArray> time_since_mux_change;

    std::shared_ptr<arrow::UInt64Array> num_samples;

    std::shared_ptr<arrow::UInt16Array> channel;
    std::shared_ptr<arrow::UInt8Array> well;
    std::shared_ptr<arrow::DictionaryArray> pore_type;
    std::shared_ptr<arrow::FloatArray> calibration_offset;
    std::shared_ptr<arrow::FloatArray> calibration_scale;
    std::shared_ptr<arrow::DictionaryArray> end_reason;
    std::shared_ptr<arrow::BooleanArray> end_reason_forced;
    std::shared_ptr<arrow::DictionaryArray> run_info;

    TableSpecVersion table_version;
};

class POD5_FORMAT_EXPORT ReadTableRecordBatch : public TableRecordBatch {
public:
    ReadTableRecordBatch(
        std::shared_ptr<arrow::RecordBatch> && batch,
        std::shared_ptr<ReadTableSchemaDescription const> const & field_locations);
    ReadTableRecordBatch(ReadTableRecordBatch &&);
    ReadTableRecordBatch & operator=(ReadTableRecordBatch &&);

    std::shared_ptr<UuidArray> read_id_column() const;
    std::shared_ptr<arrow::ListArray> signal_column() const;

    Result<std::string> get_pore_type(std::int16_t pore_dict_index) const;
    Result<std::pair<ReadEndReason, std::string>> get_end_reason(
        std::int16_t end_reason_dict_index) const;
    Result<std::string> get_run_info(std::int16_t run_info_dict_index) const;

    Result<ReadTableRecordColumns> columns() const;

    Result<std::shared_ptr<arrow::UInt64Array>> get_signal_rows(std::int64_t batch_row);

private:
    std::shared_ptr<ReadTableSchemaDescription const> m_field_locations;
    mutable std::mutex m_dictionary_access_lock;
};

class POD5_FORMAT_EXPORT ReadTableReader : public TableReader {
public:
    ReadTableReader(
        std::shared_ptr<void> && input_source,
        std::shared_ptr<arrow::ipc::RecordBatchFileReader> && reader,
        std::shared_ptr<ReadTableSchemaDescription const> const & field_locations,
        SchemaMetadataDescription && schema_metadata,
        arrow::MemoryPool * pool);

    ReadTableReader(ReadTableReader && other);
    ReadTableReader & operator=(ReadTableReader && other);

    Result<ReadTableRecordBatch> read_record_batch(std::size_t i) const;

    Status build_read_id_lookup();

    Result<std::size_t> search_for_read_ids(
        ReadIdSearchInput const & search_input,
        gsl::span<uint32_t> const & batch_counts,
        gsl::span<uint32_t> const & batch_rows);

private:
    struct IndexData {
        boost::uuids::uuid id;
        std::size_t batch;
        std::size_t batch_row;
    };

    std::shared_ptr<ReadTableSchemaDescription const> m_field_locations;
    std::vector<IndexData> m_sorted_file_read_ids;

    mutable std::mutex m_batch_get_mutex;
};

POD5_FORMAT_EXPORT Result<ReadTableReader> make_read_table_reader(
    std::shared_ptr<arrow::io::RandomAccessFile> const & sink,
    arrow::MemoryPool * pool);

}  // namespace pod5
