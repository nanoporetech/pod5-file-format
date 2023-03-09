#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/read_table_utils.h"
#include "pod5_format/result.h"
#include "pod5_format/run_info_table_schema.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/table_reader.h"
#include "pod5_format/types.h"

#include <arrow/io/type_fwd.h>
#include <boost/uuid/uuid.hpp>
#include <gsl/gsl-lite.hpp>

#include <mutex>
#include <unordered_map>

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

struct RunInfoTableRecordColumns {
    // V0 Fields
    std::shared_ptr<arrow::StringArray> acquisition_id;
    std::shared_ptr<arrow::TimestampArray> acquisition_start_time;
    std::shared_ptr<arrow::Int16Array> adc_max;
    std::shared_ptr<arrow::Int16Array> adc_min;
    std::shared_ptr<arrow::MapArray> context_tags;
    std::shared_ptr<arrow::StringArray> experiment_name;
    std::shared_ptr<arrow::StringArray> flow_cell_id;
    std::shared_ptr<arrow::StringArray> flow_cell_product_code;
    std::shared_ptr<arrow::StringArray> protocol_name;
    std::shared_ptr<arrow::StringArray> protocol_run_id;
    std::shared_ptr<arrow::TimestampArray> protocol_start_time;
    std::shared_ptr<arrow::StringArray> sample_id;
    std::shared_ptr<arrow::UInt16Array> sample_rate;
    std::shared_ptr<arrow::StringArray> sequencing_kit;
    std::shared_ptr<arrow::StringArray> sequencer_position;
    std::shared_ptr<arrow::StringArray> sequencer_position_type;
    std::shared_ptr<arrow::StringArray> software;
    std::shared_ptr<arrow::StringArray> system_name;
    std::shared_ptr<arrow::StringArray> system_type;
    std::shared_ptr<arrow::MapArray> tracking_id;

    TableSpecVersion table_version;
};

class POD5_FORMAT_EXPORT RunInfoTableRecordBatch : public TableRecordBatch {
public:
    RunInfoTableRecordBatch(
        std::shared_ptr<arrow::RecordBatch> && batch,
        std::shared_ptr<RunInfoTableSchemaDescription const> const & field_locations);
    RunInfoTableRecordBatch(RunInfoTableRecordBatch &&);
    RunInfoTableRecordBatch & operator=(RunInfoTableRecordBatch &&);

    Result<RunInfoTableRecordColumns> columns() const;

private:
    std::shared_ptr<RunInfoTableSchemaDescription const> m_field_locations;
};

class POD5_FORMAT_EXPORT RunInfoTableReader : public TableReader {
public:
    RunInfoTableReader(
        std::shared_ptr<void> && input_source,
        std::shared_ptr<arrow::ipc::RecordBatchFileReader> && reader,
        std::shared_ptr<RunInfoTableSchemaDescription const> const & field_locations,
        SchemaMetadataDescription && schema_metadata,
        arrow::MemoryPool * pool);

    RunInfoTableReader(RunInfoTableReader && other);
    RunInfoTableReader & operator=(RunInfoTableReader && other);

    Result<RunInfoTableRecordBatch> read_record_batch(std::size_t i) const;

    Result<std::shared_ptr<RunInfoData const>> find_run_info(
        std::string const & acquisition_id) const;

    Result<std::shared_ptr<RunInfoData const>> get_run_info(std::size_t index) const;
    Result<std::size_t> get_run_info_count() const;

private:
    Result<std::shared_ptr<RunInfoData const>> load_run_info_from_batch(
        RunInfoTableRecordBatch const & batch,
        std::size_t batch_index,
        std::size_t global_index) const;
    arrow::Status prepare_run_infos_vector() const;

    std::shared_ptr<RunInfoTableSchemaDescription const> m_field_locations;
    mutable std::mutex m_batch_get_mutex;
    mutable std::unordered_map<std::string, std::shared_ptr<RunInfoData const>> m_run_info_lookup;
    mutable std::vector<std::shared_ptr<RunInfoData const>> m_run_infos;
    mutable std::mutex m_run_info_lookup_mutex;
};

POD5_FORMAT_EXPORT Result<RunInfoTableReader> make_run_info_table_reader(
    std::shared_ptr<arrow::io::RandomAccessFile> const & sink,
    arrow::MemoryPool * pool);

}  // namespace pod5
