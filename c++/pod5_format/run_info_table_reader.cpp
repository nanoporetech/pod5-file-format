#include "pod5_format/run_info_table_reader.h"

#include "pod5_format/schema_metadata.h"
#include "pod5_format/schema_utils.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

#include <algorithm>

namespace pod5 {

inline std::vector<std::pair<std::string, std::string>> value_for_map(
    std::shared_ptr<arrow::MapArray> const & map_array,
    std::size_t row_index)
{
    std::size_t offset = map_array->value_offset(row_index);
    std::size_t length = map_array->value_length(row_index);

    auto const & keys = std::dynamic_pointer_cast<arrow::StringArray>(map_array->keys());
    auto const & items = std::dynamic_pointer_cast<arrow::StringArray>(map_array->items());

    std::vector<std::pair<std::string, std::string>> result;
    for (std::size_t i = offset; i < offset + length; ++i) {
        result.push_back(std::make_pair(keys->Value(i).to_string(), items->Value(i).to_string()));
    }
    return result;
}

RunInfoTableRecordBatch::RunInfoTableRecordBatch(
    std::shared_ptr<arrow::RecordBatch> && batch,
    std::shared_ptr<RunInfoTableSchemaDescription const> const & field_locations)
: TableRecordBatch(std::move(batch))
, m_field_locations(field_locations)
{
}

RunInfoTableRecordBatch::RunInfoTableRecordBatch(RunInfoTableRecordBatch && other)
: TableRecordBatch(std::move(other))
{
    m_field_locations = std::move(other.m_field_locations);
}

RunInfoTableRecordBatch & RunInfoTableRecordBatch::operator=(RunInfoTableRecordBatch && other)
{
    TableRecordBatch & base = *this;
    base = other;

    m_field_locations = std::move(other.m_field_locations);
    return *this;
}

Result<RunInfoTableRecordColumns> RunInfoTableRecordBatch::columns() const
{
    RunInfoTableRecordColumns result;
    result.table_version = m_field_locations->table_version();

    auto const & bat = batch();

    // V0 fields:
    result.acquisition_id = find_column(bat, m_field_locations->acquisition_id);
    result.acquisition_start_time = find_column(bat, m_field_locations->acquisition_start_time);
    result.adc_max = find_column(bat, m_field_locations->adc_max);
    result.adc_min = find_column(bat, m_field_locations->adc_min);
    result.context_tags = find_column(bat, m_field_locations->context_tags);
    result.experiment_name = find_column(bat, m_field_locations->experiment_name);
    result.flow_cell_id = find_column(bat, m_field_locations->flow_cell_id);
    result.flow_cell_product_code = find_column(bat, m_field_locations->flow_cell_product_code);
    result.protocol_name = find_column(bat, m_field_locations->protocol_name);
    result.protocol_run_id = find_column(bat, m_field_locations->protocol_run_id);
    result.protocol_start_time = find_column(bat, m_field_locations->protocol_start_time);
    result.sample_id = find_column(bat, m_field_locations->sample_id);
    result.sample_rate = find_column(bat, m_field_locations->sample_rate);
    result.sequencing_kit = find_column(bat, m_field_locations->sequencing_kit);
    result.sequencer_position = find_column(bat, m_field_locations->sequencer_position);
    result.sequencer_position_type = find_column(bat, m_field_locations->sequencer_position_type);
    result.software = find_column(bat, m_field_locations->software);
    result.system_name = find_column(bat, m_field_locations->system_name);
    result.system_type = find_column(bat, m_field_locations->system_type);
    result.tracking_id = find_column(bat, m_field_locations->tracking_id);

    return result;
}

//---------------------------------------------------------------------------------------------------------------------

RunInfoTableReader::RunInfoTableReader(
    std::shared_ptr<void> && input_source,
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> && reader,
    std::shared_ptr<RunInfoTableSchemaDescription const> const & field_locations,
    SchemaMetadataDescription && schema_metadata,
    arrow::MemoryPool * pool)
: TableReader(std::move(input_source), std::move(reader), std::move(schema_metadata), pool)
, m_field_locations(field_locations)
{
}

RunInfoTableReader::RunInfoTableReader(RunInfoTableReader && other)
: TableReader(std::move(other))
, m_field_locations(std::move(other.m_field_locations))
{
}

RunInfoTableReader & RunInfoTableReader::operator=(RunInfoTableReader && other)
{
    static_cast<TableReader &>(*this) = std::move(static_cast<TableReader &>(*this));
    m_field_locations = std::move(other.m_field_locations);
    return *this;
}

Result<RunInfoTableRecordBatch> RunInfoTableReader::read_record_batch(std::size_t i) const
{
    std::lock_guard<std::mutex> l(m_batch_get_mutex);
    ARROW_ASSIGN_OR_RAISE(auto record_batch, reader()->ReadRecordBatch(i));
    return RunInfoTableRecordBatch{std::move(record_batch), m_field_locations};
}

Result<std::shared_ptr<RunInfoData const>> RunInfoTableReader::find_run_info(
    std::string const & acquisition_id) const
{
    std::lock_guard<std::mutex> l(m_run_info_lookup_mutex);
    auto it = m_run_info_lookup.find(acquisition_id);
    if (it != m_run_info_lookup.end()) {
        return it->second;
    }

    ARROW_RETURN_NOT_OK(prepare_run_infos_vector());

    std::shared_ptr<const RunInfoData> run_info = nullptr;
    std::size_t glb_run_info_index = 0;
    for (std::size_t i = 0; i < num_record_batches(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto batch, read_record_batch(i));
        auto acq_id = find_column(batch.batch(), m_field_locations->acquisition_id);

        for (std::size_t j = 0; j < batch.num_rows(); ++j) {
            if (acq_id->Value(j) == acquisition_id) {
                ARROW_ASSIGN_OR_RAISE(
                    run_info, load_run_info_from_batch(batch, j, glb_run_info_index++));
                break;
            }
        }

        if (run_info) {
            break;
        }
    }

    if (!run_info) {
        return arrow::Status::Invalid(
            "Failed to find acquisition id '", acquisition_id, "' in run info table");
    }

    return run_info;
}

Result<std::shared_ptr<RunInfoData const>> RunInfoTableReader::get_run_info(std::size_t index) const
{
    ARROW_RETURN_NOT_OK(prepare_run_infos_vector());

    if (index < 0 || index >= m_run_infos.size()) {
        return arrow::Status::IndexError(
            "Invalid index into run infos (expected ", index, " < ", m_run_infos.size(), ")");
    }

    if (m_run_infos[index]) {
        return m_run_infos[index];
    }

    ARROW_ASSIGN_OR_RAISE(auto first_batch, read_record_batch(0));
    auto const batch_size = first_batch.num_rows();

    auto const batch_idx = index / batch_size;
    auto const batch_row = index - (batch_idx * batch_size);

    if (batch_idx >= num_record_batches()) {
        return Status::Invalid("Row outside batch bounds");
    }

    ARROW_ASSIGN_OR_RAISE(auto batch, read_record_batch(batch_idx));

    return load_run_info_from_batch(batch, batch_row, index);
}

Result<std::size_t> RunInfoTableReader::get_run_info_count() const
{
    auto batch_count = num_record_batches();
    if (batch_count == 0) {
        return 0;
    }

    ARROW_ASSIGN_OR_RAISE(auto first_batch, read_record_batch(0));
    ARROW_ASSIGN_OR_RAISE(auto last_batch, read_record_batch(batch_count - 1));

    return (batch_count - 1) * first_batch.num_rows() + last_batch.num_rows();
}

Result<std::shared_ptr<RunInfoData const>> RunInfoTableReader::load_run_info_from_batch(
    RunInfoTableRecordBatch const & batch,
    std::size_t batch_index,
    std::size_t global_index) const
{
    ARROW_ASSIGN_OR_RAISE(auto columns, batch.columns());

    auto acquisition_id = columns.acquisition_id->Value(batch_index).to_string();
    auto run_info = std::make_shared<RunInfoData>(
        acquisition_id,
        columns.acquisition_start_time->Value(batch_index),
        columns.adc_max->Value(batch_index),
        columns.adc_min->Value(batch_index),
        value_for_map(columns.context_tags, batch_index),
        columns.experiment_name->Value(batch_index).to_string(),
        columns.flow_cell_id->Value(batch_index).to_string(),
        columns.flow_cell_product_code->Value(batch_index).to_string(),
        columns.protocol_name->Value(batch_index).to_string(),
        columns.protocol_run_id->Value(batch_index).to_string(),
        columns.protocol_start_time->Value(batch_index),
        columns.sample_id->Value(batch_index).to_string(),
        columns.sample_rate->Value(batch_index),
        columns.sequencing_kit->Value(batch_index).to_string(),
        columns.sequencer_position->Value(batch_index).to_string(),
        columns.sequencer_position_type->Value(batch_index).to_string(),
        columns.software->Value(batch_index).to_string(),
        columns.system_name->Value(batch_index).to_string(),
        columns.system_type->Value(batch_index).to_string(),
        value_for_map(columns.tracking_id, batch_index));

    // Cache run info for later retrieval by index:
    m_run_infos[global_index] = run_info;
    m_run_info_lookup[acquisition_id] = run_info;
    return run_info;
}

arrow::Status RunInfoTableReader::prepare_run_infos_vector() const
{
    if (m_run_infos.empty()) {
        ARROW_ASSIGN_OR_RAISE(auto row_count, get_run_info_count())
        m_run_infos.resize(row_count);
    }

    return Status::OK();
}

//---------------------------------------------------------------------------------------------------------------------

Result<RunInfoTableReader> make_run_info_table_reader(
    std::shared_ptr<arrow::io::RandomAccessFile> const & input,
    arrow::MemoryPool * pool)
{
    arrow::ipc::IpcReadOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(input, options));

    auto read_metadata_key_values = reader->schema()->metadata();
    if (!read_metadata_key_values) {
        return Status::IOError("Missing metadata on run info table schema");
    }
    ARROW_ASSIGN_OR_RAISE(
        auto read_metadata, read_schema_key_value_metadata(read_metadata_key_values));
    ARROW_ASSIGN_OR_RAISE(
        auto field_locations, read_run_info_table_schema(read_metadata, reader->schema()));

    return RunInfoTableReader(
        {input}, std::move(reader), field_locations, std::move(read_metadata), pool);
}

}  // namespace pod5
