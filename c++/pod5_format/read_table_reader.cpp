#include "pod5_format/read_table_reader.h"

#include "pod5_format/read_table_utils.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/schema_utils.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

#include <algorithm>

namespace pod5 {

ReadTableRecordBatch::ReadTableRecordBatch(
    std::shared_ptr<arrow::RecordBatch> && batch,
    std::shared_ptr<ReadTableSchemaDescription const> const & field_locations)
: TableRecordBatch(std::move(batch))
, m_field_locations(field_locations)
{
}

ReadTableRecordBatch::ReadTableRecordBatch(ReadTableRecordBatch && other)
: TableRecordBatch(std::move(other))
{
    m_field_locations = std::move(other.m_field_locations);
}

ReadTableRecordBatch & ReadTableRecordBatch::operator=(ReadTableRecordBatch && other)
{
    TableRecordBatch & base = *this;
    base = other;

    m_field_locations = std::move(other.m_field_locations);
    return *this;
}

std::shared_ptr<UuidArray> ReadTableRecordBatch::read_id_column() const
{
    return find_column(batch(), m_field_locations->read_id);
}

std::shared_ptr<arrow::ListArray> ReadTableRecordBatch::signal_column() const
{
    return find_column(batch(), m_field_locations->signal);
}

Result<ReadTableRecordColumns> ReadTableRecordBatch::columns() const
{
    ReadTableRecordColumns result;
    result.table_version = m_field_locations->table_version();

    auto const & bat = batch();

    // V0 fields:
    result.read_id = find_column(bat, m_field_locations->read_id);
    result.signal = find_column(bat, m_field_locations->signal);
    result.read_number = find_column(bat, m_field_locations->read_number);
    result.start_sample = find_column(bat, m_field_locations->start);
    result.median_before = find_column(bat, m_field_locations->median_before);

    // V1 fields:
    if (result.table_version >= ReadTableSpecVersion::v1()) {
        result.num_minknow_events = find_column(bat, m_field_locations->num_minknow_events);

        result.tracked_scaling_scale = find_column(bat, m_field_locations->tracked_scaling_scale);
        result.tracked_scaling_shift = find_column(bat, m_field_locations->tracked_scaling_shift);
        result.predicted_scaling_scale =
            find_column(bat, m_field_locations->predicted_scaling_scale);
        result.predicted_scaling_shift =
            find_column(bat, m_field_locations->predicted_scaling_shift);
        result.num_reads_since_mux_change =
            find_column(bat, m_field_locations->num_reads_since_mux_change);
        result.time_since_mux_change = find_column(bat, m_field_locations->time_since_mux_change);
    }

    // V2 fields:
    if (result.table_version >= ReadTableSpecVersion::v2()) {
        result.num_samples = find_column(bat, m_field_locations->num_samples);
    }

    // V3 fields:
    if (result.table_version >= ReadTableSpecVersion::v3()) {
        result.channel = find_column(bat, m_field_locations->channel);
        result.well = find_column(bat, m_field_locations->well);
        result.pore_type = find_column(bat, m_field_locations->pore_type);
        result.calibration_offset = find_column(bat, m_field_locations->calibration_offset);
        result.calibration_scale = find_column(bat, m_field_locations->calibration_scale);
        result.end_reason = find_column(bat, m_field_locations->end_reason);
        result.end_reason_forced = find_column(bat, m_field_locations->end_reason_forced);
        result.run_info = find_column(bat, m_field_locations->run_info);
    }

    return result;
}

Result<std::shared_ptr<arrow::UInt64Array>> ReadTableRecordBatch::get_signal_rows(
    std::int64_t batch_row)
{
    auto signal_col = signal_column();

    auto const & values = signal_col->values();

    auto const offset = signal_col->value_offset(batch_row);
    if (offset >= values->length()) {
        return arrow::Status::Invalid(
            "Invalid signal row offset '", offset, "' is outside the size of the values array.");
    }

    auto const length = signal_col->value_length(batch_row);
    if (length > values->length() - offset) {
        return arrow::Status::Invalid(
            "Invalid signal row length '", length, "' is outside the size of the values array.");
    }

    return std::static_pointer_cast<arrow::UInt64Array>(values->Slice(offset, length));
}

Result<std::string> ReadTableRecordBatch::get_pore_type(std::int16_t pore_index) const
{
    std::lock_guard<std::mutex> l(m_dictionary_access_lock);

    if (!m_field_locations->pore_type.found_field()) {
        return arrow::Status::Invalid("pore field is not present in the file");
    }

    auto pore_column = find_column(batch(), m_field_locations->pore_type);
    auto pore_data = std::static_pointer_cast<arrow::StringArray>(pore_column->dictionary());
    if (pore_index < 0 || pore_index >= pore_data->length()) {
        return arrow::Status::IndexError(
            "Invalid index ", pore_index, " for pore array of length ", pore_data->length());
    }

    return pore_data->Value(pore_index).to_string();
}

Result<std::pair<ReadEndReason, std::string>> ReadTableRecordBatch::get_end_reason(
    std::int16_t end_reason_index) const
{
    std::lock_guard<std::mutex> l(m_dictionary_access_lock);

    if (!m_field_locations->end_reason.found_field()) {
        return arrow::Status::Invalid("end_reason field is not present in the file");
    }

    auto end_reason_column = find_column(batch(), m_field_locations->end_reason);
    auto end_reason_data =
        std::static_pointer_cast<arrow::StringArray>(end_reason_column->dictionary());
    if (end_reason_index >= end_reason_data->length()) {
        return arrow::Status::IndexError(
            "Invalid index ",
            end_reason_index,
            " for end reason array of length ",
            end_reason_data->length());
    }

    auto str_value = end_reason_data->Value(end_reason_index).to_string();

    return std::make_pair(end_reason_from_string(str_value), str_value);
}

Result<std::string> ReadTableRecordBatch::get_run_info(std::int16_t run_info_index) const
{
    std::lock_guard<std::mutex> l(m_dictionary_access_lock);

    if (!m_field_locations->run_info.found_field()) {
        return arrow::Status::Invalid("end_reason field is not present in the file");
    }

    auto run_info_column = find_column(batch(), m_field_locations->run_info);
    auto run_info_data =
        std::static_pointer_cast<arrow::StringArray>(run_info_column->dictionary());
    if (run_info_index < 0 || run_info_index >= run_info_data->length()) {
        return arrow::Status::IndexError(
            "Invalid index ",
            run_info_index,
            " for run info array of length ",
            run_info_data->length());
    }

    return run_info_data->Value(run_info_index).to_string();
}

//---------------------------------------------------------------------------------------------------------------------

ReadTableReader::ReadTableReader(
    std::shared_ptr<void> && input_source,
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> && reader,
    std::shared_ptr<ReadTableSchemaDescription const> const & field_locations,
    SchemaMetadataDescription && schema_metadata,
    arrow::MemoryPool * pool)
: TableReader(std::move(input_source), std::move(reader), std::move(schema_metadata), pool)
, m_field_locations(field_locations)
{
}

ReadTableReader::ReadTableReader(ReadTableReader && other)
: TableReader(std::move(other))
, m_field_locations(std::move(other.m_field_locations))
, m_sorted_file_read_ids(std::move(other.m_sorted_file_read_ids))
{
}

ReadTableReader & ReadTableReader::operator=(ReadTableReader && other)
{
    static_cast<TableReader &>(*this) = std::move(static_cast<TableReader &>(*this));
    m_field_locations = std::move(other.m_field_locations);
    m_sorted_file_read_ids = std::move(other.m_sorted_file_read_ids);
    return *this;
}

Result<ReadTableRecordBatch> ReadTableReader::read_record_batch(std::size_t i) const
{
    std::lock_guard<std::mutex> l(m_batch_get_mutex);
    auto record_batch = reader()->ReadRecordBatch(i);
    if (!record_batch.ok()) {
        return record_batch.status();
    }
    return ReadTableRecordBatch{std::move(*record_batch), m_field_locations};
}

Status ReadTableReader::build_read_id_lookup()
{
    if (!m_sorted_file_read_ids.empty()) {
        return Status::OK();
    }

    std::vector<IndexData> file_read_ids;

    auto const batch_count = num_record_batches();
    std::size_t abs_row_count = 0;

    // Loop each batch and copy read ids out into the index:
    for (std::size_t i = 0; i < batch_count; ++i) {
        ARROW_ASSIGN_OR_RAISE(auto batch, read_record_batch(i));

        if (file_read_ids.empty()) {
            file_read_ids.reserve(batch.num_rows() * batch_count);
        }
        file_read_ids.resize(file_read_ids.size() + batch.num_rows());

        auto read_id_col = batch.read_id_column();
        auto raw_read_id_values = read_id_col->raw_values();
        for (std::size_t row = 0; row < (std::size_t)read_id_col->length(); ++row) {
            // Record the id, and its location within the file:
            file_read_ids[abs_row_count].id = raw_read_id_values[row];
            file_read_ids[abs_row_count].batch = i;
            file_read_ids[abs_row_count].batch_row = row;
            abs_row_count += 1;
        }
    }

    // Sort by read id for searching later:
    std::sort(file_read_ids.begin(), file_read_ids.end(), [](auto const & a, auto const & b) {
        return a.id < b.id;
    });

    // Move data out now we successfully build the index:
    m_sorted_file_read_ids = std::move(file_read_ids);

    return Status::OK();
}

Result<std::size_t> ReadTableReader::search_for_read_ids(
    ReadIdSearchInput const & search_input,
    gsl::span<uint32_t> const & batch_counts,
    gsl::span<uint32_t> const & batch_rows)
{
    ARROW_RETURN_NOT_OK(build_read_id_lookup());

    std::size_t successes = 0;

    std::vector<std::vector<std::uint32_t>> batch_data(batch_counts.size());
    auto const initial_reserve_size = search_input.read_id_count() / batch_counts.size();
    for (auto & br : batch_data) {
        br.reserve(initial_reserve_size);
    }

    auto file_ids_current_it = m_sorted_file_read_ids.begin();
    auto const file_ids_end = m_sorted_file_read_ids.end();
    for (std::size_t i = 0; i < search_input.read_id_count(); ++i) {
        auto const & search_item = search_input[i];

        // Increment file pointer while less than the search term:
        while (file_ids_current_it->id < search_item.id && file_ids_current_it != file_ids_end) {
            ++file_ids_current_it;
        }

        // No more ids to search, both lists are sorted and we haven't found this one, we won't find any others.
        if (file_ids_current_it == file_ids_end) {
            break;
        }

        // If we found it record the location:
        if (file_ids_current_it->id == search_item.id) {
            batch_data[file_ids_current_it->batch].push_back(file_ids_current_it->batch_row);
            successes += 1;
        }
    }

    std::size_t full_size_so_far = 0;
    for (std::size_t i = 0; i < batch_data.size(); ++i) {
        auto & data = batch_data[i];
        batch_counts[i] = data.size();

        // Ensure the batch indices within the batch are sorted:
        std::sort(data.begin(), data.end());

        // Copy the row indices into the packed vector:
        std::copy(data.begin(), data.end(), batch_rows.begin() + full_size_so_far);

        full_size_so_far += data.size();
    }

    return successes;
}

//---------------------------------------------------------------------------------------------------------------------

Result<ReadTableReader> make_read_table_reader(
    std::shared_ptr<arrow::io::RandomAccessFile> const & input,
    arrow::MemoryPool * pool)
{
    arrow::ipc::IpcReadOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(input, options));

    auto read_metadata_key_values = reader->schema()->metadata();
    if (!read_metadata_key_values) {
        return Status::IOError("Missing metadata on read table schema");
    }
    ARROW_ASSIGN_OR_RAISE(
        auto read_metadata, read_schema_key_value_metadata(read_metadata_key_values));
    ARROW_ASSIGN_OR_RAISE(
        auto field_locations, read_read_table_schema(read_metadata, reader->schema()));

    return ReadTableReader(
        {input}, std::move(reader), field_locations, std::move(read_metadata), pool);
}

}  // namespace pod5
