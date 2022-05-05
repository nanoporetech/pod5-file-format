#include "mkr_format/read_table_reader.h"

#include "mkr_format/read_table_utils.h"
#include "mkr_format/schema_metadata.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

#include <algorithm>

namespace mkr {

namespace {
class StructHelper {
public:
    StructHelper(std::shared_ptr<arrow::StructArray> const& struct_data)
            : m_struct_data(struct_data) {}

    template <typename ArrayType>
    auto get_row_value(int field, std::int16_t row_index) const {
        auto field_array = std::static_pointer_cast<ArrayType>(m_struct_data->field(field));
        assert(field_array);
        return field_array->Value(row_index);
    }

    bool get_boolean(int field, std::int16_t row_index) const {
        return get_row_value<arrow::BooleanArray>(field, row_index);
    }
    std::int16_t get_int16(int field, std::int16_t row_index) const {
        return get_row_value<arrow::Int16Array>(field, row_index);
    }
    std::uint16_t get_uint16(int field, std::int16_t row_index) const {
        return get_row_value<arrow::UInt16Array>(field, row_index);
    }
    std::uint8_t get_uint8(int field, std::int16_t row_index) const {
        return get_row_value<arrow::UInt8Array>(field, row_index);
    }
    float get_float(int field, std::int16_t row_index) const {
        return get_row_value<arrow::FloatArray>(field, row_index);
    }
    std::string get_string(int field, std::int16_t row_index) const {
        return get_row_value<arrow::StringArray>(field, row_index).to_string();
    }

    std::int64_t get_timestamp(int field, std::int16_t row_index) const {
        auto field_array =
                std::static_pointer_cast<arrow::TimestampArray>(m_struct_data->field(field));
        assert(field_array);
        return field_array->Value(row_index);
    }

    Result<mkr::RunInfoData::MapType> get_string_map(int field, std::int16_t row_index) const {
        auto field_array = std::static_pointer_cast<arrow::MapArray>(m_struct_data->field(field));
        auto offsets = std::static_pointer_cast<arrow::Int32Array>(field_array->offsets());
        auto keys = std::static_pointer_cast<arrow::StringArray>(field_array->keys());
        auto values = std::static_pointer_cast<arrow::StringArray>(field_array->items());

        auto const start_index = offsets->Value(row_index);
        auto const end_index = offsets->Value(row_index + 1);

        if (keys->length() < end_index) {
            return Status::Invalid("Incorrect number of keys in map field, got ", keys->length(),
                                   " expected ", end_index);
        }

        if (values->length() < end_index) {
            return Status::Invalid("Incorrect number of values in map field, got ",
                                   values->length(), " expected ", end_index);
        }

        mkr::RunInfoData::MapType result;
        for (std::int32_t i = start_index; i < end_index; ++i) {
            result.emplace_back(keys->Value(i).to_string(), values->Value(i).to_string());
        }

        return result;
    }

private:
    std::shared_ptr<arrow::StructArray> m_struct_data;
};
}  // namespace

ReadTableRecordBatch::ReadTableRecordBatch(
        std::shared_ptr<arrow::RecordBatch>&& batch,
        std::shared_ptr<ReadTableSchemaDescription> const& field_locations)
        : TableRecordBatch(std::move(batch)), m_field_locations(field_locations) {}

std::shared_ptr<UuidArray> ReadTableRecordBatch::read_id_column() const {
    return std::static_pointer_cast<UuidArray>(batch()->column(m_field_locations->read_id));
}
std::shared_ptr<arrow::ListArray> ReadTableRecordBatch::signal_column() const {
    return std::static_pointer_cast<arrow::ListArray>(batch()->column(m_field_locations->signal));
}
std::shared_ptr<arrow::DictionaryArray> ReadTableRecordBatch::pore_column() const {
    return std::static_pointer_cast<arrow::DictionaryArray>(
            batch()->column(m_field_locations->pore));
}
std::shared_ptr<arrow::DictionaryArray> ReadTableRecordBatch::calibration_column() const {
    return std::static_pointer_cast<arrow::DictionaryArray>(
            batch()->column(m_field_locations->calibration));
}
std::shared_ptr<arrow::UInt32Array> ReadTableRecordBatch::read_number_column() const {
    return std::static_pointer_cast<arrow::UInt32Array>(
            batch()->column(m_field_locations->read_number));
}
std::shared_ptr<arrow::UInt64Array> ReadTableRecordBatch::start_sample_column() const {
    return std::static_pointer_cast<arrow::UInt64Array>(
            batch()->column(m_field_locations->start_sample));
}
std::shared_ptr<arrow::FloatArray> ReadTableRecordBatch::median_before_column() const {
    return std::static_pointer_cast<arrow::FloatArray>(
            batch()->column(m_field_locations->median_before));
}
std::shared_ptr<arrow::DictionaryArray> ReadTableRecordBatch::end_reason_column() const {
    return std::static_pointer_cast<arrow::DictionaryArray>(
            batch()->column(m_field_locations->end_reason));
}
std::shared_ptr<arrow::DictionaryArray> ReadTableRecordBatch::run_info_column() const {
    return std::static_pointer_cast<arrow::DictionaryArray>(
            batch()->column(m_field_locations->run_info));
}

Result<PoreData> ReadTableRecordBatch::get_pore(std::int16_t pore_index) const {
    auto pore_data = std::static_pointer_cast<arrow::StructArray>(pore_column()->dictionary());
    StructHelper hlp(pore_data);

    return PoreData{hlp.get_uint16(m_field_locations->pore_fields.channel, pore_index),
                    hlp.get_uint8(m_field_locations->pore_fields.well, pore_index),
                    hlp.get_string(m_field_locations->pore_fields.pore_type, pore_index)};
}

Result<CalibrationData> ReadTableRecordBatch::get_calibration(
        std::int16_t calibration_index) const {
    auto calibration_data =
            std::static_pointer_cast<arrow::StructArray>(calibration_column()->dictionary());
    StructHelper hlp(calibration_data);

    return CalibrationData{
            hlp.get_float(m_field_locations->calibration_fields.offset, calibration_index),
            hlp.get_float(m_field_locations->calibration_fields.scale, calibration_index)};
}

Result<EndReasonData> ReadTableRecordBatch::get_end_reason(std::int16_t end_reason_index) const {
    auto end_reason_data =
            std::static_pointer_cast<arrow::StructArray>(end_reason_column()->dictionary());
    StructHelper hlp(end_reason_data);

    return EndReasonData{
            hlp.get_string(m_field_locations->end_reason_fields.end_reason, end_reason_index),
            hlp.get_boolean(m_field_locations->end_reason_fields.forced, end_reason_index),
    };
}

Result<RunInfoData> ReadTableRecordBatch::get_run_info(std::int16_t run_info_index) const {
    auto run_info_data =
            std::static_pointer_cast<arrow::StructArray>(run_info_column()->dictionary());
    StructHelper hlp(run_info_data);

    ARROW_ASSIGN_OR_RAISE(
            auto context_tags,
            hlp.get_string_map(m_field_locations->run_info_fields.context_tags, run_info_index));
    ARROW_ASSIGN_OR_RAISE(
            auto tracking_id,
            hlp.get_string_map(m_field_locations->run_info_fields.tracking_id, run_info_index));

    return RunInfoData{
            hlp.get_string(m_field_locations->run_info_fields.acquisition_id, run_info_index),
            hlp.get_timestamp(m_field_locations->run_info_fields.acquisition_start_time,
                              run_info_index),
            hlp.get_int16(m_field_locations->run_info_fields.adc_max, run_info_index),
            hlp.get_int16(m_field_locations->run_info_fields.adc_min, run_info_index),
            std::move(context_tags),
            hlp.get_string(m_field_locations->run_info_fields.experiment_name, run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.flow_cell_id, run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.flow_cell_product_code,
                           run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.protocol_name, run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.protocol_run_id, run_info_index),
            hlp.get_timestamp(m_field_locations->run_info_fields.protocol_start_time,
                              run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.sample_id, run_info_index),
            hlp.get_uint16(m_field_locations->run_info_fields.sample_rate, run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.sequencing_kit, run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.sequencer_position, run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.sequencer_position_type,
                           run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.software, run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.system_name, run_info_index),
            hlp.get_string(m_field_locations->run_info_fields.system_type, run_info_index),
            std::move(tracking_id)};
}

//---------------------------------------------------------------------------------------------------------------------

ReadTableReader::ReadTableReader(std::shared_ptr<void>&& input_source,
                                 std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                                 std::shared_ptr<ReadTableSchemaDescription> const& field_locations,
                                 SchemaMetadataDescription&& schema_metadata,
                                 arrow::MemoryPool* pool)
        : TableReader(std::move(input_source), std::move(reader), std::move(schema_metadata), pool),
          m_field_locations(field_locations) {}

Result<ReadTableRecordBatch> ReadTableReader::read_record_batch(std::size_t i) const {
    auto record_batch = reader()->ReadRecordBatch(i);
    if (!record_batch.ok()) {
        return record_batch.status();
    }
    return ReadTableRecordBatch{std::move(*record_batch), m_field_locations};
}

Status ReadTableReader::build_read_id_lookup() {
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
    std::sort(file_read_ids.begin(), file_read_ids.end(),
              [](auto const& a, auto const& b) { return a.id < b.id; });

    // Move data out now we successfully build the index:
    m_sorted_file_read_ids = std::move(file_read_ids);

    return Status::OK();
}

Result<std::vector<TraversalStep>> ReadTableReader::search_for_read_ids(
        ReadIdSearchInput const& search_input,
        TraversalType sort_order,
        std::size_t* successful_find_count) {
    ARROW_RETURN_NOT_OK(build_read_id_lookup());

    std::vector<TraversalStep> output_steps;
    output_steps.resize(search_input.read_id_count());

    std::size_t successes = 0;
    std::size_t failures = 0;

    auto file_ids_current_it = m_sorted_file_read_ids.begin();
    auto const file_ids_end = m_sorted_file_read_ids.end();
    for (std::size_t i = 0; i < output_steps.size(); ++i) {
        auto const& search_item = search_input[i];

        // Increment file pointer while less than the search term:
        while (file_ids_current_it->id < search_item.id && file_ids_current_it != file_ids_end) {
            ++file_ids_current_it;
        }

        // If we found it record the location:
        if (file_ids_current_it->id == search_item.id) {
            output_steps[successes].batch = file_ids_current_it->batch;
            output_steps[successes].batch_row = file_ids_current_it->batch_row;
            output_steps[successes].original_index = search_item.index;
            successes += 1;
        } else {
            // Find the location of the next failure record:
            auto failure_pt = output_steps.end() - 1 - failures;
            // Otherwise record a failure, at the back of the array:
            failure_pt->batch = std::numeric_limits<std::size_t>::max();
            failure_pt->batch_row = std::numeric_limits<std::size_t>::max();
            failure_pt->original_index = search_item.index;
            failures += 1;
        }
    }

    // Sort output as requested by user:
    switch (sort_order) {
    case TraversalType::read_efficient:
        std::sort(output_steps.begin(), output_steps.end(), [](auto const& a, auto const& b) {
            return std::make_tuple(a.batch, a.batch_row) < std::make_tuple(b.batch, b.batch_row);
        });

    case TraversalType::original_order:
        std::sort(output_steps.begin(), output_steps.end(),
                  [](auto const& a, auto const& b) { return a.original_index < b.original_index; });
    }

    if (successful_find_count) {
        *successful_find_count = successes;
    }

    return output_steps;
}

//---------------------------------------------------------------------------------------------------------------------

Result<ReadTableReader> make_read_table_reader(
        std::shared_ptr<arrow::io::RandomAccessFile> const& input,
        arrow::MemoryPool* pool) {
    arrow::ipc::IpcReadOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::ipc::RecordBatchFileReader::Open(input.get(), options));

    auto read_metadata_key_values = reader->schema()->metadata();
    if (!read_metadata_key_values) {
        return Status::IOError("Missing metadata on read table schema");
    }
    ARROW_ASSIGN_OR_RAISE(auto read_metadata,
                          read_schema_key_value_metadata(read_metadata_key_values));
    ARROW_ASSIGN_OR_RAISE(auto field_locations, read_read_table_schema(reader->schema()));

    return ReadTableReader({input}, std::move(reader), field_locations, std::move(read_metadata),
                           pool);
}

}  // namespace mkr