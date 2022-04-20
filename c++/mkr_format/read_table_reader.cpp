#include "mkr_format/read_table_reader.h"

#include "mkr_format/read_table_utils.h"
#include "mkr_format/schema_metadata.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

#include <iostream>

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

    std::chrono::system_clock::time_point get_timestamp(int field, std::int16_t row_index) const {
        auto field_array =
                std::static_pointer_cast<arrow::TimestampArray>(m_struct_data->field(field));
        assert(field_array);
        auto since_epoch_ms = field_array->Value(row_index);
        return std::chrono::system_clock::time_point() + std::chrono::milliseconds(since_epoch_ms);
    }

    std::map<std::string, std::string> get_string_map(int field, std::int16_t row_index) const {
        auto field_array = std::static_pointer_cast<arrow::MapArray>(m_struct_data->field(field));
        auto offsets = std::static_pointer_cast<arrow::Int32Array>(field_array->offsets());
        auto keys = std::static_pointer_cast<arrow::StringArray>(field_array->keys());
        auto values = std::static_pointer_cast<arrow::StringArray>(field_array->items());

        std::map<std::string, std::string> result;
        for (std::size_t i = offsets->Value(row_index); i < offsets->Value(row_index + 1); ++i) {
            result[keys->Value(i).to_string()] = values->Value(i).to_string();
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

PoreData ReadTableRecordBatch::get_pore(std::int16_t pore_index) const {
    auto pore_data = std::static_pointer_cast<arrow::StructArray>(pore_column()->dictionary());
    StructHelper hlp(pore_data);

    return PoreData{hlp.get_uint16(m_field_locations->pore_fields.channel, pore_index),
                    hlp.get_uint8(m_field_locations->pore_fields.well, pore_index),
                    hlp.get_string(m_field_locations->pore_fields.pore_type, pore_index)};
}

CalibrationData ReadTableRecordBatch::get_calibration(std::int16_t calibration_index) const {
    auto calibration_data =
            std::static_pointer_cast<arrow::StructArray>(calibration_column()->dictionary());
    StructHelper hlp(calibration_data);

    return CalibrationData{
            hlp.get_float(m_field_locations->calibration_fields.offset, calibration_index),
            hlp.get_float(m_field_locations->calibration_fields.scale, calibration_index)};
}

EndReasonData ReadTableRecordBatch::get_end_reason(std::int16_t end_reason_index) const {
    auto end_reason_data =
            std::static_pointer_cast<arrow::StructArray>(end_reason_column()->dictionary());
    StructHelper hlp(end_reason_data);

    return EndReasonData{
            hlp.get_string(m_field_locations->end_reason_fields.end_reason, end_reason_index),
            hlp.get_boolean(m_field_locations->end_reason_fields.forced, end_reason_index),
    };
}

RunInfoData ReadTableRecordBatch::get_run_info(std::int16_t run_info_index) const {
    auto run_info_data =
            std::static_pointer_cast<arrow::StructArray>(run_info_column()->dictionary());
    StructHelper hlp(run_info_data);

    return RunInfoData{
            hlp.get_string(m_field_locations->run_info_fields.acquisition_id, run_info_index),
            hlp.get_timestamp(m_field_locations->run_info_fields.acquisition_start_time,
                              run_info_index),
            hlp.get_int16(m_field_locations->run_info_fields.adc_max, run_info_index),
            hlp.get_int16(m_field_locations->run_info_fields.adc_min, run_info_index),
            hlp.get_string_map(m_field_locations->run_info_fields.context_tags, run_info_index),
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
            hlp.get_string_map(m_field_locations->run_info_fields.tracking_id, run_info_index)};
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