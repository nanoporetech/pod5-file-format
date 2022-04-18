#include "mkr_format/c_api.h"

#include "mkr_format/file_reader.h"
#include "mkr_format/file_writer.h"
#include "mkr_format/read_table_reader.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/type.h>

#include <chrono>

struct MkrFileReader {
    MkrFileReader(std::unique_ptr<mkr::FileReader> reader_) : reader(std::move(reader_)) {}
    std::unique_ptr<mkr::FileReader> reader;
};

struct MkrFileWriter {
    MkrFileWriter(std::unique_ptr<mkr::FileWriter> writer_) : writer(std::move(writer_)) {}
    std::unique_ptr<mkr::FileWriter> writer;
};

struct MkrReadRecordBatch {
    MkrReadRecordBatch(mkr::ReadTableRecordBatch&& batch_) : batch(std::move(batch_)) {}
    mkr::ReadTableRecordBatch batch;
};

extern "C" {

//---------------------------------------------------------------------------------------------------------------------
void mkr_init() { mkr::register_extension_types(); }

void mkr_terminate() { mkr::unregister_extension_types(); }

//---------------------------------------------------------------------------------------------------------------------
mkr_error_t g_mkr_error_no;
std::string g_mkr_error_string;

void mkr_reset_error() {
    g_mkr_error_no = mkr_error_t::MKR_OK;
    g_mkr_error_string.clear();
}

void mkr_set_error(arrow::Status status) {
    g_mkr_error_no = (mkr_error_t)status.code();
    g_mkr_error_string = status.ToString();
}

mkr_error_t mkr_get_error_no() { return g_mkr_error_no; }

char const* mkr_get_error_string() { return g_mkr_error_string.c_str(); }

#define MKR_C_RETURN_NOT_OK(result) \
    if (!result.ok()) {             \
        mkr_set_error(result);      \
        return g_mkr_error_no;      \
    }

#define MKR_C_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr) \
    auto&& result_name = (rexpr);                           \
    if (!(result_name).ok()) {                              \
        mkr_set_error((result_name).status());              \
        return g_mkr_error_no;                              \
    }                                                       \
    lhs = std::move(result_name).ValueUnsafe();

#define MKR_C_ASSIGN_OR_RAISE(lhs, rexpr)                                                     \
    MKR_C_ASSIGN_OR_RAISE_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, \
                               rexpr);

//---------------------------------------------------------------------------------------------------------------------
bool check_string_not_empty(char const* str) {
    if (!str) {
        mkr_set_error(arrow::Status::Invalid("null string passed to C API"));
        return false;
    }

    if (strlen(str) == 0) {
        mkr_set_error(arrow::Status::Invalid("empty string passed to C API"));
        return false;
    }

    return true;
}

bool check_not_null(void const* ptr) {
    if (!ptr) {
        mkr_set_error(arrow::Status::Invalid("null passed to C API"));
        return false;
    }
    return true;
}

bool check_file_not_null(void const* file) {
    if (!file) {
        mkr_set_error(arrow::Status::Invalid("null file passed to C API"));
        return false;
    }
    return true;
}

bool check_output_pointer_not_null(void const* output) {
    if (!output) {
        mkr_set_error(arrow::Status::Invalid("null output parameter passed to C API"));
        return false;
    }
    return true;
}

//---------------------------------------------------------------------------------------------------------------------
MkrFileReader* mkr_open_split_file(char const* signal_filename, char const* reads_filename) {
    mkr_reset_error();

    if (!check_string_not_empty(signal_filename) || !check_string_not_empty(reads_filename)) {
        return nullptr;
    }

    auto internal_reader = mkr::open_split_file_reader(signal_filename, reads_filename, {});
    if (!internal_reader.ok()) {
        mkr_set_error(internal_reader.status());
        return nullptr;
    }

    auto reader = std::make_unique<MkrFileReader>(std::move(*internal_reader));
    return reader.release();
}

MkrFileReader* mkr_open_combined_file(char const* filename) {
    mkr_reset_error();

    if (!check_string_not_empty(filename)) {
        return nullptr;
    }

    auto internal_reader = mkr::open_combined_file_reader(filename, {});
    if (!internal_reader.ok()) {
        mkr_set_error(internal_reader.status());
        return nullptr;
    }

    auto reader = std::make_unique<MkrFileReader>(std::move(*internal_reader));
    return reader.release();
}

mkr_error_t mkr_close_and_free_reader(MkrFileReader* file) {
    mkr_reset_error();

    std::unique_ptr<MkrFileReader> ptr{file};
    ptr.reset();
    return MKR_OK;
}

mkr_error_t mkr_get_read_batch_count(size_t* count, MkrFileReader* reader) {
    mkr_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(count)) {
        return g_mkr_error_no;
    }

    *count = reader->reader->num_read_record_batches();
    return MKR_OK;
}

mkr_error_t mkr_get_read_batch(MkrReadRecordBatch** batch, MkrFileReader* reader, size_t index) {
    mkr_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(batch)) {
        return g_mkr_error_no;
    }

    MKR_C_ASSIGN_OR_RAISE(auto internal_batch, reader->reader->read_read_record_batch(index));

    auto wrapped_batch = std::make_unique<MkrReadRecordBatch>(std::move(internal_batch));

    *batch = wrapped_batch.release();
    return MKR_OK;
}

mkr_error_t mkr_free_read_batch(MkrReadRecordBatch* batch) {
    mkr_reset_error();

    if (!check_not_null(batch)) {
        return g_mkr_error_no;
    }

    std::unique_ptr<MkrReadRecordBatch> ptr{batch};
    ptr.reset();
    return MKR_OK;
}

mkr_error_t mkr_get_read_batch_row_count(size_t* count, MkrReadRecordBatch* batch) {
    mkr_reset_error();

    if (!check_not_null(batch) || !check_output_pointer_not_null(count)) {
        return g_mkr_error_no;
    }

    *count = batch->batch.num_rows();
    return MKR_OK;
}

mkr_error_t mkr_get_read_batch_row_info(MkrReadRecordBatch* batch,
                                        size_t row,
                                        uint8_t* read_id,
                                        int16_t* pore,
                                        int16_t* calibration,
                                        uint32_t* read_number,
                                        uint64_t* start_sample,
                                        float* median_before,
                                        int16_t* end_reason,
                                        int16_t* run_info,
                                        int64_t* signal_row_count) {
    mkr_reset_error();

    if (!check_not_null(batch) || !check_output_pointer_not_null(read_id) ||
        !check_output_pointer_not_null(pore) || !check_output_pointer_not_null(calibration) ||
        !check_output_pointer_not_null(read_number) ||
        !check_output_pointer_not_null(start_sample) ||
        !check_output_pointer_not_null(median_before) ||
        !check_output_pointer_not_null(end_reason) || !check_output_pointer_not_null(run_info) ||
        !check_output_pointer_not_null(signal_row_count)) {
        return g_mkr_error_no;
    }

    auto read_id_col = batch->batch.read_id_column();
    auto read_id_val = read_id_col->Value(row);
    std::copy(read_id_val.begin(), read_id_val.end(), read_id);

    auto read_number_col = batch->batch.read_number_column();
    *read_number = read_number_col->Value(row);

    auto start_sample_col = batch->batch.start_sample_column();
    *start_sample = start_sample_col->Value(row);

    auto median_before_col = batch->batch.median_before_column();
    *median_before = median_before_col->Value(row);

    auto pore_col =
            std::static_pointer_cast<arrow::Int16Array>(batch->batch.pore_column()->indices());
    *pore = pore_col->Value(row);
    auto calibration_col = std::static_pointer_cast<arrow::Int16Array>(
            batch->batch.calibration_column()->indices());
    *calibration = calibration_col->Value(row);
    auto end_reason_col = std::static_pointer_cast<arrow::Int16Array>(
            batch->batch.end_reason_column()->indices());
    *end_reason = end_reason_col->Value(row);
    auto run_info_col =
            std::static_pointer_cast<arrow::Int16Array>(batch->batch.run_info_column()->indices());
    *run_info = run_info_col->Value(row);

    auto signal_col = batch->batch.signal_column();
    *signal_row_count = signal_col->value_slice(row)->length();

    return MKR_OK;
}

mkr_error_t mkr_get_pore(MkrReadRecordBatch* batch,
                         int16_t pore,
                         uint16_t* channel,
                         uint8_t* well,
                         char** pore_type) {
    mkr_reset_error();

    if (!check_not_null(batch) || !check_output_pointer_not_null(channel) ||
        !check_output_pointer_not_null(well) || !check_output_pointer_not_null(pore_type)) {
        return g_mkr_error_no;
    }

    auto pore_data = batch->batch.get_pore(pore);
    *channel = pore_data.channel;
    *well = pore_data.well;
    assert(false);
    //*pore_type = pore_data.pore_type.;
}

//---------------------------------------------------------------------------------------------------------------------
mkr::FileWriterOptions make_internal_writer_options(MkrWriterOptions const* options) {
    mkr::FileWriterOptions internal_options;
    if (options) {
        if (options->max_signal_chunk_size != 0) {
            internal_options.set_max_signal_chunk_size(options->max_signal_chunk_size);
        }

        if (options->signal_compression_type == UNCOMPRESSED_SIGNAL) {
            internal_options.set_signal_type(mkr::SignalType::UncompressedSignal);
        }
    }
    return internal_options;
}

MkrFileWriter* mkr_create_split_file(char const* signal_filename,
                                     char const* reads_filename,
                                     char const* writer_name,
                                     MkrWriterOptions const* options) {
    mkr_reset_error();

    if (!check_string_not_empty(signal_filename) || !check_string_not_empty(reads_filename) ||
        !check_string_not_empty(writer_name)) {
        return nullptr;
    }

    auto internal_writer = mkr::create_split_file_writer(
            signal_filename, reads_filename, writer_name, make_internal_writer_options(options));
    if (!internal_writer.ok()) {
        mkr_set_error(internal_writer.status());
        return nullptr;
    }

    auto writer = std::make_unique<MkrFileWriter>(std::move(*internal_writer));
    return writer.release();
}

MkrFileWriter* mkr_create_combined_file(char const* filename,
                                        char const* writer_name,
                                        MkrWriterOptions const* options) {
    mkr_reset_error();

    if (!check_string_not_empty(filename) || !check_string_not_empty(writer_name)) {
        return nullptr;
    }

    auto internal_writer = mkr::create_combined_file_writer(filename, writer_name,
                                                            make_internal_writer_options(options));
    if (!internal_writer.ok()) {
        mkr_set_error(internal_writer.status());
        return nullptr;
    }

    auto writer = std::make_unique<MkrFileWriter>(std::move(*internal_writer));
    return writer.release();
}

mkr_error_t mkr_close_and_free_writer(MkrFileWriter* file) {
    mkr_reset_error();

    std::unique_ptr<MkrFileWriter> ptr{file};
    ptr.reset();
    return MKR_OK;
}

mkr_error_t mkr_add_pore(int16_t* pore_index,
                         MkrFileWriter* file,
                         std::uint16_t channel,
                         std::uint8_t well,
                         char const* pore_type) {
    mkr_reset_error();

    if (!check_string_not_empty(pore_type) || !check_file_not_null(file) ||
        !check_output_pointer_not_null(pore_index)) {
        return g_mkr_error_no;
    }

    MKR_C_ASSIGN_OR_RAISE(*pore_index, file->writer->add_pore({channel, well, pore_type}));
    return MKR_OK;
}

mkr_error_t mkr_add_end_reason(int16_t* end_reason_index,
                               MkrFileWriter* file,
                               mkr_end_reason_t end_reason,
                               bool forced) {
    mkr_reset_error();

    if (!check_file_not_null(file) || !check_output_pointer_not_null(end_reason_index)) {
        return g_mkr_error_no;
    }

    mkr::EndReasonData::ReadEndReason end_reason_internal =
            mkr::EndReasonData::ReadEndReason::unknown;
    switch (end_reason) {
    case MKR_END_REASON_UNKNOWN:
        end_reason_internal = mkr::EndReasonData::ReadEndReason::unknown;
        break;
    case MKR_END_REASON_MUX_CHANGE:
        end_reason_internal = mkr::EndReasonData::ReadEndReason::mux_change;
        break;
    case MKR_END_REASON_UNBLOCK_MUX_CHANGE:
        end_reason_internal = mkr::EndReasonData::ReadEndReason::unblock_mux_change;
        break;
    case MKR_END_REASON_DATA_SERVICE_UNBLOCK_MUX_CHANGE:
        end_reason_internal = mkr::EndReasonData::ReadEndReason::data_service_unblock_mux_change;
        break;
    case MKR_END_REASON_SIGNAL_POSITIVE:
        end_reason_internal = mkr::EndReasonData::ReadEndReason::signal_positive;
        break;
    case MKR_END_REASON_SIGNAL_NEGATIVE:
        end_reason_internal = mkr::EndReasonData::ReadEndReason::signal_negative;
        break;
    default:
        mkr_set_error(
                arrow::Status::Invalid("out of range end reason passed to mkr_add_end_reason"));
        return g_mkr_error_no;
    }

    MKR_C_ASSIGN_OR_RAISE(*end_reason_index,
                          file->writer->add_end_reason({end_reason_internal, forced}));
    return MKR_OK;
}

mkr_error_t mkr_add_calibration(int16_t* calibration_index,
                                MkrFileWriter* file,
                                float offset,
                                float scale) {
    mkr_reset_error();

    if (!check_file_not_null(file) || !check_output_pointer_not_null(calibration_index)) {
        return g_mkr_error_no;
    }

    MKR_C_ASSIGN_OR_RAISE(*calibration_index, file->writer->add_calibration({offset, scale}));
    return MKR_OK;
}

mkr_error_t mkr_add_run_info(int16_t* run_info_index,
                             MkrFileWriter* file,
                             char const* acquisition_id,
                             std::int64_t acquisition_start_time_ms,
                             std::int16_t adc_max,
                             std::int16_t adc_min,
                             std::size_t context_tags_count,
                             char const** context_tags_keys,
                             char const** context_tags_values,
                             char const* experiment_name,
                             char const* flow_cell_id,
                             char const* flow_cell_product_code,
                             char const* protocol_name,
                             char const* protocol_run_id,
                             std::int64_t protocol_start_time_ms,
                             char const* sample_id,
                             std::uint16_t sample_rate,
                             char const* sequencing_kit,
                             char const* sequencer_position,
                             char const* sequencer_position_type,
                             char const* software,
                             char const* system_name,
                             char const* system_type,
                             std::size_t tracking_id_count,
                             char const** tracking_id_keys,
                             char const** tracking_id_values) {
    mkr_reset_error();

    if (!check_file_not_null(file) || !check_output_pointer_not_null(run_info_index)) {
        return g_mkr_error_no;
    }

    auto const parse_map =
            [](std::size_t tracking_id_count, char const** tracking_id_keys,
               char const** tracking_id_values) -> mkr::Result<std::map<std::string, std::string>> {
        std::map<std::string, std::string> result;
        for (std::size_t i = 0; i < tracking_id_count; ++i) {
            auto key = tracking_id_keys[i];
            auto value = tracking_id_values[i];
            if (key == nullptr || value == nullptr) {
                return arrow::Status::Invalid("null file passed to C API");
            }

            result[key] = value;
        }
        return result;
    };

    std::chrono::system_clock::time_point acquisition_start_time{};
    acquisition_start_time += std::chrono::milliseconds(acquisition_start_time_ms);
    std::chrono::system_clock::time_point protocol_start_time{};
    protocol_start_time += std::chrono::milliseconds(protocol_start_time_ms);
    MKR_C_ASSIGN_OR_RAISE(auto const context_tags,
                          parse_map(context_tags_count, context_tags_keys, context_tags_values));
    MKR_C_ASSIGN_OR_RAISE(auto const tracking_id,
                          parse_map(tracking_id_count, tracking_id_keys, tracking_id_values));

    MKR_C_ASSIGN_OR_RAISE(
            *run_info_index,
            file->writer->add_run_info(mkr::RunInfoData(
                    acquisition_id, acquisition_start_time, adc_max, adc_min, context_tags,
                    experiment_name, flow_cell_id, flow_cell_product_code, protocol_name,
                    protocol_run_id, protocol_start_time, sample_id, sample_rate, sequencing_kit,
                    sequencer_position, sequencer_position_type, software, system_name, system_type,
                    tracking_id)));

    return MKR_OK;
}

mkr_error_t mkr_add_read(MkrFileWriter* file,
                         uint8_t const* read_id,
                         int16_t pore,
                         int16_t calibration,
                         uint32_t read_number,
                         uint64_t start_sample,
                         float median_before,
                         int16_t end_reason,
                         int16_t run_info,
                         int16_t const* signal,
                         size_t signal_size) {
    mkr_reset_error();

    if (!check_file_not_null(file) || !check_not_null(read_id) || !check_not_null(signal)) {
        return g_mkr_error_no;
    }

    boost::uuids::uuid read_id_uuid;
    std::copy(read_id, read_id + sizeof(read_id_uuid), read_id_uuid.begin());

    MKR_C_RETURN_NOT_OK(file->writer->add_complete_read(
            mkr::ReadData{read_id_uuid, pore, calibration, read_number, start_sample, median_before,
                          end_reason, run_info},
            gsl::make_span(signal, signal_size)));
    return MKR_OK;
}
}

//---------------------------------------------------------------------------------------------------------------------
/*
std::shared_ptr<arrow::Schema> pyarrow_test() {
    return arrow::schema({
            arrow::field("signal", arrow::large_list(arrow::int16())),
            arrow::field("samples", arrow::uint32()),
    });
}
*/