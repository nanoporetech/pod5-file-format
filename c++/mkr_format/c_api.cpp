#include "mkr_format/c_api.h"

#include "mkr_format/file_reader.h"
#include "mkr_format/file_writer.h"
#include "mkr_format/read_table_reader.h"
#include "mkr_format/signal_compression.h"
#include "mkr_format/signal_table_reader.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>

#include <chrono>

//---------------------------------------------------------------------------------------------------------------------
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

namespace {
//---------------------------------------------------------------------------------------------------------------------
mkr_error_t g_mkr_error_no;
std::string g_mkr_error_string;
}  // namespace

extern "C" void mkr_set_error(arrow::Status status) {
    g_mkr_error_no = (mkr_error_t)status.code();
    g_mkr_error_string = status.ToString();
}

namespace {

void mkr_reset_error() {
    g_mkr_error_no = mkr_error_t::MKR_OK;
    g_mkr_error_string.clear();
}

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
template <typename RealType, typename GetData, typename CType>
mkr_error_t mkr_create_dict_type(MkrReadRecordBatch* batch,
                                 GetData data_fn,
                                 int16_t dict_index,
                                 CType** data_out) {
    mkr_reset_error();

    if (!check_not_null(batch) || !check_output_pointer_not_null(data_out)) {
        return g_mkr_error_no;
    }

    MKR_C_ASSIGN_OR_RAISE(auto internal_data, (batch->batch.*data_fn)(dict_index));
    auto data = std::make_unique<RealType>(std::move(internal_data));

    *data_out = data.release();
    return MKR_OK;
}

template <typename RealType, typename T>
mkr_error_t mkr_release_dict_type(T* dict_data) {
    mkr_reset_error();

    if (!check_not_null(dict_data)) {
        return g_mkr_error_no;
    }

    std::unique_ptr<RealType> helper(static_cast<RealType*>(dict_data));
    helper.reset();

    return MKR_OK;
}

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

}  // namespace

extern "C" {

//---------------------------------------------------------------------------------------------------------------------
mkr_error_t mkr_init() {
    mkr_reset_error();
    MKR_C_RETURN_NOT_OK(mkr::register_extension_types());
    return MKR_OK;
}

mkr_error_t mkr_terminate() {
    mkr_reset_error();
    MKR_C_RETURN_NOT_OK(mkr::unregister_extension_types());
    return MKR_OK;
}

mkr_error_t mkr_get_error_no() { return g_mkr_error_no; }

char const* mkr_get_error_string() { return g_mkr_error_string.c_str(); }

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

mkr_error_t mkr_get_signal_row_indices(MkrReadRecordBatch* batch,
                                       size_t row,
                                       int64_t signal_row_indices_count,
                                       uint64_t* signal_row_indices) {
    mkr_reset_error();

    if (!check_not_null(batch) || !check_output_pointer_not_null(signal_row_indices)) {
        return g_mkr_error_no;
    }

    auto const signal_col = batch->batch.signal_column();
    auto const& row_data =
            std::static_pointer_cast<arrow::UInt64Array>(signal_col->value_slice(row));

    if (signal_row_indices_count != row_data->length()) {
        mkr_set_error(mkr::Status::Invalid("Incorrect number of signal indices, expected ",
                                           row_data->length(), " received ",
                                           signal_row_indices_count));
        return g_mkr_error_no;
    }

    for (std::int64_t i = 0; i < signal_row_indices_count; ++i) {
        signal_row_indices[i] = row_data->Value(i);
    }

    return MKR_OK;
}

struct PoreDataCHelper : public PoreDictData {
    PoreDataCHelper(mkr::PoreData&& internal_data_) : internal_data(std::move(internal_data_)) {
        channel = internal_data.channel;
        well = internal_data.well;
        pore_type = internal_data.pore_type.c_str();
    }

    mkr::PoreData internal_data;
};

mkr_error_t mkr_get_pore(MkrReadRecordBatch* batch, int16_t pore, PoreDictData** pore_data) {
    return mkr_create_dict_type<PoreDataCHelper>(batch, &mkr::ReadTableRecordBatch::get_pore, pore,
                                                 pore_data);
}

mkr_error_t mkr_release_pore(PoreDictData* pore_data) {
    return mkr_release_dict_type<PoreDataCHelper>(pore_data);
}

struct CalibrationDataCHelper : public CalibrationDictData {
    CalibrationDataCHelper(mkr::CalibrationData&& internal_data_)
            : internal_data(std::move(internal_data_)) {
        offset = internal_data.offset;
        scale = internal_data.scale;
    }

    mkr::CalibrationData internal_data;
};

mkr_error_t mkr_get_calibration(MkrReadRecordBatch* batch,
                                int16_t calibration,
                                CalibrationDictData** calibration_data) {
    return mkr_create_dict_type<CalibrationDataCHelper>(
            batch, &mkr::ReadTableRecordBatch::get_calibration, calibration, calibration_data);
}

mkr_error_t mkr_release_calibration(CalibrationDictData* calibration_data) {
    return mkr_release_dict_type<CalibrationDictData>(calibration_data);
}

struct EndReasonDataCHelper : public EndReasonDictData {
    EndReasonDataCHelper(mkr::EndReasonData&& internal_data_)
            : internal_data(std::move(internal_data_)) {
        name = internal_data.name.c_str();
        forced = internal_data.forced;
    }

    mkr::EndReasonData internal_data;
};

mkr_error_t mkr_get_end_reason(MkrReadRecordBatch* batch,
                               int16_t end_reason,
                               EndReasonDictData** end_reason_data) {
    return mkr_create_dict_type<EndReasonDataCHelper>(
            batch, &mkr::ReadTableRecordBatch::get_end_reason, end_reason, end_reason_data);
}

mkr_error_t mkr_release_end_reason(EndReasonDictData* end_reason_data) {
    return mkr_release_dict_type<EndReasonDataCHelper>(end_reason_data);
}

struct RunInfoDataCHelper : public RunInfoDictData {
    struct InternalMapHelper {
        std::vector<char const*> keys;
        std::vector<char const*> values;
    };

    RunInfoDataCHelper(mkr::RunInfoData&& internal_data_)
            : internal_data(std::move(internal_data_)) {
        acquisition_id = internal_data.acquisition_id.c_str();
        acquisition_start_time_ms = internal_data.acquisition_start_time;
        adc_max = internal_data.adc_max;
        adc_min = internal_data.adc_min;
        context_tags = map_to_c(internal_data.context_tags, context_tags_helper);
        experiment_name = internal_data.experiment_name.c_str();
        flow_cell_id = internal_data.flow_cell_id.c_str();
        flow_cell_product_code = internal_data.flow_cell_product_code.c_str();
        protocol_name = internal_data.protocol_name.c_str();
        protocol_run_id = internal_data.protocol_name.c_str();
        protocol_start_time_ms = internal_data.protocol_start_time;
        sample_id = internal_data.sample_id.c_str();
        sample_rate = internal_data.sample_rate;
        sequencing_kit = internal_data.sequencing_kit.c_str();
        sequencer_position = internal_data.sequencer_position.c_str();
        sequencer_position_type = internal_data.sequencer_position_type.c_str();
        software = internal_data.software.c_str();
        system_name = internal_data.system_name.c_str();
        system_type = internal_data.system_type.c_str();
        tracking_id = map_to_c(internal_data.tracking_id, tracking_id_helper);
    }

    KeyValueData map_to_c(mkr::RunInfoData::MapType const& map, InternalMapHelper& helper) {
        helper.keys.reserve(map.size());
        helper.values.reserve(map.size());
        for (auto const& item : map) {
            helper.keys.push_back(item.first.c_str());
            helper.values.push_back(item.second.c_str());
        }

        KeyValueData result;
        result.size = helper.keys.size();
        result.keys = helper.keys.data();
        result.values = helper.values.data();
        return result;
    }

    mkr::RunInfoData internal_data;
    InternalMapHelper context_tags_helper;
    InternalMapHelper tracking_id_helper;
};

mkr_error_t mkr_get_run_info(MkrReadRecordBatch* batch,
                             int16_t run_info,
                             RunInfoDictData** run_info_data) {
    return mkr_create_dict_type<RunInfoDataCHelper>(batch, &mkr::ReadTableRecordBatch::get_run_info,
                                                    run_info, run_info_data);
}

MKR_FORMAT_EXPORT mkr_error_t mkr_release_run_info(RunInfoDictData* run_info_data) {
    return mkr_release_dict_type<RunInfoDataCHelper>(run_info_data);
}

class SignalRowInfoCHelper : public SignalRowInfo {
public:
    SignalRowInfoCHelper(mkr::SignalTableRecordBatch const& b) : batch(b) {}

    mkr::SignalTableRecordBatch batch;
};

mkr_error_t mkr_get_signal_row_info(MkrFileReader* reader,
                                    size_t signal_rows_count,
                                    uint64_t* signal_rows,
                                    SignalRowInfo** signal_row_info) {
    mkr_reset_error();

    if (!check_not_null(reader) || !check_output_pointer_not_null(signal_row_info)) {
        return g_mkr_error_no;
    }

    // Sort all rows first, in order to make searching faster.
    std::vector<std::uint64_t> signal_rows_sorted{signal_rows, signal_rows + signal_rows_count};
    std::sort(signal_rows_sorted.begin(), signal_rows_sorted.end());

    // Then loop all rows, forward.
    for (std::size_t completed_rows = 0;
         completed_rows <
         signal_rows_sorted.size();) {  // No increment here, we do it below when we succeed.
        auto const start_row = signal_rows_sorted[completed_rows];

        std::size_t batch_start_row = 0;
        MKR_C_ASSIGN_OR_RAISE(std::size_t row_batch, (reader->reader->signal_batch_for_row_id(
                                                             start_row, &batch_start_row)));
        MKR_C_ASSIGN_OR_RAISE(auto batch, reader->reader->read_signal_record_batch(row_batch));
        auto const batch_num_rows = batch.num_rows();

        // Try to find answers for as many of the rows as possible, incrementing for loop index when we succeed
        while (true) {
            auto const row = signal_rows_sorted[completed_rows];
            if (row >= (batch_start_row + batch_num_rows)) {
                break;
            }

            auto const batch_row_index = row - batch_start_row;

            auto output = std::make_unique<SignalRowInfoCHelper>(batch);

            output->batch_index = row_batch;
            output->batch_row_index = batch_row_index;

            auto samples = batch.samples_column();
            output->stored_sample_count = samples->Value(batch_row_index);
            MKR_C_ASSIGN_OR_RAISE(output->stored_byte_count,
                                  batch.samples_byte_count(batch_row_index));

            signal_row_info[completed_rows] = output.release();
            completed_rows += 1;
        }
    }

    return MKR_OK;
}

mkr_error_t mkr_free_signal_row_info(size_t signal_rows_count, SignalRowInfo_t** signal_row_info) {
    for (std::size_t i = 0; i < signal_rows_count; ++i) {
        std::unique_ptr<SignalRowInfoCHelper> helper(
                static_cast<SignalRowInfoCHelper*>(signal_row_info[i]));
        helper.reset();
    }
    return MKR_OK;
}

mkr_error_t mkr_get_signal(MkrFileReader* reader,
                           SignalRowInfo_t* row_info,
                           std::size_t sample_count,
                           std::int16_t* sample_data) {
    mkr_reset_error();

    if (!check_not_null(reader) || !check_not_null(row_info) ||
        !check_output_pointer_not_null(sample_data)) {
        return g_mkr_error_no;
    }

    SignalRowInfoCHelper* row_info_data = static_cast<SignalRowInfoCHelper*>(row_info);

    MKR_C_RETURN_NOT_OK(row_info_data->batch.extract_signal_row(
            row_info->batch_row_index, gsl::make_span(sample_data, sample_count)));

    return MKR_OK;
}

//---------------------------------------------------------------------------------------------------------------------
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
    MKR_C_RETURN_NOT_OK(ptr->writer->close());
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
                               int forced) {
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
                          file->writer->add_end_reason({end_reason_internal, forced != 0}));
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
               char const** tracking_id_values) -> mkr::Result<mkr::RunInfoData::MapType> {
        mkr::RunInfoData::MapType result;
        for (std::size_t i = 0; i < tracking_id_count; ++i) {
            auto key = tracking_id_keys[i];
            auto value = tracking_id_values[i];
            if (key == nullptr || value == nullptr) {
                return arrow::Status::Invalid("null file passed to C API");
            }

            result.emplace_back(key, value);
        }
        return result;
    };

    MKR_C_ASSIGN_OR_RAISE(auto const context_tags,
                          parse_map(context_tags_count, context_tags_keys, context_tags_values));
    MKR_C_ASSIGN_OR_RAISE(auto const tracking_id,
                          parse_map(tracking_id_count, tracking_id_keys, tracking_id_values));

    MKR_C_ASSIGN_OR_RAISE(
            *run_info_index,
            file->writer->add_run_info(mkr::RunInfoData(
                    acquisition_id, acquisition_start_time_ms, adc_max, adc_min, context_tags,
                    experiment_name, flow_cell_id, flow_cell_product_code, protocol_name,
                    protocol_run_id, protocol_start_time_ms, sample_id, sample_rate, sequencing_kit,
                    sequencer_position, sequencer_position_type, software, system_name, system_type,
                    tracking_id)));

    return MKR_OK;
}

mkr_error_t mkr_add_reads(MkrFileWriter* file,
                          uint32_t read_count,
                          read_id_t const* read_id,
                          int16_t const* pore,
                          int16_t const* calibration,
                          uint32_t const* read_number,
                          uint64_t const* start_sample,
                          float const* median_before,
                          int16_t const* end_reason,
                          int16_t const* run_info,
                          int16_t const** signal,
                          uint32_t const* signal_size) {
    mkr_reset_error();

    if (!check_file_not_null(file) || !check_not_null(read_id) || !check_not_null(pore) ||
        !check_not_null(calibration) || !check_not_null(read_number) ||
        !check_not_null(start_sample) || !check_not_null(median_before) ||
        !check_not_null(end_reason) || !check_not_null(run_info) || !check_not_null(signal) ||
        !check_not_null(signal_size)) {
        return g_mkr_error_no;
    }

    for (std::uint32_t read = 0; read < read_count; ++read) {
        boost::uuids::uuid read_id_uuid;
        std::copy(read_id[read], read_id[read] + sizeof(read_id_uuid), read_id_uuid.begin());

        MKR_C_RETURN_NOT_OK(file->writer->add_complete_read(
                mkr::ReadData{read_id_uuid, pore[read], calibration[read], read_number[read],
                              start_sample[read], median_before[read], end_reason[read],
                              run_info[read]},
                gsl::make_span(signal[read], signal_size[read])));
    }

    return MKR_OK;
}

mkr_error_t mkr_add_reads_pre_compressed(MkrFileWriter* file,
                                         uint32_t read_count,
                                         read_id_t const* read_id,
                                         int16_t const* pore,
                                         int16_t const* calibration,
                                         uint32_t const* read_number,
                                         uint64_t const* start_sample,
                                         float const* median_before,
                                         int16_t const* end_reason,
                                         int16_t const* run_info,
                                         char const*** compressed_signal,
                                         size_t const** compressed_signal_size,
                                         uint32_t const** sample_counts,
                                         size_t const* signal_chunk_count) {
    mkr_reset_error();

    if (!check_file_not_null(file) || !check_not_null(read_id) || !check_not_null(pore) ||
        !check_not_null(calibration) || !check_not_null(read_number) ||
        !check_not_null(start_sample) || !check_not_null(median_before) ||
        !check_not_null(end_reason) || !check_not_null(run_info) ||
        !check_not_null(compressed_signal) || !check_not_null(compressed_signal_size) ||
        !check_not_null(sample_counts) || !check_not_null(signal_chunk_count)) {
        return g_mkr_error_no;
    }

    for (std::uint32_t read = 0; read < read_count; ++read) {
        boost::uuids::uuid read_id_uuid;
        std::copy(read_id[read], read_id[read] + sizeof(read_id_uuid), read_id_uuid.begin());

        std::vector<std::uint64_t> signal_rows;
        for (std::size_t i = 0; i < signal_chunk_count[read]; ++i) {
            auto signal = compressed_signal[read][i];
            auto signal_size = compressed_signal_size[read][i];
            auto sample_count = sample_counts[read][i];
            MKR_C_ASSIGN_OR_RAISE(
                    auto row_id,
                    file->writer->add_pre_compressed_signal(
                            read_id_uuid,
                            gsl::make_span(signal, signal_size).as_span<std::uint8_t const>(),
                            sample_count));
            signal_rows.push_back(row_id);
        }

        MKR_C_RETURN_NOT_OK(file->writer->add_complete_read(
                mkr::ReadData{read_id_uuid, pore[read], calibration[read], read_number[read],
                              start_sample[read], median_before[read], end_reason[read],
                              run_info[read]},
                gsl::make_span(signal_rows)));
    }
    return MKR_OK;
}

mkr_error_t mkr_flush_signal_table(MkrFileWriter* file) {
    MKR_C_RETURN_NOT_OK(file->writer->flush_signal_table());
    return MKR_OK;
}

mkr_error_t mkr_flush_reads_table(MkrFileWriter* file) {
    MKR_C_RETURN_NOT_OK(file->writer->flush_reads_table());
    return MKR_OK;
}

size_t mkr_vbz_compressed_signal_max_size(size_t sample_count) {
    mkr_reset_error();
    return mkr::compressed_signal_max_size(sample_count);
}

mkr_error_t mkr_vbz_compress_signal(int16_t const* signal,
                                    size_t signal_size,
                                    char* compressed_signal_out,
                                    size_t* compressed_signal_size) {
    mkr_reset_error();

    if (!check_not_null(signal) || !check_output_pointer_not_null(compressed_signal_out) ||
        !check_output_pointer_not_null(compressed_signal_size)) {
        return g_mkr_error_no;
    }

    MKR_C_ASSIGN_OR_RAISE(auto buffer, mkr::compress_signal(gsl::make_span(signal, signal_size),
                                                            arrow::system_memory_pool()));

    if ((std::size_t)buffer->size() > *compressed_signal_size) {
        mkr_set_error(mkr::Status::Invalid("Compressed signal size (", buffer->size(),
                                           ") is greater than provided buffer size (",
                                           compressed_signal_size, ")"));
        return g_mkr_error_no;
    }

    std::copy(buffer->data(), buffer->data() + buffer->size(), compressed_signal_out);
    *compressed_signal_size = buffer->size();

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