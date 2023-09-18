#include "pod5_format/c_api.h"

#include "pod5_format/file_reader.h"
#include "pod5_format/file_writer.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/signal_compression.h"
#include "pod5_format/signal_table_reader.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <boost/uuid/uuid_io.hpp>

#include <chrono>
#include <iostream>

//---------------------------------------------------------------------------------------------------------------------
struct Pod5FileReader {
    Pod5FileReader(std::shared_ptr<pod5::FileReader> && reader_) : reader(std::move(reader_)) {}

    std::shared_ptr<pod5::FileReader> reader;
};

struct Pod5FileWriter {
    Pod5FileWriter(std::unique_ptr<pod5::FileWriter> && writer_) : writer(std::move(writer_)) {}

    std::unique_ptr<pod5::FileWriter> writer;
};

struct Pod5ReadRecordBatch {
    Pod5ReadRecordBatch(
        pod5::ReadTableRecordBatch && batch_,
        std::shared_ptr<pod5::FileReader> const & reader)
    : batch(std::move(batch_))
    , reader(reader)
    {
    }

    pod5::ReadTableRecordBatch batch;
    std::shared_ptr<pod5::FileReader> reader;
};

namespace {
//---------------------------------------------------------------------------------------------------------------------
pod5_error_t g_pod5_error_no;
std::string g_pod5_error_string;
}  // namespace

extern "C" void pod5_set_error(arrow::Status status)
{
    g_pod5_error_no = (pod5_error_t)status.code();
    g_pod5_error_string = status.ToString();
}

namespace {

void pod5_reset_error()
{
    g_pod5_error_no = pod5_error_t::POD5_OK;
    g_pod5_error_string.clear();
}

#define POD5_C_RETURN_NOT_OK(result)    \
    do {                                \
        ::arrow::Status __s = (result); \
        if (!__s.ok()) {                \
            pod5_set_error(__s);        \
            return g_pod5_error_no;     \
        }                               \
    } while (0)

#define POD5_C_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr) \
    auto && result_name = (rexpr);                           \
    if (!(result_name).ok()) {                               \
        pod5_set_error((result_name).status());              \
        return g_pod5_error_no;                              \
    }                                                        \
    lhs = std::move(result_name).ValueUnsafe();

#define POD5_C_ASSIGN_OR_RAISE(lhs, rexpr) \
    POD5_C_ASSIGN_OR_RAISE_IMPL(           \
        ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

//---------------------------------------------------------------------------------------------------------------------
bool check_string_not_empty(char const * str)
{
    if (!str) {
        pod5_set_error(arrow::Status::Invalid("null string passed to C API"));
        return false;
    }

    if (strlen(str) == 0) {
        pod5_set_error(arrow::Status::Invalid("empty string passed to C API"));
        return false;
    }

    return true;
}

bool check_not_null(void const * ptr)
{
    if (!ptr) {
        pod5_set_error(arrow::Status::Invalid("null passed to C API"));
        return false;
    }
    return true;
}

bool check_file_not_null(void const * file)
{
    if (!file) {
        pod5_set_error(arrow::Status::Invalid("null file passed to C API"));
        return false;
    }
    return true;
}

bool check_output_pointer_not_null(void const * output)
{
    if (!output) {
        pod5_set_error(arrow::Status::Invalid("null output parameter passed to C API"));
        return false;
    }
    return true;
}

//---------------------------------------------------------------------------------------------------------------------
pod5::FileWriterOptions make_internal_writer_options(Pod5WriterOptions const * options)
{
    pod5::FileWriterOptions internal_options;
    if (options) {
        if (options->max_signal_chunk_size != 0) {
            internal_options.set_max_signal_chunk_size(options->max_signal_chunk_size);
        }

        if (options->signal_compression_type == UNCOMPRESSED_SIGNAL) {
            internal_options.set_signal_type(pod5::SignalType::UncompressedSignal);
        }

        if (options->signal_table_batch_size != 0) {
            internal_options.set_signal_table_batch_size(options->signal_table_batch_size);
        }
        if (options->read_table_batch_size != 0) {
            internal_options.set_read_table_batch_size(options->read_table_batch_size);
        }
    }
    return internal_options;
}

pod5::FileReaderOptions make_internal_reader_options(Pod5ReaderOptions const * options)
{
    pod5::FileReaderOptions internal_options;
    if (options) {
        if (options->force_disable_file_mapping != false) {
            internal_options.set_force_disable_file_mapping(options->force_disable_file_mapping);
        }
    }
    return internal_options;
}

}  // namespace

extern "C" {

//---------------------------------------------------------------------------------------------------------------------
pod5_error_t pod5_init()
{
    pod5_reset_error();
    POD5_C_RETURN_NOT_OK(pod5::register_extension_types());
    return POD5_OK;
}

pod5_error_t pod5_terminate()
{
    pod5_reset_error();
    POD5_C_RETURN_NOT_OK(pod5::unregister_extension_types());
    return POD5_OK;
}

pod5_error_t pod5_get_error_no() { return g_pod5_error_no; }

char const * pod5_get_error_string() { return g_pod5_error_string.c_str(); }

//---------------------------------------------------------------------------------------------------------------------
Pod5FileReader * pod5_open_file(char const * filename)
{
    pod5_reset_error();

    if (!check_string_not_empty(filename)) {
        return nullptr;
    }

    auto internal_reader = pod5::open_file_reader(filename, {});
    if (!internal_reader.ok()) {
        pod5_set_error(internal_reader.status());
        return nullptr;
    }

    auto reader = std::make_unique<Pod5FileReader>(std::move(*internal_reader));
    return reader.release();
}

Pod5FileReader * pod5_open_file_options(char const * filename, Pod5ReaderOptions_t const * options)
{
    pod5_reset_error();

    if (!check_string_not_empty(filename) || !check_not_null(options)) {
        return nullptr;
    }

    auto internal_reader = pod5::open_file_reader(filename, make_internal_reader_options(options));
    if (!internal_reader.ok()) {
        pod5_set_error(internal_reader.status());
        return nullptr;
    }

    auto reader = std::make_unique<Pod5FileReader>(std::move(*internal_reader));
    return reader.release();
}

pod5_error_t pod5_close_and_free_reader(Pod5FileReader * file)
{
    pod5_reset_error();

    std::unique_ptr<Pod5FileReader> ptr{file};
    ptr.reset();
    return POD5_OK;
}

pod5_error_t pod5_get_file_info(Pod5FileReader_t * reader, FileInfo * file_info)
{
    pod5_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(file_info)) {
        return g_pod5_error_no;
    }

    auto const metadata = reader->reader->schema_metadata();

    std::copy(
        metadata.file_identifier.begin(),
        metadata.file_identifier.end(),
        file_info->file_identifier);

    file_info->version.major = metadata.writing_pod5_version.major_version();
    file_info->version.minor = metadata.writing_pod5_version.minor_version();
    file_info->version.revision = metadata.writing_pod5_version.revision_version();

    return POD5_OK;
}

pod5_error_t pod5_get_file_read_table_location(
    Pod5FileReader_t * reader,
    EmbeddedFileData_t * file_data)
{
    pod5_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(file_data)) {
        return g_pod5_error_no;
    }
    auto const & read_table_location = reader->reader->read_table_location();

    file_data->offset = read_table_location.offset;
    file_data->offset = read_table_location.offset;
    file_data->length = read_table_location.size;
    return POD5_OK;
}

pod5_error_t pod5_get_file_signal_table_location(
    Pod5FileReader_t * reader,
    EmbeddedFileData_t * file_data)
{
    pod5_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(file_data)) {
        return g_pod5_error_no;
    }
    auto const signal_table_location = reader->reader->signal_table_location();

    file_data->offset = signal_table_location.offset;
    file_data->length = signal_table_location.size;
    return POD5_OK;
}

pod5_error_t pod5_get_file_run_info_table_location(
    Pod5FileReader_t * reader,
    EmbeddedFileData_t * file_data)
{
    pod5_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(file_data)) {
        return g_pod5_error_no;
    }
    auto const run_info_table_location = reader->reader->run_info_table_location();

    file_data->offset = run_info_table_location.offset;
    file_data->length = run_info_table_location.size;
    return POD5_OK;
}

pod5_error_t pod5_get_read_count(Pod5FileReader_t * reader, size_t * count)
{
    pod5_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(count)) {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(*count, reader->reader->read_count());

    return POD5_OK;
}

pod5_error_t pod5_get_read_ids(Pod5FileReader_t * reader, size_t count, read_id_t * read_ids)
{
    pod5_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(read_ids)) {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(auto read_count, reader->reader->read_count());
    if (count < read_count) {
        pod5_set_error(arrow::Status::Invalid("array to short to receive read ids"));
        return g_pod5_error_no;
    }

    std::size_t count_so_far = 0;
    for (std::size_t i = 0; i < reader->reader->num_read_record_batches(); ++i) {
        POD5_C_ASSIGN_OR_RAISE(auto const batch, reader->reader->read_read_record_batch(i));

        auto const read_id_column = batch.read_id_column();
        auto raw_data = reinterpret_cast<uint8_t const *>(read_id_column->raw_values());
        std::copy(
            raw_data,
            raw_data + (read_id_column->length() * sizeof(read_id_t)),
            reinterpret_cast<uint8_t *>(read_ids + count_so_far));
        count_so_far += read_id_column->length();
    }

    return POD5_OK;
}

pod5_error_t pod5_plan_traversal(
    Pod5FileReader_t * reader,
    uint8_t const * read_id_array,
    size_t read_id_count,
    uint32_t * batch_counts,
    uint32_t * batch_rows,
    size_t * find_success_count_out)
{
    pod5_reset_error();

    if (!check_file_not_null(reader) || !check_not_null(read_id_array)
        || !check_output_pointer_not_null(batch_counts)
        || !check_output_pointer_not_null(batch_rows))
    {
        return g_pod5_error_no;
    }

    auto search_input = pod5::ReadIdSearchInput(
        gsl::make_span(reinterpret_cast<boost::uuids::uuid const *>(read_id_array), read_id_count));

    POD5_C_ASSIGN_OR_RAISE(
        auto find_success_count,
        reader->reader->search_for_read_ids(
            search_input,
            gsl::make_span(batch_counts, reader->reader->num_read_record_batches()),
            gsl::make_span(batch_rows, read_id_count)));

    if (find_success_count_out) {
        *find_success_count_out = find_success_count;
    }

    return POD5_OK;
}

pod5_error_t pod5_get_read_batch_count(size_t * count, Pod5FileReader * reader)
{
    pod5_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(count)) {
        return g_pod5_error_no;
    }

    *count = reader->reader->num_read_record_batches();
    return POD5_OK;
}

pod5_error_t
pod5_get_read_batch(Pod5ReadRecordBatch ** batch, Pod5FileReader * reader, size_t index)
{
    pod5_reset_error();

    if (!check_file_not_null(reader) || !check_output_pointer_not_null(batch)) {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(auto internal_batch, reader->reader->read_read_record_batch(index));

    auto wrapped_batch =
        std::make_unique<Pod5ReadRecordBatch>(std::move(internal_batch), reader->reader);

    *batch = wrapped_batch.release();
    return POD5_OK;
}

pod5_error_t pod5_free_read_batch(Pod5ReadRecordBatch * batch)
{
    pod5_reset_error();

    if (!check_not_null(batch)) {
        return g_pod5_error_no;
    }

    std::unique_ptr<Pod5ReadRecordBatch> ptr{batch};
    ptr.reset();
    return POD5_OK;
}

pod5_error_t pod5_get_read_batch_row_count(size_t * count, Pod5ReadRecordBatch * batch)
{
    pod5_reset_error();

    if (!check_not_null(batch) || !check_output_pointer_not_null(count)) {
        return g_pod5_error_no;
    }

    *count = batch->batch.num_rows();
    return POD5_OK;
}

pod5_error_t check_row_index_and_set_error(size_t row, size_t batch_size)
{
    if (row >= batch_size) {
        pod5_set_error(arrow::Status::IndexError(
            "Invalid index into batch. Index ", row, " with batch size ", batch_size));
        return g_pod5_error_no;
    }

    return POD5_OK;
}

pod5_error_t pod5_get_read_batch_row_info_data(
    Pod5ReadRecordBatch_t * batch,
    size_t row,
    uint16_t struct_version,
    void * row_data,
    uint16_t * read_table_version)
{
    if (!check_not_null(batch) || !check_output_pointer_not_null(row_data)) {
        return g_pod5_error_no;
    }

    static_assert(
        READ_BATCH_ROW_INFO_VERSION == READ_BATCH_ROW_INFO_VERSION_3,
        "New versions must be explicitly loaded");

    if (struct_version == READ_BATCH_ROW_INFO_VERSION_3) {
        auto typed_row_data = static_cast<ReadBatchRowInfoV3 *>(row_data);

        POD5_C_ASSIGN_OR_RAISE(auto cols, batch->batch.columns());

        // Inform the caller of the version of the input table.
        *read_table_version = cols.table_version.as_int();

        if (check_row_index_and_set_error(row, cols.read_id->length()) != POD5_OK) {
            return g_pod5_error_no;
        }

        auto read_id_val = cols.read_id->Value(row);
        std::copy(read_id_val.begin(), read_id_val.end(), typed_row_data->read_id);

        typed_row_data->read_number = cols.read_number->Value(row);
        typed_row_data->start_sample = cols.start_sample->Value(row);
        typed_row_data->median_before = cols.median_before->Value(row);
        typed_row_data->channel = cols.channel->Value(row);
        typed_row_data->well = cols.well->Value(row);
        auto pore_type_col = std::static_pointer_cast<arrow::Int16Array>(cols.pore_type->indices());
        typed_row_data->pore_type = pore_type_col->Value(row);
        typed_row_data->calibration_offset = cols.calibration_offset->Value(row);
        typed_row_data->calibration_scale = cols.calibration_scale->Value(row);
        auto end_reason_col =
            std::static_pointer_cast<arrow::Int16Array>(cols.end_reason->indices());
        typed_row_data->end_reason = end_reason_col->Value(row);
        typed_row_data->end_reason_forced = cols.end_reason_forced->Value(row);
        auto run_info_col = std::static_pointer_cast<arrow::Int16Array>(cols.run_info->indices());
        typed_row_data->run_info = run_info_col->Value(row);
        typed_row_data->num_minknow_events = cols.num_minknow_events->Value(row);
        typed_row_data->tracked_scaling_scale = cols.tracked_scaling_scale->Value(row);
        typed_row_data->tracked_scaling_shift = cols.tracked_scaling_shift->Value(row);
        typed_row_data->predicted_scaling_scale = cols.predicted_scaling_scale->Value(row);
        typed_row_data->predicted_scaling_shift = cols.predicted_scaling_shift->Value(row);
        typed_row_data->num_reads_since_mux_change = cols.num_reads_since_mux_change->Value(row);
        typed_row_data->time_since_mux_change = cols.time_since_mux_change->Value(row);

        typed_row_data->signal_row_count = cols.signal->value_length(row);
        typed_row_data->num_samples = cols.num_samples->Value(row);
    } else {
        pod5_set_error(
            arrow::Status::Invalid("Invalid struct version '", struct_version, "' passed"));
        return g_pod5_error_no;
    }

    return POD5_OK;
}

pod5_error_t pod5_get_signal_row_indices(
    Pod5ReadRecordBatch * batch,
    size_t row,
    int64_t signal_row_indices_count,
    uint64_t * signal_row_indices)
{
    pod5_reset_error();

    if (!check_not_null(batch) || !check_output_pointer_not_null(signal_row_indices)) {
        return g_pod5_error_no;
    }

    auto const signal_col = batch->batch.signal_column();
    if (check_row_index_and_set_error(row, signal_col->length()) != POD5_OK) {
        return g_pod5_error_no;
    }

    auto const & row_data =
        std::static_pointer_cast<arrow::UInt64Array>(signal_col->value_slice(row));

    if (signal_row_indices_count != row_data->length()) {
        pod5_set_error(pod5::Status::Invalid(
            "Incorrect number of signal indices, expected ",
            row_data->length(),
            " received ",
            signal_row_indices_count));
        return g_pod5_error_no;
    }

    for (std::int64_t i = 0; i < signal_row_indices_count; ++i) {
        signal_row_indices[i] = row_data->Value(i);
    }

    return POD5_OK;
}

pod5_error_t pod5_get_calibration_extra_info(
    Pod5ReadRecordBatch_t * batch,
    size_t row,
    CalibrationExtraData_t * calibration_extra_data)
{
    pod5_reset_error();

    if (!check_not_null(batch) || !check_output_pointer_not_null(calibration_extra_data)) {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(auto cols, batch->batch.columns());

    if (check_row_index_and_set_error(row, cols.calibration_scale->length()) != POD5_OK) {
        return g_pod5_error_no;
    }

    auto scale = cols.calibration_scale->Value(row);
    auto const run_info_dict_index =
        std::static_pointer_cast<arrow::Int16Array>(cols.run_info->indices())->Value(row);

    POD5_C_ASSIGN_OR_RAISE(
        auto const acquisition_id, batch->batch.get_run_info(run_info_dict_index));
    POD5_C_ASSIGN_OR_RAISE(auto run_info_data, batch->reader->find_run_info(acquisition_id));

    calibration_extra_data->digitisation = run_info_data->adc_max - run_info_data->adc_min + 1;
    calibration_extra_data->range = scale * calibration_extra_data->digitisation;

    return POD5_OK;
}

struct RunInfoDataCHelper : public RunInfoDictData {
    struct InternalMapHelper {
        std::vector<char const *> keys;
        std::vector<char const *> values;
    };

    RunInfoDataCHelper(std::shared_ptr<pod5::RunInfoData const> const & internal_data_)
    : internal_data(internal_data_)
    {
        acquisition_id = internal_data->acquisition_id.c_str();
        acquisition_start_time_ms = internal_data->acquisition_start_time;
        adc_max = internal_data->adc_max;
        adc_min = internal_data->adc_min;
        context_tags = map_to_c(internal_data->context_tags, context_tags_helper);
        experiment_name = internal_data->experiment_name.c_str();
        flow_cell_id = internal_data->flow_cell_id.c_str();
        flow_cell_product_code = internal_data->flow_cell_product_code.c_str();
        protocol_name = internal_data->protocol_name.c_str();
        protocol_run_id = internal_data->protocol_run_id.c_str();
        protocol_start_time_ms = internal_data->protocol_start_time;
        sample_id = internal_data->sample_id.c_str();
        sample_rate = internal_data->sample_rate;
        sequencing_kit = internal_data->sequencing_kit.c_str();
        sequencer_position = internal_data->sequencer_position.c_str();
        sequencer_position_type = internal_data->sequencer_position_type.c_str();
        software = internal_data->software.c_str();
        system_name = internal_data->system_name.c_str();
        system_type = internal_data->system_type.c_str();
        tracking_id = map_to_c(internal_data->tracking_id, tracking_id_helper);
    }

    KeyValueData map_to_c(pod5::RunInfoData::MapType const & map, InternalMapHelper & helper)
    {
        helper.keys.reserve(map.size());
        helper.values.reserve(map.size());
        for (auto const & item : map) {
            helper.keys.push_back(item.first.c_str());
            helper.values.push_back(item.second.c_str());
        }

        KeyValueData result;
        result.size = helper.keys.size();
        result.keys = helper.keys.data();
        result.values = helper.values.data();
        return result;
    }

    std::shared_ptr<pod5::RunInfoData const> internal_data;
    InternalMapHelper context_tags_helper;
    InternalMapHelper tracking_id_helper;
};

pod5_error_t
pod5_get_run_info(Pod5ReadRecordBatch * batch, int16_t run_info, RunInfoDictData ** run_info_data)
{
    pod5_reset_error();

    if (!check_not_null(batch) || !check_output_pointer_not_null(run_info_data)) {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(auto const acquisition_id, batch->batch.get_run_info(run_info));
    POD5_C_ASSIGN_OR_RAISE(auto internal_data, batch->reader->find_run_info(acquisition_id));

    auto data = std::make_unique<RunInfoDataCHelper>(std::move(internal_data));
    *run_info_data = data.release();
    return POD5_OK;
}

pod5_error_t pod5_get_file_run_info(
    Pod5FileReader_t * file,
    run_info_index_t run_info_index,
    RunInfoDictData_t ** run_info_data)
{
    pod5_reset_error();

    if (!check_not_null(file) || !check_output_pointer_not_null(run_info_data)) {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(auto internal_data, file->reader->get_run_info(run_info_index));

    auto data = std::make_unique<RunInfoDataCHelper>(std::move(internal_data));
    *run_info_data = data.release();
    return POD5_OK;
}

pod5_error_t pod5_free_run_info(RunInfoDictData_t * run_info_data)
{
    pod5_reset_error();

    if (!check_not_null(run_info_data)) {
        return g_pod5_error_no;
    }

    std::unique_ptr<RunInfoDataCHelper> helper(static_cast<RunInfoDataCHelper *>(run_info_data));
    helper.reset();

    return POD5_OK;
}

pod5_error_t pod5_release_run_info(RunInfoDictData * run_info_data)
{
    return pod5_free_run_info(run_info_data);
}

pod5_error_t pod5_get_file_run_info_count(
    Pod5FileReader_t * file,
    run_info_index_t * run_info_count)
{
    pod5_reset_error();

    if (!check_not_null(file)) {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(*run_info_count, file->reader->get_run_info_count());

    return POD5_OK;
}

pod5_error_t pod5_get_end_reason(
    Pod5ReadRecordBatch_t * batch,
    int16_t end_reason,
    pod5_end_reason * end_reason_value,
    char * end_reason_string_value,
    size_t * end_reason_string_value_size)
{
    pod5_reset_error();

    if (!check_output_pointer_not_null(end_reason_value)
        || !check_output_pointer_not_null(end_reason_string_value)
        || !check_output_pointer_not_null(end_reason_string_value_size))
    {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(auto const end_reason_val, batch->batch.get_end_reason(end_reason));
    auto const input_buffer_len = *end_reason_string_value_size;
    *end_reason_string_value_size = end_reason_val.second.size() + 1;
    if (end_reason_val.second.size() >= input_buffer_len) {
        return POD5_ERROR_STRING_NOT_LONG_ENOUGH;
    }

    switch (end_reason_val.first) {
    case pod5::ReadEndReason::mux_change:
        *end_reason_value = POD5_END_REASON_MUX_CHANGE;
        break;
    case pod5::ReadEndReason::unblock_mux_change:
        *end_reason_value = POD5_END_REASON_UNBLOCK_MUX_CHANGE;
        break;
    case pod5::ReadEndReason::data_service_unblock_mux_change:
        *end_reason_value = POD5_END_REASON_DATA_SERVICE_UNBLOCK_MUX_CHANGE;
        break;
    case pod5::ReadEndReason::signal_positive:
        *end_reason_value = POD5_END_REASON_SIGNAL_POSITIVE;
        break;
    case pod5::ReadEndReason::signal_negative:
        *end_reason_value = POD5_END_REASON_SIGNAL_NEGATIVE;
        break;
    default:
    case pod5::ReadEndReason::unknown:
        *end_reason_value = POD5_END_REASON_UNKNOWN;
        break;
    }

    std::copy(end_reason_val.second.begin(), end_reason_val.second.end(), end_reason_string_value);
    end_reason_string_value[*end_reason_string_value_size] = '\0';
    return POD5_OK;
}

pod5_error_t pod5_get_pore_type(
    Pod5ReadRecordBatch_t * batch,
    int16_t pore_type,
    char * pore_type_string_value,
    size_t * pore_type_string_value_size)
{
    pod5_reset_error();

    if (!check_output_pointer_not_null(pore_type_string_value)
        || !check_output_pointer_not_null(pore_type_string_value_size))
    {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(auto const pore_type_str, batch->batch.get_pore_type(pore_type));
    auto const input_buffer_len = *pore_type_string_value_size;
    *pore_type_string_value_size = pore_type_str.size() + 1;
    if (pore_type_str.size() >= input_buffer_len) {
        return POD5_ERROR_STRING_NOT_LONG_ENOUGH;
    }

    std::copy(pore_type_str.begin(), pore_type_str.end(), pore_type_string_value);
    pore_type_string_value[*pore_type_string_value_size] = '\0';
    return POD5_OK;
}

class SignalRowInfoCHelper : public SignalRowInfo {
public:
    SignalRowInfoCHelper(pod5::SignalTableRecordBatch const & b) : batch(b) {}

    pod5::SignalTableRecordBatch batch;
};

pod5_error_t pod5_get_signal_row_info(
    Pod5FileReader * reader,
    size_t signal_rows_count,
    uint64_t * signal_rows,
    SignalRowInfo ** signal_row_info)
{
    pod5_reset_error();

    if (!check_not_null(reader) || !check_output_pointer_not_null(signal_row_info)) {
        return g_pod5_error_no;
    }

    // Sort all rows first, in order to make searching faster.
    std::vector<std::uint64_t> signal_rows_sorted{signal_rows, signal_rows + signal_rows_count};
    std::sort(signal_rows_sorted.begin(), signal_rows_sorted.end());

    // Then loop all rows, forward.
    for (std::size_t completed_rows = 0; completed_rows < signal_rows_sorted.size();)
    {  // No increment here, we do it below when we succeed.
        auto const start_row = signal_rows_sorted[completed_rows];

        std::size_t batch_row = 0;
        POD5_C_ASSIGN_OR_RAISE(
            std::size_t row_batch,
            (reader->reader->signal_batch_for_row_id(start_row, &batch_row)));
        POD5_C_ASSIGN_OR_RAISE(auto batch, reader->reader->read_signal_record_batch(row_batch));

        auto output = std::make_unique<SignalRowInfoCHelper>(batch);

        output->batch_index = start_row;
        output->batch_row_index = batch_row;

        auto samples = batch.samples_column();
        output->stored_sample_count = samples->Value(batch_row);
        POD5_C_ASSIGN_OR_RAISE(output->stored_byte_count, batch.samples_byte_count(batch_row));

        signal_row_info[completed_rows] = output.release();
        completed_rows += 1;
    }

    return POD5_OK;
}

pod5_error_t pod5_free_signal_row_info(size_t signal_rows_count, SignalRowInfo_t ** signal_row_info)
{
    for (std::size_t i = 0; i < signal_rows_count; ++i) {
        std::unique_ptr<SignalRowInfoCHelper> helper(
            static_cast<SignalRowInfoCHelper *>(signal_row_info[i]));
        helper.reset();
    }
    return POD5_OK;
}

pod5_error_t pod5_get_signal(
    Pod5FileReader * reader,
    SignalRowInfo_t * row_info,
    std::size_t sample_count,
    std::int16_t * sample_data)
{
    pod5_reset_error();

    if (!check_not_null(reader) || !check_not_null(row_info)
        || !check_output_pointer_not_null(sample_data))
    {
        return g_pod5_error_no;
    }

    SignalRowInfoCHelper * row_info_data = static_cast<SignalRowInfoCHelper *>(row_info);

    POD5_C_RETURN_NOT_OK(row_info_data->batch.extract_signal_row(
        row_info->batch_row_index, gsl::make_span(sample_data, sample_count)));

    return POD5_OK;
}

pod5_error_t pod5_get_read_complete_sample_count(
    Pod5FileReader_t * reader,
    Pod5ReadRecordBatch_t * batch,
    size_t batch_row,
    size_t * sample_count)
{
    pod5_reset_error();

    if (!check_not_null(reader) || !check_output_pointer_not_null(sample_count)) {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(auto const & signal_rows, batch->batch.get_signal_rows(batch_row));

    POD5_C_ASSIGN_OR_RAISE(
        *sample_count,
        reader->reader->extract_sample_count(
            gsl::make_span(signal_rows->raw_values(), signal_rows->length())));
    return POD5_OK;
}

pod5_error_t pod5_get_read_complete_signal(
    Pod5FileReader_t * reader,
    Pod5ReadRecordBatch_t * batch,
    size_t batch_row,
    size_t sample_count,
    int16_t * signal)
{
    pod5_reset_error();

    if (!check_not_null(reader) || !check_output_pointer_not_null(signal)) {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(auto const & signal_rows, batch->batch.get_signal_rows(batch_row));

    POD5_C_RETURN_NOT_OK(reader->reader->extract_samples(
        gsl::make_span(signal_rows->raw_values(), signal_rows->length()),
        gsl::make_span(signal, sample_count)));
    return POD5_OK;
}

//---------------------------------------------------------------------------------------------------------------------
Pod5FileWriter *
pod5_create_file(char const * filename, char const * writer_name, Pod5WriterOptions const * options)
{
    pod5_reset_error();

    if (!check_string_not_empty(filename) || !check_string_not_empty(writer_name)) {
        return nullptr;
    }

    auto internal_writer =
        pod5::create_file_writer(filename, writer_name, make_internal_writer_options(options));
    if (!internal_writer.ok()) {
        pod5_set_error(internal_writer.status());
        return nullptr;
    }

    auto writer = std::make_unique<Pod5FileWriter>(std::move(*internal_writer));
    return writer.release();
}

pod5_error_t pod5_close_and_free_writer(Pod5FileWriter * file)
{
    pod5_reset_error();

    std::unique_ptr<Pod5FileWriter> ptr{file};
    POD5_C_RETURN_NOT_OK(ptr->writer->close());

    ptr.reset();
    return POD5_OK;
}

pod5_error_t pod5_add_pore(int16_t * pore_index, Pod5FileWriter * file, char const * pore_type)
{
    pod5_reset_error();

    if (!check_string_not_empty(pore_type) || !check_file_not_null(file)
        || !check_output_pointer_not_null(pore_index))
    {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(*pore_index, file->writer->add_pore_type(pore_type));
    return POD5_OK;
}

pod5_error_t pod5_add_run_info(
    int16_t * run_info_index,
    Pod5FileWriter * file,
    char const * acquisition_id,
    std::int64_t acquisition_start_time_ms,
    std::int16_t adc_max,
    std::int16_t adc_min,
    std::size_t context_tags_count,
    char const ** context_tags_keys,
    char const ** context_tags_values,
    char const * experiment_name,
    char const * flow_cell_id,
    char const * flow_cell_product_code,
    char const * protocol_name,
    char const * protocol_run_id,
    std::int64_t protocol_start_time_ms,
    char const * sample_id,
    std::uint16_t sample_rate,
    char const * sequencing_kit,
    char const * sequencer_position,
    char const * sequencer_position_type,
    char const * software,
    char const * system_name,
    char const * system_type,
    std::size_t tracking_id_count,
    char const ** tracking_id_keys,
    char const ** tracking_id_values)
{
    pod5_reset_error();

    if (!check_file_not_null(file)) {
        return g_pod5_error_no;
    }

    auto const parse_map =
        [](std::size_t tracking_id_count,
           char const ** tracking_id_keys,
           char const ** tracking_id_values) -> pod5::Result<pod5::RunInfoData::MapType> {
        pod5::RunInfoData::MapType result;
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

    POD5_C_ASSIGN_OR_RAISE(
        auto const context_tags,
        parse_map(context_tags_count, context_tags_keys, context_tags_values));
    POD5_C_ASSIGN_OR_RAISE(
        auto const tracking_id, parse_map(tracking_id_count, tracking_id_keys, tracking_id_values));

    POD5_C_ASSIGN_OR_RAISE(
        *run_info_index,
        file->writer->add_run_info(pod5::RunInfoData(
            acquisition_id,
            acquisition_start_time_ms,
            adc_max,
            adc_min,
            context_tags,
            experiment_name,
            flow_cell_id,
            flow_cell_product_code,
            protocol_name,
            protocol_run_id,
            protocol_start_time_ms,
            sample_id,
            sample_rate,
            sequencing_kit,
            sequencer_position,
            sequencer_position_type,
            software,
            system_name,
            system_type,
            tracking_id)));

    return POD5_OK;
}

inline bool check_read_data_struct(std::uint16_t struct_version, void const * row_data)
{
    static_assert(
        READ_BATCH_ROW_INFO_VERSION == READ_BATCH_ROW_INFO_VERSION_3,
        "New versions must be explicitly loaded");

    if (!check_not_null(row_data)) {
        return false;
    }

    if (struct_version < READ_BATCH_ROW_INFO_VERSION_3) {
        pod5_set_error(arrow::Status::Invalid("Unable to write V1 + V2 reads, update to V3 API."));
        return false;
    }

    if (struct_version == READ_BATCH_ROW_INFO_VERSION_3) {
        auto const * typed_row_data = static_cast<ReadBatchRowInfoArrayV3 const *>(row_data);

        if (!check_not_null(typed_row_data->read_id) || !check_not_null(typed_row_data->read_number)
            || !check_not_null(typed_row_data->start_sample)
            || !check_not_null(typed_row_data->median_before)
            || !check_not_null(typed_row_data->channel) || !check_not_null(typed_row_data->well)
            || !check_not_null(typed_row_data->calibration_scale)
            || !check_not_null(typed_row_data->calibration_offset)
            || !check_not_null(typed_row_data->end_reason)
            || !check_not_null(typed_row_data->end_reason_forced)
            || !check_not_null(typed_row_data->run_info_id)
            || !check_not_null(typed_row_data->num_minknow_events)
            || !check_not_null(typed_row_data->tracked_scaling_scale)
            || !check_not_null(typed_row_data->tracked_scaling_shift)
            || !check_not_null(typed_row_data->predicted_scaling_scale)
            || !check_not_null(typed_row_data->predicted_scaling_shift)
            || !check_not_null(typed_row_data->num_reads_since_mux_change)
            || !check_not_null(typed_row_data->time_since_mux_change))
        {
            return false;
        }
    }

    return true;
}

inline bool load_struct_row_into_read_data(
    std::unique_ptr<pod5::FileWriter> const & writer,
    pod5::ReadData & read_data,
    std::uint16_t struct_version,
    void const * row_data,
    std::uint32_t row_id)
{
    static_assert(
        READ_BATCH_ROW_INFO_VERSION == READ_BATCH_ROW_INFO_VERSION_3,
        "New versions must be explicitly loaded");

    // Version 0-2 are no longer supported for writing.
    if (struct_version == READ_BATCH_ROW_INFO_VERSION_3) {
        auto const * typed_row_data = static_cast<ReadBatchRowInfoArrayV3 const *>(row_data);

        boost::uuids::uuid read_id_uuid;
        std::copy(
            typed_row_data->read_id[row_id],
            typed_row_data->read_id[row_id] + sizeof(read_id_uuid),
            read_id_uuid.begin());

        pod5::ReadEndReason end_reason_internal = pod5::ReadEndReason::unknown;
        switch (typed_row_data->end_reason[row_id]) {
        case POD5_END_REASON_UNKNOWN:
            end_reason_internal = pod5::ReadEndReason::unknown;
            break;
        case POD5_END_REASON_MUX_CHANGE:
            end_reason_internal = pod5::ReadEndReason::mux_change;
            break;
        case POD5_END_REASON_UNBLOCK_MUX_CHANGE:
            end_reason_internal = pod5::ReadEndReason::unblock_mux_change;
            break;
        case POD5_END_REASON_DATA_SERVICE_UNBLOCK_MUX_CHANGE:
            end_reason_internal = pod5::ReadEndReason::data_service_unblock_mux_change;
            break;
        case POD5_END_REASON_SIGNAL_POSITIVE:
            end_reason_internal = pod5::ReadEndReason::signal_positive;
            break;
        case POD5_END_REASON_SIGNAL_NEGATIVE:
            end_reason_internal = pod5::ReadEndReason::signal_negative;
            break;
        default:
            pod5_set_error(arrow::Status::Invalid(
                "out of range end reason ",
                typed_row_data->end_reason[row_id],
                " passed to add read"));
            return false;
        }

        auto const end_reason_index = writer->lookup_end_reason(end_reason_internal);
        if (!end_reason_index.ok()) {
            pod5_set_error(end_reason_index.status());
            return false;
        }

        read_data = pod5::ReadData{
            read_id_uuid,
            typed_row_data->read_number[row_id],
            typed_row_data->start_sample[row_id],
            typed_row_data->channel[row_id],
            typed_row_data->well[row_id],
            typed_row_data->pore_type[row_id],
            typed_row_data->calibration_offset[row_id],
            typed_row_data->calibration_scale[row_id],
            typed_row_data->median_before[row_id],
            *end_reason_index,
            typed_row_data->end_reason_forced[row_id] != 0,
            typed_row_data->run_info_id[row_id],
            typed_row_data->num_minknow_events[row_id],
            typed_row_data->tracked_scaling_scale[row_id],
            typed_row_data->tracked_scaling_shift[row_id],
            typed_row_data->predicted_scaling_scale[row_id],
            typed_row_data->predicted_scaling_shift[row_id],
            typed_row_data->num_reads_since_mux_change[row_id],
            typed_row_data->time_since_mux_change[row_id],
        };
    } else {
        pod5_set_error(
            arrow::Status::Invalid("Invalid writer struct version '", struct_version, "' passed"));
        return false;
    }
    return true;
};

pod5_error_t pod5_add_reads_data(
    Pod5FileWriter_t * file,
    uint32_t read_count,
    uint16_t struct_version,
    void const * row_data,
    int16_t const ** signal,
    uint32_t const * signal_size)
{
    pod5_reset_error();

    if (!check_file_not_null(file) || !check_read_data_struct(struct_version, row_data)) {
        return g_pod5_error_no;
    }

    for (std::uint32_t read = 0; read < read_count; ++read) {
        pod5::ReadData read_data;
        if (!load_struct_row_into_read_data(
                file->writer, read_data, struct_version, row_data, read)) {
            return g_pod5_error_no;
        }

        POD5_C_RETURN_NOT_OK(file->writer->add_complete_read(
            read_data, gsl::make_span(signal[read], signal_size[read])));
    }

    return POD5_OK;
}

pod5_error_t pod5_add_reads_data_pre_compressed(
    Pod5FileWriter_t * file,
    uint32_t read_count,
    uint16_t struct_version,
    void const * row_data,
    char const *** compressed_signal,
    size_t const ** compressed_signal_size,
    uint32_t const ** sample_counts,
    size_t const * signal_chunk_count)
{
    pod5_reset_error();

    if (!check_file_not_null(file) || !check_read_data_struct(struct_version, row_data)) {
        return g_pod5_error_no;
    }

    for (std::uint32_t read = 0; read < read_count; ++read) {
        pod5::ReadData read_data;
        if (!load_struct_row_into_read_data(
                file->writer, read_data, struct_version, row_data, read)) {
            return g_pod5_error_no;
        }

        std::uint64_t total_sample_count = 0;
        std::vector<std::uint64_t> signal_rows;
        for (std::size_t i = 0; i < signal_chunk_count[read]; ++i) {
            auto signal = compressed_signal[read][i];
            auto signal_size = compressed_signal_size[read][i];
            auto sample_count = sample_counts[read][i];
            total_sample_count += sample_count;
            POD5_C_ASSIGN_OR_RAISE(
                auto row_id,
                file->writer->add_pre_compressed_signal(
                    read_data.read_id,
                    gsl::make_span(signal, signal_size).as_span<std::uint8_t const>(),
                    sample_count));
            signal_rows.push_back(row_id);
        }

        POD5_C_RETURN_NOT_OK(file->writer->add_complete_read(
            read_data, gsl::make_span(signal_rows), total_sample_count));
    }
    return POD5_OK;
}

size_t pod5_vbz_compressed_signal_max_size(size_t sample_count)
{
    pod5_reset_error();
    return pod5::compressed_signal_max_size(sample_count);
}

pod5_error_t pod5_vbz_compress_signal(
    int16_t const * signal,
    size_t signal_size,
    char * compressed_signal_out,
    size_t * compressed_signal_size)
{
    pod5_reset_error();

    if (!check_not_null(signal) || !check_output_pointer_not_null(compressed_signal_out)
        || !check_output_pointer_not_null(compressed_signal_size))
    {
        return g_pod5_error_no;
    }

    POD5_C_ASSIGN_OR_RAISE(
        auto buffer,
        pod5::compress_signal(gsl::make_span(signal, signal_size), arrow::system_memory_pool()));

    if ((std::size_t)buffer->size() > *compressed_signal_size) {
        pod5_set_error(pod5::Status::Invalid(
            "Compressed signal size (",
            buffer->size(),
            ") is greater than provided buffer size (",
            compressed_signal_size,
            ")"));
        return g_pod5_error_no;
    }

    std::copy(buffer->data(), buffer->data() + buffer->size(), compressed_signal_out);
    *compressed_signal_size = buffer->size();

    return POD5_OK;
}

pod5_error_t pod5_vbz_decompress_signal(
    char const * compressed_signal,
    size_t compressed_signal_size,
    size_t sample_count,
    short * signal_out)
{
    pod5_reset_error();

    if (!check_not_null(compressed_signal) || !check_output_pointer_not_null(signal_out)) {
        return g_pod5_error_no;
    }

    auto const in_span =
        gsl::make_span(compressed_signal, compressed_signal_size).as_span<std::uint8_t const>();
    auto out_span = gsl::make_span(signal_out, sample_count);
    POD5_C_RETURN_NOT_OK(pod5::decompress_signal(in_span, arrow::system_memory_pool(), out_span));

    return POD5_OK;
}

pod5_error_t pod5_format_read_id(read_id_t const read_id, char * read_id_string)
{
    pod5_reset_error();

    if (!check_not_null(read_id) || !check_output_pointer_not_null(read_id_string)) {
        return g_pod5_error_no;
    }

    auto uuid_data = reinterpret_cast<boost::uuids::uuid const *>(read_id);
    std::string string_data = boost::uuids::to_string(*uuid_data);
    if (string_data.size() != 36) {
        pod5_set_error(pod5::Status::Invalid("Unexpected length of UUID"));
        return g_pod5_error_no;
    }

    std::copy(string_data.begin(), string_data.end(), read_id_string);
    read_id_string[string_data.size()] = '\0';

    return POD5_OK;
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
