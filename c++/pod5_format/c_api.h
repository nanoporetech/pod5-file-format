#pragma once

#include "pod5_format/pod5_format_export.h"

#include <stddef.h>
#include <stdint.h>

#ifndef _WIN32
#define POD5_DEPRECATED __attribute__((deprecated))
#else
#define POD5_DEPRECATED
#endif

#ifdef __cplusplus
extern "C" {
#endif

struct Pod5FileReader;
typedef struct Pod5FileReader Pod5FileReader_t;
struct Pod5FileWriter;
typedef struct Pod5FileWriter Pod5FileWriter_t;
struct Pod5ReadRecordBatch;
typedef struct Pod5ReadRecordBatch Pod5ReadRecordBatch_t;

//---------------------------------------------------------------------------------------------------------------------
// Error management
//---------------------------------------------------------------------------------------------------------------------

/// \brief Integer error codes.
/// \note Taken from the arrow status enum.
enum pod5_error {
    POD5_OK = 0,
    POD5_ERROR_OUTOFMEMORY = 1,
    POD5_ERROR_KEYERROR = 2,
    POD5_ERROR_TYPEERROR = 3,
    POD5_ERROR_INVALID = 4,
    POD5_ERROR_IOERROR = 5,
    POD5_ERROR_CAPACITYERROR = 6,
    POD5_ERROR_INDEXERROR = 7,
    POD5_ERROR_CANCELLED = 8,
    POD5_ERROR_UNKNOWNERROR = 9,
    POD5_ERROR_NOTIMPLEMENTED = 10,
    POD5_ERROR_SERIALIZATIONERROR = 11,
    POD5_ERROR_STRING_NOT_LONG_ENOUGH = 11,
};
typedef enum pod5_error pod5_error_t;

/// \brief Get the most recent error number from all pod5 api's.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_error_no();
/// \brief Get the most recent error description string from all pod5 api's.
/// \note The string's lifetime is internally managed, a caller should not free it.
POD5_FORMAT_EXPORT char const * pod5_get_error_string();

//---------------------------------------------------------------------------------------------------------------------
// Global state
//---------------------------------------------------------------------------------------------------------------------

/// \brief Initialise and register global pod5 types
POD5_FORMAT_EXPORT pod5_error_t pod5_init();
/// \brief Terminate global pod5 types
POD5_FORMAT_EXPORT pod5_error_t pod5_terminate();

//---------------------------------------------------------------------------------------------------------------------
// Shared Structures
//---------------------------------------------------------------------------------------------------------------------

enum pod5_end_reason {
    POD5_END_REASON_UNKNOWN = 0,
    POD5_END_REASON_MUX_CHANGE = 1,
    POD5_END_REASON_UNBLOCK_MUX_CHANGE = 2,
    POD5_END_REASON_DATA_SERVICE_UNBLOCK_MUX_CHANGE = 3,
    POD5_END_REASON_SIGNAL_POSITIVE = 4,
    POD5_END_REASON_SIGNAL_NEGATIVE = 5
};
typedef enum pod5_end_reason pod5_end_reason_t;

typedef uint16_t run_info_index_t;

typedef uint8_t read_id_t[16];
typedef uint8_t run_id_t[16];

// Single entry of read data:
struct ReadBatchRowInfoV3 {
    // The read id data, in binary form.
    read_id_t read_id;

    // Read number for the read.
    uint32_t read_number;
    // Start sample for the read.
    uint64_t start_sample;
    // Median before level.
    float median_before;

    // Channel for the read.
    uint16_t channel;
    // Channel for the read.
    uint8_t well;
    // Dictionary index for the pore type.
    int16_t pore_type;
    // Calibration offset type for the read.
    float calibration_offset;
    // Palibration type for the read.
    float calibration_scale;
    // End reason index for the read.
    int16_t end_reason;
    // Was the end reason for the read forced (0 for false, 1 for true).
    uint8_t end_reason_forced;
    // Dictionary index for run id for the read, can be used to look up run info.
    int16_t run_info;

    // Number of minknow events that the read contains
    uint64_t num_minknow_events;

    // Scale/Shift for tracked read scaling values (based on previous reads)
    float tracked_scaling_scale;
    float tracked_scaling_shift;

    // Scale/Shift for predicted read scaling values (based on this read's raw signal)
    float predicted_scaling_scale;
    float predicted_scaling_shift;

    // How many reads have been selected prior to this read on the channel-well since it was made active.
    uint32_t num_reads_since_mux_change;
    // How many seconds have passed since the channel-well was made active
    float time_since_mux_change;

    // Number of signal row entries for the read.
    int64_t signal_row_count;

    // The length of the read in samples.
    uint64_t num_samples;
};

// Typedef for latest batch row info structure.
typedef struct ReadBatchRowInfoV3 ReadBatchRowInfo_t;

// Array of read data:
struct ReadBatchRowInfoArrayV3 {
    // The read id data, in binary form.
    read_id_t const * read_id;

    // Read number for the read.
    uint32_t const * read_number;
    // Start sample for the read.
    uint64_t const * start_sample;
    // Median before level.
    float const * median_before;

    // Channel for the read.
    uint16_t const * channel;
    // Well for the read.
    uint8_t const * well;
    // Pore type for the read.
    int16_t const * pore_type;
    // Calibration offset type for the read.
    float const * calibration_offset;
    // Palibration type for the read.
    float const * calibration_scale;
    // End reason type for the read.
    pod5_end_reason_t const * end_reason;
    // Was the end reason for the read forced (0 for false, 1 for true).
    uint8_t const * end_reason_forced;
    // Run info type for the read.
    int16_t const * run_info_id;

    // Number of minknow events that the read contains
    uint64_t const * num_minknow_events;

    // Scale/Shift for tracked read scaling values (based on previous reads)
    float const * tracked_scaling_scale;
    float const * tracked_scaling_shift;

    // Scale/Shift for predicted read scaling values (based on this read's raw signal)
    float const * predicted_scaling_scale;
    float const * predicted_scaling_shift;

    // How many reads have been selected prior to this read on the channel-well since it was made active.
    uint32_t const * num_reads_since_mux_change;
    // How many seconds have passed since the channel-well was made active
    float const * time_since_mux_change;
};

// Typedef for latest batch row info structure.
typedef struct ReadBatchRowInfoArrayV3 ReadBatchRowInfoArray_t;

#define READ_BATCH_ROW_INFO_VERSION_0 0
// Addition of num_minknow_events fields, scaling fields.
#define READ_BATCH_ROW_INFO_VERSION_1 1
// Addition of num_samples fields.
#define READ_BATCH_ROW_INFO_VERSION_2 2
// Flattening of read structures.
#define READ_BATCH_ROW_INFO_VERSION_3 3
// Latest available version.
#define READ_BATCH_ROW_INFO_VERSION READ_BATCH_ROW_INFO_VERSION_3

//---------------------------------------------------------------------------------------------------------------------
// Reading files
//---------------------------------------------------------------------------------------------------------------------

/// \brief Open a file reader
/// \param filename         The filename of the pod5 file.
POD5_FORMAT_EXPORT Pod5FileReader_t * pod5_open_file(char const * filename);

/// \brief Close a file reader, releasing all memory held by the reader.
POD5_FORMAT_EXPORT pod5_error_t pod5_close_and_free_reader(Pod5FileReader_t * file);

struct FileInfo {
    read_id_t file_identifier;

    struct Version {
        uint16_t major;
        uint16_t minor;
        uint16_t revision;
    } version;
};
typedef struct FileInfo FileInfo_t;

/// \brief Find the number of read batches in the file.
/// \param[out] file        The file to be queried.
/// \param      file_info   The info read from the file.
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_file_info(Pod5FileReader_t * reader, FileInfo_t * file_info);

struct EmbeddedFileData {
    // The file name to open - note this may not be the original file name, if the file has been migrated.
    char const * file_name;
    size_t offset;
    size_t length;
};
typedef struct EmbeddedFileData EmbeddedFileData_t;

/// \brief Find the location of the read table data
/// \param[out] file        The file to be queried.
/// \param      file_data   The output read table file data.
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_file_read_table_location(Pod5FileReader_t * reader, EmbeddedFileData_t * file_data);

/// \brief Find the location of the signal table data
/// \param[out] file        The file to be queried.
/// \param      file_data   The output signal table file data.
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_file_signal_table_location(Pod5FileReader_t * reader, EmbeddedFileData_t * file_data);

/// \brief Find the location of the run info table data
/// \param[out] file        The file to be queried.
/// \param      file_data   The output signal table file data.
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_file_run_info_table_location(Pod5FileReader_t * reader, EmbeddedFileData_t * file_data);

/// \brief Find the number of reads in the file.
/// \param      reader  The file reader to read from
/// \param[out] count   The number of reads in the file
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_count(Pod5FileReader_t * reader, size_t * count);

/// \brief Find the number of reads in the file.
/// \param        reader        The file reader to read from.
/// \param        count         The number of read_id's allocated in [read_ids], an error is raised if the count is not greater or equal to pod5_get_read_count.
/// \param[out]   read_ids      The read id's written in a contiguous array.
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_read_ids(Pod5FileReader_t * reader, size_t count, read_id_t * read_ids);

/// \brief Plan the most efficient route through the data for the given read ids
/// \param      file                The file to be queried.
/// \param      read_id_array       The read id array (contiguous array, 16 bytes per id).
/// \param      read_id_count       The number of read ids.
/// \param[out] batch_counts        The number of rows per batch that need to be visited (rows listed in batch_rows),
///                                 input array length should be the number of read table batches.
/// \param[out] batch_rows          Rows to visit per batch, packed into one array. Offsets into this array from
///                                 [batch_counts] provide the per-batch row data. Input array length should
///                                 equal read_id_count.
/// \param[out] find_success_count  The number of requested read ids that were found.
/// \note The output array is sorted in file storage order, to improve read efficiency.
///       [find_success_count] is the number of successful find steps in the result [steps].
///       Failed finds are all sorted to the back of the [steps] array, and are marked with an
///       invalid batch and batch_row value.
POD5_FORMAT_EXPORT pod5_error_t pod5_plan_traversal(
    Pod5FileReader_t * reader,
    uint8_t const * read_id_array,
    size_t read_id_count,
    uint32_t * batch_counts,
    uint32_t * batch_rows,
    size_t * find_success_count);

/// \brief Find the number of read batches in the file.
/// \param[out] count   The number of read batches in the file
/// \param      reader  The file reader to read from
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_read_batch_count(size_t * count, Pod5FileReader_t * reader);

/// \brief Get a read batch from the file.
/// \param[out] batch   The extracted batch.
/// \param      reader  The file reader to read from
/// \param      index   The index of the batch to read.
/// \note Batches returned from this API must be freed using #pod5_free_read_batch
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_read_batch(Pod5ReadRecordBatch_t ** batch, Pod5FileReader_t * reader, size_t index);

/// \brief Release a read batch when it is not longer used.
/// \param batch The batch to release.
POD5_FORMAT_EXPORT pod5_error_t pod5_free_read_batch(Pod5ReadRecordBatch_t * batch);

/// \brief Find the number of rows in a batch.
/// \param batch    The batch to query the number of rows for.
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_read_batch_row_count(size_t * count, Pod5ReadRecordBatch_t *);

/// \brief Find the info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      row                 The row index to query.
/// \param      struct_version      The version of the struct being passed in, calling code
///                                 should use [READ_BATCH_ROW_INFO_VERSION].
/// \param[out] row_data            The data for reading into, should be a pointer to ReadBatchRowInfo_t.
/// \param[out] read_table_version  The table version read from the file, will indicate which fields should be available.
///                                 See READ_BATCH_ROW_INFO_VERSION and ReadBatchRowInfo_t above for corresponding fields.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_batch_row_info_data(
    Pod5ReadRecordBatch_t * batch,
    size_t row,
    uint16_t struct_version,
    void * row_data,
    uint16_t * read_table_version);

/// \brief Find the info for a row in a read batch.
/// \param      batch                       The read batch to query.
/// \param      row                         The row index to query.
/// \param      signal_row_indices_count    Number of entries in the signal_row_indices array.
/// \param[out] signal_row_indices          The signal row indices read out of the read row.
/// \note signal_row_indices_count Must equal signal_row_count returned from pod5_get_read_batch_row_info
///       or an error is generated.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_signal_row_indices(
    Pod5ReadRecordBatch_t * batch,
    size_t row,
    int64_t signal_row_indices_count,
    uint64_t * signal_row_indices);

struct CalibrationExtraData {
    // The digitisation value used by the sequencer, equal to:
    //
    // adc_max - adc_min + 1
    uint16_t digitisation;
    // The range of the calibrated channel in pA.
    float range;
};
typedef struct CalibrationExtraData CalibrationExtraData_t;

/// \brief Find the extra calibration info for a row in a read batch.
/// \param      batch                   The read batch to query.
/// \param      row                     The read row index.
/// \param[out] calibration_extra_data  Output location for the calibration data.
/// \note The values are computed from data held in the file, and written directly to the address provided, there is no need to release any data.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_calibration_extra_info(
    Pod5ReadRecordBatch_t * batch,
    size_t row,
    CalibrationExtraData_t * calibration_extra_data);

struct KeyValueData {
    size_t size;
    char const ** keys;
    char const ** values;
};

struct RunInfoDictData {
    char const * acquisition_id;
    int64_t acquisition_start_time_ms;
    int16_t adc_max;
    int16_t adc_min;
    struct KeyValueData context_tags;
    char const * experiment_name;
    char const * flow_cell_id;
    char const * flow_cell_product_code;
    char const * protocol_name;
    char const * protocol_run_id;
    int64_t protocol_start_time_ms;
    char const * sample_id;
    uint16_t sample_rate;
    char const * sequencing_kit;
    char const * sequencer_position;
    char const * sequencer_position_type;
    char const * software;
    char const * system_name;
    char const * system_type;
    struct KeyValueData tracking_id;
};
typedef struct RunInfoDictData RunInfoDictData_t;

/// \brief Find the run info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      run_info            The run info index to query from the passed batch.
/// \param[out] run_info_data       Output location for the run info data.
/// \note The returned run_info value should be released using pod5_free_run_info when it is no longer used.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_run_info(
    Pod5ReadRecordBatch_t * batch,
    int16_t run_info,
    RunInfoDictData_t ** run_info_data);

/// \brief Find the run info for a row in a file.
/// \param      file                The file to query.
/// \param      run_info_index      The run info index to query from the passed file.
/// \param[out] run_info_data       Output location for the run info data.
/// \note The returned run_info value should be released using pod5_free_run_info when it is no longer used.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_file_run_info(
    Pod5FileReader_t * file,
    run_info_index_t run_info_index,
    RunInfoDictData_t ** run_info_data);

/// \brief Release a RunInfoDictData struct after use.
POD5_FORMAT_EXPORT pod5_error_t pod5_free_run_info(RunInfoDictData_t * run_info_data);

/// \brief Release a RunInfoDictData struct after use.
/// \deprecated
POD5_FORMAT_EXPORT POD5_DEPRECATED pod5_error_t
pod5_release_run_info(RunInfoDictData_t * run_info_data);

/// \brief Find the run info for a row in a read file.
/// \param      file                The file to query.
/// \param[out] run_info_count      The number of run info's that are present in they queried file
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_file_run_info_count(Pod5FileReader_t * file, run_info_index_t * run_info_count);

/// \brief Find the end reason for a row in a read batch.
/// \param        batch                           The read batch to query.
/// \param        end_reason                      The end reason index to query from the passed batch.
/// \param        end_reason_value                The enum value for end reason.
/// \param[out]   end_reason_string_value         Output location for the string value for the end reason.
/// \param[inout] end_reason_string_value_size    Size of [end_reason_string_value], the number of characters written (including 1 for null character) is placed in this value on return.
/// \note If the string input is not long enough POD5_ERROR_STRING_NOT_LONG_ENOUGH is returned.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_end_reason(
    Pod5ReadRecordBatch_t * batch,
    int16_t end_reason,
    pod5_end_reason_t * end_reason_value,
    char * end_reason_string_value,
    size_t * end_reason_string_value_size);

/// \brief Find the pore type for a row in a read batch.
/// \param        batch                           The read batch to query.
/// \param        pore_type                       The pore type index to query from the passed batch.
/// \param[out]   pore_type_string_value          Output location for the string value for the pore type.
/// \param[inout] pore_type_string_value_size     Size of [pore_type_string_value], the number of characters written (including 1 for null character) is placed in this value on return.
/// \note If the string input is not long enough POD5_ERROR_STRING_NOT_LONG_ENOUGH is returned.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_pore_type(
    Pod5ReadRecordBatch_t * batch,
    int16_t pore_type,
    char * pore_type_string_value,
    size_t * pore_type_string_value_size);

struct SignalRowInfo {
    size_t batch_index;
    size_t batch_row_index;
    uint32_t stored_sample_count;
    size_t stored_byte_count;
};
typedef struct SignalRowInfo SignalRowInfo_t;

/// \brief Find the info for a signal row in a reader.
/// \param      reader                      The reader to query.
/// \param      signal_rows_count           The number of signal rows to query.
/// \param      signal_rows                 The signal rows to query.
/// \param[out] signal_row_info             Pointers to the output signal row information (must be an array of size signal_rows_count)
POD5_FORMAT_EXPORT pod5_error_t pod5_get_signal_row_info(
    Pod5FileReader_t * reader,
    size_t signal_rows_count,
    uint64_t * signal_rows,
    SignalRowInfo_t ** signal_row_info);

/// \brief Release a list of signal row infos allocated by [pod5_get_signal_row_info].
/// \param      signal_rows_count           The number of signal rows to release.
/// \param      signal_row_info             The signal row infos to release.
/// \note Calls to pod5_free_signal_row_info must be 1:1 with [pod5_get_signal_row_info], you cannot free part of the returned data.
POD5_FORMAT_EXPORT pod5_error_t
pod5_free_signal_row_info(size_t signal_rows_count, SignalRowInfo_t ** signal_row_info);

/// \brief Find the info for a signal row in a reader.
/// \param      reader          The reader to query.
/// \param      row_info        The signal row info batch index to query data for.
/// \param      sample_count    The number of samples allocated in [sample_data] (must equal the length of signal data in the row).
/// \param[out] sample_data     The output location for the queried samples.
/// \note The signal data is allocated by the caller and should be released as appropriate by the caller.
/// \todo MAJOR_VERSION Rename to include "chunk" or "row" or similar to indicate this gets only part of read signal.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_signal(
    Pod5FileReader_t * reader,
    SignalRowInfo_t * row_info,
    size_t sample_count,
    int16_t * sample_data);

/// \brief Find the sample count for a full read.
/// \param      reader          The reader to query.
/// \param      batch           The read batch to query.
/// \param      batch_row       The read row to query data for.
/// \param[out] sample_count    The number of samples in the read - including all chunks of raw data.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_complete_sample_count(
    Pod5FileReader_t * reader,
    Pod5ReadRecordBatch_t * batch,
    size_t batch_row,
    size_t * sample_count);

/// \brief Find the signal for a full read.
/// \param      reader          The reader to query.
/// \param      batch           The read batch to query.
/// \param      batch_row       The read row to query data for.
/// \param      sample_count    The number of samples allocated in [sample_data] (must equal the length of signal data in the queryied read row).
/// \param[out] sample_data     The output location for the queried samples.
/// \note The signal data is allocated by pod5 and should be released as appropriate by the caller.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_complete_signal(
    Pod5FileReader_t * reader,
    Pod5ReadRecordBatch_t * batch,
    size_t batch_row,
    size_t sample_count,
    int16_t * signal);

//---------------------------------------------------------------------------------------------------------------------
// Writing files
//---------------------------------------------------------------------------------------------------------------------

// Signal compression options.
enum CompressionOption {
    /// \brief Use the default signal compression option.
    DEFAULT_SIGNAL_COMPRESSION = 0,
    /// \brief Use vbz to compress read signals in tables.
    VBZ_SIGNAL_COMPRESSION = 1,
    /// \brief Write signals uncompressed to tables.
    UNCOMPRESSED_SIGNAL = 2,
};

// Options to control how a file is written.
struct Pod5WriterOptions {
    /// \brief Maximum number of samples to place in one signal record in the signals table.
    /// \note Use zero to use default value.
    uint32_t max_signal_chunk_size;
    /// \brief Signal type to write to the signals table.
    /// \note Use 'DEFAULT_SIGNAL_COMPRESSION' to use default value.
    int8_t signal_compression_type;

    /// \brief The size of each batch written for the signal table (zero for default).
    size_t signal_table_batch_size;
    /// \brief The size of each batch written for the reads table (zero for default).
    size_t read_table_batch_size;
};
typedef struct Pod5WriterOptions Pod5WriterOptions_t;

/// \brief Create a new pod5 file using specified filenames and options.
/// \param filename         The filename of the pod5 file.
/// \param writer_name      A descriptive string for the user software writing this file.
/// \param options          Options controlling how the file will be written.
POD5_FORMAT_EXPORT Pod5FileWriter_t * pod5_create_file(
    char const * filename,
    char const * writer_name,
    Pod5WriterOptions_t const * options);

/// \brief Close a file writer, releasing all memory held by the writer.
POD5_FORMAT_EXPORT pod5_error_t pod5_close_and_free_writer(Pod5FileWriter_t * file);

/// \brief Add a new pore type to the file.
/// \param[out] pore_index  The index of the added pore.
/// \param      file        The file to add the new pore type to.
/// \param      pore_type   The pore type string for the pore.
POD5_FORMAT_EXPORT pod5_error_t
pod5_add_pore(int16_t * pore_index, Pod5FileWriter_t * file, char const * pore_type);

/// \brief Add a new run info to the file, containing tracking information about a sequencing run.
/// \param[out] run_info_index              The index of the added run_info.
/// \param      file                        The file to add the new pore type to.
/// \param      acquisition_id              The offset parameter for the calibration.
/// \param      acquisition_start_time_ms   Milliseconds after unix epoch when the acquisition was started.
/// \param      adc_max                     Maximum ADC value supported by this hardware.
/// \param      adc_min                     Minimum ADC value supported by this hardware.
/// \param      context_tags_count          Number of entries in the context tags map.
/// \param      context_tags_keys           Array of strings used as keys into the context tags map (must have context_tags_count entries).
/// \param      context_tags_values         Array of strings used as values in the context tags map (must have context_tags_count entries).
/// \param      experiment_name             Name given by the user to the group including this protocol.
/// \param      flow_cell_id                Id for the flow cell used in the run.
/// \param      flow_cell_product_code      Product code for the flow cell used in the run.
/// \param      protocol_name               Name given by the user to the protocol.
/// \param      protocol_run_id             Run id for the protocol.
/// \param      protocol_start_time_ms      Milliseconds after unix epoch when the protocol was started.
/// \param      sample_id                   Name given by the user for sample id.
/// \param      sample_rate                 Sample rate of the run.
/// \param      sequencing_kit              Sequencing kit used in the run.
/// \param      sequencer_position          Sequencer position used in the run.
/// \param      sequencer_position_type     Sequencer position type used in the run.
/// \param      software                    Name of the software used to produce the run.
/// \param      system_name                 Name of the system used to produce the run.
/// \param      system_type                 Type of the system used to produce the run.
/// \param      tracking_id_count           Number of entries in the tracking id map.
/// \param      tracking_id_keys            Array of strings used as keys into the tracking id map (must have tracking_id_count entries).
/// \param      tracking_id_values          Array of strings used as values in the tracking id map (must have tracking_id_count entries).
POD5_FORMAT_EXPORT pod5_error_t pod5_add_run_info(
    int16_t * run_info_index,
    Pod5FileWriter_t * file,
    char const * acquisition_id,
    int64_t acquisition_start_time_ms,
    int16_t adc_max,
    int16_t adc_min,
    size_t context_tags_count,
    char const ** context_tags_keys,
    char const ** context_tags_values,
    char const * experiment_name,
    char const * flow_cell_id,
    char const * flow_cell_product_code,
    char const * protocol_name,
    char const * protocol_run_id,
    int64_t protocol_start_time_ms,
    char const * sample_id,
    uint16_t sample_rate,
    char const * sequencing_kit,
    char const * sequencer_position,
    char const * sequencer_position_type,
    char const * software,
    char const * system_name,
    char const * system_type,
    size_t tracking_id_count,
    char const ** tracking_id_keys,
    char const ** tracking_id_values);

/// \brief Add a read to the file.
///
/// For each read `r`, where `0 <= r < read_count`:
/// - `((RowInfo_t const*)row_data)[r]` describes the read metadata, where `RowInfo_t` is determined by [struct_version]
/// - `signal[r]` is the raw signal data for the read
/// - `signal_size[r]` is the length of `signal[r]` (in samples, not in bytes)
///
/// \param      file            The file to add the reads to.
/// \param      read_count      The number of reads to add with this call.
/// \param      struct_version  The version of the struct of [row_data] being filled, use READ_BATCH_ROW_INFO_VERSION.
/// \param      row_data        The array data for injecting into the file, should be ReadBatchRowInfoArray_t.
///                             This must be an array of length [read_count].
/// \param      signal          The signal data for the reads.
/// \param      signal_size     The number of samples in the signal data.
///                             This must be an array of length [read_count].
POD5_FORMAT_EXPORT pod5_error_t pod5_add_reads_data(
    Pod5FileWriter_t * file,
    uint32_t read_count,
    uint16_t struct_version,
    void const * row_data,
    int16_t const ** signal,
    uint32_t const * signal_size);

/// \brief Add a read to the file, with pre compressed signal chunk sections.
///
/// Consider using the simpler [pod5_add_reads_data] unless you have performance requirements that demand
/// more control over compression and chunking.
///
/// Data should be compressed using [pod5_vbz_compress_signal].
///
/// For each read `r`, where `0 <= r < read_count`:
/// - `((RowInfo_t const*)row_data)[r]` describes the read metadata, where `RowInfo_t` is determined by [struct_version]
/// - `signal_chunk_count[r]` is the number of signal chunks
/// - for each signal chunk `i` where `0 <= i < signal_chunk_count[r]`:
///   - `sample_counts[r][i]` is the number of samples in the chunk (ie: the size of the uncompressed data in
///     samples, not in bytes)
///   - `compressed_signal[r][i]` is the compressed data
///   - `compressed_signal_size[r][i]` is the length of the compressed data at `compressed_signal[r][i]`
///
/// \param      file                    The file to add the read to.
/// \param      read_count              The number of reads to add with this call.
/// \param      struct_version          The version of the struct of [row_data] being filled, use READ_BATCH_ROW_INFO_VERSION.
/// \param      row_data                The array data for injecting into the file, should be ReadBatchRowInfoArray_t.
///                                     This must be an array of length [read_count].
/// \param      compressed_signal       The signal chunks data for the read.
/// \param      compressed_signal_size  The sizes (in bytes) of each signal chunk.
/// \param      sample_counts           The number of samples of each signal chunk. In other words, it is the *uncompressed* size of the
///                                     corresponding [compressed_signal] array, in samples (not bytes!).
/// \param      signal_chunk_count      The number of sections of compressed signal.
///                                     This must be an array of length [read_count].
POD5_FORMAT_EXPORT pod5_error_t pod5_add_reads_data_pre_compressed(
    Pod5FileWriter_t * file,
    uint32_t read_count,
    uint16_t struct_version,
    void const * row_data,
    char const *** compressed_signal,
    size_t const ** compressed_signal_size,
    uint32_t const ** sample_counts,
    size_t const * signal_chunk_count);

/// \brief Find the max size of a compressed array of samples.
/// \param sample_count The number of samples in the source signal.
/// \return The max number of bytes required for the compressed signal.
POD5_FORMAT_EXPORT size_t pod5_vbz_compressed_signal_max_size(size_t sample_count);

/// \brief VBZ compress an array of samples.
/// \param          signal                      The signal to compress.
/// \param          signal_size                 The number of samples to compress.
/// \param[out]     compressed_signal_out       The compressed signal.
/// \param[inout]   compressed_signal_size      The number of compressed bytes, should be set to the size of compressed_signal_out on call.
POD5_FORMAT_EXPORT pod5_error_t pod5_vbz_compress_signal(
    int16_t const * signal,
    size_t signal_size,
    char * compressed_signal_out,
    size_t * compressed_signal_size);

/// \brief VBZ decompress an array of samples.
/// \param          compressed_signal           The signal to compress.
/// \param          compressed_signal_size      The number of compressed bytes, should be set to the size of compressed_signal_out on call.
/// \param          sample_count                The number of samples to decompress.
/// \param[out]     signal_out                  The compressed signal.
POD5_FORMAT_EXPORT pod5_error_t pod5_vbz_decompress_signal(
    char const * compressed_signal,
    size_t compressed_signal_size,
    size_t sample_count,
    short * signal_out);

//---------------------------------------------------------------------------------------------------------------------
// Global state
//---------------------------------------------------------------------------------------------------------------------

/// \brief Format a packed binary read id as a readable read id string:
/// \param          read_id           A 16 byte binary formatted UUID.
/// \param[out]     read_id_string    Output string containing the string formatted UUID (expects a string of at least 37 bytes, one null byte is written.)
POD5_FORMAT_EXPORT pod5_error_t pod5_format_read_id(read_id_t const read_id, char * read_id_string);

#ifdef __cplusplus
}
#endif

//std::shared_ptr<arrow::Schema> pyarrow_test();
