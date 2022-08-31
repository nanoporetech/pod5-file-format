#pragma once

#include "pod5_format/pod5_format_export.h"

#include <stddef.h>
#include <stdint.h>

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
};
typedef enum pod5_error pod5_error_t;

/// \brief Get the most recent error number from all pod5 api's.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_error_no();
/// \brief Get the most recent error description string from all pod5 api's.
/// \note The string's lifetime is internally managed, a caller should not free it.
POD5_FORMAT_EXPORT char const* pod5_get_error_string();

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

typedef uint8_t read_id_t[16];

// Single entry of read data:
struct ReadBatchRowInfoV1 {
    // The read id data, in binary form.
    read_id_t read_id;

    // Read number for the read.
    uint32_t read_number;
    // Start sample for the read.
    uint64_t start_sample;
    // Median before level.
    float median_before;

    // Pore type for the read.
    int16_t pore;
    // Palibration type for the read.
    int16_t calibration;
    // End reason type for the read.
    int16_t end_reason;
    // Run info type for the read.
    int16_t run_info;

    // Number of minknow events that the read contains
    uint64_t num_minknow_events;

    // Scale/Shift for tracked read scaling values (based on previous reads)
    float tracked_scaling_scale;
    float tracked_scaling_shift;

    // Scale/Shift for predicted read scaling values (based on this read's raw signal)
    float predicted_scaling_scale;
    float predicted_scaling_shift;

    // Should the predicted scale/shift be trusted - 1 for trusted, 0 for untrusted
    unsigned char trust_tracked_scale;
    unsigned char trust_tracked_shift;

    // Number of signal row entries for the read.
    int64_t signal_row_count;
};

// Typedef for latest batch row info structure.
typedef struct ReadBatchRowInfoV1 ReadBatchRowInfo_t;

// Array of read data:
struct ReadBatchRowInfoArrayV1 {
    // The read id data, in binary form.
    read_id_t const* read_id;

    // Read number for the read.
    uint32_t const* read_number;
    // Start sample for the read.
    uint64_t const* start_sample;
    // Median before level.
    float const* median_before;

    // Pore type for the read.
    int16_t const* pore;
    // Palibration type for the read.
    int16_t const* calibration;
    // End reason type for the read.
    int16_t const* end_reason;
    // Run info type for the read.
    int16_t const* run_info;

    // Number of minknow events that the read contains
    uint64_t const* num_minknow_events;

    // Scale/Shift for tracked read scaling values (based on previous reads)
    float const* tracked_scaling_scale;
    float const* tracked_scaling_shift;

    // Scale/Shift for predicted read scaling values (based on this read's raw signal)
    float const* predicted_scaling_scale;
    float const* predicted_scaling_shift;

    // Should the predicted scale/shift be trusted - 1 for trusted, 0 for untrusted
    unsigned char const* trust_tracked_scale;
    unsigned char const* trust_tracked_shift;
};

// Typedef for latest batch row info structure.
typedef struct ReadBatchRowInfoArrayV1 ReadBatchRowInfoArray_t;

#define READ_BATCH_ROW_INFO_VERSION_0 0
// Addition of num_minknow_events fields, scaling fields.
#define READ_BATCH_ROW_INFO_VERSION_1 1
// Latest available version.
#define READ_BATCH_ROW_INFO_VERSION READ_BATCH_ROW_INFO_VERSION_1

//---------------------------------------------------------------------------------------------------------------------
// Reading files
//---------------------------------------------------------------------------------------------------------------------

/// \brief Open a split file reader
/// \param signal_filename  The filename of the signal file.
/// \param reads_filename   The filename of the reads file.
POD5_FORMAT_EXPORT Pod5FileReader_t* pod5_open_split_file(char const* signal_filename,
                                                          char const* reads_filename);
/// \brief Open a combined file reader
/// \param filename         The filename of the combined pod5 file.
POD5_FORMAT_EXPORT Pod5FileReader_t* pod5_open_combined_file(char const* filename);

/// \brief Close a file reader, releasing all memory held by the reader.
POD5_FORMAT_EXPORT pod5_error_t pod5_close_and_free_reader(Pod5FileReader_t* file);

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
/// \param[out] file        The combined file to be queried.
/// \param      file_info   The info read from the file.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_file_info(Pod5FileReader_t* reader, FileInfo_t* file_info);

struct EmbeddedFileData {
    size_t offset;
    size_t length;
};
typedef struct EmbeddedFileData EmbeddedFileData_t;

/// \brief Find the number of read batches in the file.
/// \param[out] file        The combined file to be queried.
/// \param      file_data   The output read table file data.
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_combined_file_read_table_location(Pod5FileReader_t* reader, EmbeddedFileData_t* file_data);

/// \brief Find the number of read batches in the file.
/// \param[out] file        The combined file to be queried.
/// \param      file_data   The output signal table file data.
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_combined_file_signal_table_location(Pod5FileReader_t* reader,
                                             EmbeddedFileData_t* file_data);

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
POD5_FORMAT_EXPORT pod5_error_t pod5_plan_traversal(Pod5FileReader_t* reader,
                                                    uint8_t const* read_id_array,
                                                    size_t read_id_count,
                                                    uint32_t* batch_counts,
                                                    uint32_t* batch_rows,
                                                    size_t* find_success_count);

/// \brief Find the number of read batches in the file.
/// \param[out] count   The number of read batches in the file
/// \param      reader  The file reader to read from
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_batch_count(size_t* count, Pod5FileReader_t* reader);

/// \brief Get a read batch from the file.
/// \param[out] batch   The extracted batch.
/// \param      reader  The file reader to read from
/// \param      index   The index of the batch to read.
/// \note Batches returned from this API must be freed using #pod5_free_read_batch
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_batch(Pod5ReadRecordBatch_t** batch,
                                                    Pod5FileReader_t* reader,
                                                    size_t index);

/// \brief Release a read batch when it is not longer used.
/// \param batch The batch to release.
POD5_FORMAT_EXPORT pod5_error_t pod5_free_read_batch(Pod5ReadRecordBatch_t* batch);

/// \brief Find the number of rows in a batch.
/// \param batch    The batch to query the number of rows for.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_batch_row_count(size_t* count,
                                                              Pod5ReadRecordBatch_t*);

/// \brief Find the info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      row                 The row index to query.
/// \param[out] read_id             The read id data (must be 16 bytes).
/// \param[out] pore                Output location for the pore type for the read.
/// \param[out] calibration         Output location for the calibration type for the read.
/// \param[out] read_number         Output location for the read number.
/// \param[out] start_sample        Output location for the start sample.
/// \param[out] median_before       Output location for the median before level.
/// \param[out] end_reason          Output location for the end reason type for the read.
/// \param[out] run_info            Output location for the run info type for the read.
/// \param[out] signal_row_count    Output location for the number of signal row entries for the read.
/// \deprecated Use pod5_get_read_batch_row_info_data instead.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_batch_row_info(Pod5ReadRecordBatch_t* batch,
                                                             size_t row,
                                                             uint8_t* read_id,
                                                             int16_t* pore,
                                                             int16_t* calibration,
                                                             uint32_t* read_number,
                                                             uint64_t* start_sample,
                                                             float* median_before,
                                                             int16_t* end_reason,
                                                             int16_t* run_info,
                                                             int64_t* signal_row_count);

/// \brief Find the info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      row                 The row index to query.
/// \param      struct_version      The version of the struct being passed in, calling code
///                                 should use [READ_BATCH_ROW_INFO_VERSION].
/// \param[out] row_data            The data for reading into, should be a pointer to ReadBatchRowInfo_t.
/// \param[out] read_table_version  The table version read from the file, will indicate which fields should be available.
///                                 See READ_BATCH_ROW_INFO_VERSION and ReadBatchRowInfo_t above for corresponding fields.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_batch_row_info_data(Pod5ReadRecordBatch_t* batch,
                                                                  size_t row,
                                                                  uint16_t struct_version,
                                                                  void* row_data,
                                                                  uint16_t* read_table_version);

/// \brief Find the info for a row in a read batch.
/// \param      batch                       The read batch to query.
/// \param      row                         The row index to query.
/// \param      signal_row_indices_count    Number of entries in the signal_row_indices array.
/// \param[out] signal_row_indices          The signal row indices read out of the read row.
/// \note signal_row_indices_count Must equal signal_row_count returned from pod5_get_read_batch_row_info
///       or an error is generated.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_signal_row_indices(Pod5ReadRecordBatch_t* batch,
                                                            size_t row,
                                                            int64_t signal_row_indices_count,
                                                            uint64_t* signal_row_indices);

struct PoreDictData {
    uint16_t channel;
    uint8_t well;
    char const* pore_type;
};
typedef struct PoreDictData PoreDictData_t;

/// \brief Find the pore info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      pore                The pore index to query.
/// \param[out] pore_data           Output location for the pore data.
/// \note The returned pore value should be released using pod5_release_pore when it is no longer used.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_pore(Pod5ReadRecordBatch_t* batch,
                                              int16_t pore,
                                              PoreDictData_t** pore_data);

/// \brief Release a PoreDictData struct after use.
POD5_FORMAT_EXPORT pod5_error_t pod5_release_pore(PoreDictData_t* pore_data);

struct CalibrationDictData {
    float offset;
    float scale;
};
typedef struct CalibrationDictData CalibrationDictData_t;

/// \brief Find the calibration info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      calibration         The calibration index to query.
/// \param[out] calibration_data    Output location for the calibration data.
/// \note The returned calibration value should be released using pod5_release_calibration when it is no longer used.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_calibration(Pod5ReadRecordBatch_t* batch,
                                                     int16_t calibration,
                                                     CalibrationDictData_t** calibration_data);

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
/// \param      calibration             The calibration index to query.
/// \param      run_info                The run info index to query.
/// \param[out] calibration_extra_data  Output location for the calibration data.
/// \note The values are computed from data held in the file, and written directly to the address provided, there is no need to release any data.
POD5_FORMAT_EXPORT pod5_error_t
pod5_get_calibration_extra_info(Pod5ReadRecordBatch_t* batch,
                                int16_t calibration,
                                int16_t run_info,
                                CalibrationExtraData_t* calibration_extra_data);

/// \brief Release a CalibrationDictData struct after use.
POD5_FORMAT_EXPORT pod5_error_t pod5_release_calibration(CalibrationDictData_t* calibration_data);

struct EndReasonDictData {
    char const* name;
    int forced;
};
typedef struct EndReasonDictData EndReasonDictData_t;

/// \brief Find the calibration info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      end_reason          The end reason index to query.
/// \param[out] end_reason_data     Output location for the end reason data.
/// \note The returned end_reason value should be released using pod5_release_calibration when it is no longer used.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_end_reason(Pod5ReadRecordBatch_t* batch,
                                                    int16_t end_reason,
                                                    EndReasonDictData_t** end_reason_data);

/// \brief Release a CalibrationDictData struct after use.
POD5_FORMAT_EXPORT pod5_error_t pod5_release_end_reason(EndReasonDictData_t* end_reason_data);

struct KeyValueData {
    size_t size;
    char const** keys;
    char const** values;
};
struct RunInfoDictData {
    char const* acquisition_id;
    int64_t acquisition_start_time_ms;
    int16_t adc_max;
    int16_t adc_min;
    struct KeyValueData context_tags;
    char const* experiment_name;
    char const* flow_cell_id;
    char const* flow_cell_product_code;
    char const* protocol_name;
    char const* protocol_run_id;
    int64_t protocol_start_time_ms;
    char const* sample_id;
    uint16_t sample_rate;
    char const* sequencing_kit;
    char const* sequencer_position;
    char const* sequencer_position_type;
    char const* software;
    char const* system_name;
    char const* system_type;
    struct KeyValueData tracking_id;
};
typedef struct RunInfoDictData RunInfoDictData_t;

/// \brief Find the calibration info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      run_info            The run info index to query.
/// \param[out] run_info_data       Output location for the run info data.
/// \note The returned end_reason value should be released using pod5_release_calibration when it is no longer used.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_run_info(Pod5ReadRecordBatch_t* batch,
                                                  int16_t run_info,
                                                  RunInfoDictData_t** run_info_data);

/// \brief Release a CalibrationDictData struct after use.
POD5_FORMAT_EXPORT pod5_error_t pod5_release_run_info(RunInfoDictData_t* run_info_data);

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
POD5_FORMAT_EXPORT pod5_error_t pod5_get_signal_row_info(Pod5FileReader_t* reader,
                                                         size_t signal_rows_count,
                                                         uint64_t* signal_rows,
                                                         SignalRowInfo_t** signal_row_info);

/// \brief Release a list of signal row infos allocated by [pod5_get_signal_row_info].
/// \param      signal_rows_count           The number of signal rows to release.
/// \param      signal_row_info             The signal row infos to release.
/// \note Calls to pod5_free_signal_row_info must be 1:1 with [pod5_get_signal_row_info], you cannot free part of the returned data.

POD5_FORMAT_EXPORT pod5_error_t pod5_free_signal_row_info(size_t signal_rows_count,
                                                          SignalRowInfo_t** signal_row_info);

/// \brief Find the info for a signal row in a reader.
/// \param      reader          The reader to query.
/// \param      row_info        The signal row info batch index to query data for.
/// \param      sample_count    The number of samples allocated in [sample_data] (must equal the length of signal data in the row).
/// \param[out] sample_data     The output location for the queried samples.
/// \note The signal data is allocated by the caller and should be released as appropriate by the caller.
/// \todo MAJOR_VERSION Rename to include "chunk" or "row" or similar to indicate this gets only part of read signal.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_signal(Pod5FileReader_t* reader,
                                                SignalRowInfo_t* row_info,
                                                size_t sample_count,
                                                int16_t* sample_data);

/// \brief Find the sample count for a full read.
/// \param      reader          The reader to query.
/// \param      batch           The read batch to query.
/// \param      batch_row       The read row to query data for.
/// \param[out] sample_count    The number of samples in the read - including all chunks of raw data.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_complete_sample_count(Pod5FileReader_t* reader,
                                                                    Pod5ReadRecordBatch_t* batch,
                                                                    size_t batch_row,
                                                                    size_t* sample_count);

/// \brief Find the signal for a full read.
/// \param      reader          The reader to query.
/// \param      batch           The read batch to query.
/// \param      batch_row       The read row to query data for.
/// \param      sample_count    The number of samples allocated in [sample_data] (must equal the length of signal data in the queryied read row).
/// \param[out] sample_data     The output location for the queried samples.
/// \note The signal data is allocated by pod5 and should be released as appropriate by the caller.
POD5_FORMAT_EXPORT pod5_error_t pod5_get_read_complete_signal(Pod5FileReader_t* reader,
                                                              Pod5ReadRecordBatch_t* batch,
                                                              size_t batch_row,
                                                              size_t sample_count,
                                                              int16_t* signal);

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

/// \brief Create a new split pod5 file using specified filenames and options.
/// \param signal_filename  The filename of the signal file.
/// \param reads_filename   The filename of the reads file.
/// \param writer_name      A descriptive string for the user software writing this file.
/// \param options          Options controlling how the file will be written (optional).
POD5_FORMAT_EXPORT Pod5FileWriter_t* pod5_create_split_file(char const* signal_filename,
                                                            char const* reads_filename,
                                                            char const* writer_name,
                                                            Pod5WriterOptions_t const* options);
/// \brief Create a new combined pod5 file using specified filenames and options.
/// \param filename         The filename of the combined pod5 file.
/// \param writer_name      A descriptive string for the user software writing this file.
/// \param options          Options controlling how the file will be written.
POD5_FORMAT_EXPORT Pod5FileWriter_t* pod5_create_combined_file(char const* filename,
                                                               char const* writer_name,
                                                               Pod5WriterOptions_t const* options);

/// \brief Close a file writer, releasing all memory held by the writer.
POD5_FORMAT_EXPORT pod5_error_t pod5_close_and_free_writer(Pod5FileWriter_t* file);

/// \brief Add a new pore type to the file.
/// \param[out] pore_index  The index of the added pore.
/// \param      file        The file to add the new pore type to.
/// \param      channel     The channel the pore type uses.
/// \param      well        The well the pore type uses.
/// \param      pore_type   The pore type string for the pore.
POD5_FORMAT_EXPORT pod5_error_t pod5_add_pore(int16_t* pore_index,
                                              Pod5FileWriter_t* file,
                                              uint16_t channel,
                                              uint8_t well,
                                              char const* pore_type);

enum pod5_end_reason {
    POD5_END_REASON_UNKNOWN = 0,
    POD5_END_REASON_MUX_CHANGE = 1,
    POD5_END_REASON_UNBLOCK_MUX_CHANGE = 2,
    POD5_END_REASON_DATA_SERVICE_UNBLOCK_MUX_CHANGE = 3,
    POD5_END_REASON_SIGNAL_POSITIVE = 4,
    POD5_END_REASON_SIGNAL_NEGATIVE = 5
};
typedef enum pod5_end_reason pod5_end_reason_t;

/// \brief Add a new end reason type to the file.
/// \param[out] end_reason_index  The index of the added end reason.
/// \param      file        The file to add the new pore type to.
/// \param      end_reason  The end reason enumeration type for the end reason.
/// \param      forced      Was the end reason was forced by control, false if the end reason is signal driven.
POD5_FORMAT_EXPORT pod5_error_t pod5_add_end_reason(int16_t* end_reason_index,
                                                    Pod5FileWriter_t* file,
                                                    pod5_end_reason_t end_reason,
                                                    int forced);

/// \brief Add a new calibration to the file, calibrations are used to map ADC raw data units into floating point pico-amp space.
/// \param[out] end_reason_index  The index of the added end reason.
/// \param      file        The file to add the new pore type to.
/// \param      offset      The offset parameter for the calibration.
/// \param      scale       The scale parameter for the calibration.
POD5_FORMAT_EXPORT pod5_error_t pod5_add_calibration(int16_t* calibration_index,
                                                     Pod5FileWriter_t* file,
                                                     float offset,
                                                     float scale);

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
POD5_FORMAT_EXPORT pod5_error_t pod5_add_run_info(int16_t* run_info_index,
                                                  Pod5FileWriter_t* file,
                                                  char const* acquisition_id,
                                                  int64_t acquisition_start_time_ms,
                                                  int16_t adc_max,
                                                  int16_t adc_min,
                                                  size_t context_tags_count,
                                                  char const** context_tags_keys,
                                                  char const** context_tags_values,
                                                  char const* experiment_name,
                                                  char const* flow_cell_id,
                                                  char const* flow_cell_product_code,
                                                  char const* protocol_name,
                                                  char const* protocol_run_id,
                                                  int64_t protocol_start_time_ms,
                                                  char const* sample_id,
                                                  uint16_t sample_rate,
                                                  char const* sequencing_kit,
                                                  char const* sequencer_position,
                                                  char const* sequencer_position_type,
                                                  char const* software,
                                                  char const* system_name,
                                                  char const* system_type,
                                                  size_t tracking_id_count,
                                                  char const** tracking_id_keys,
                                                  char const** tracking_id_values);

/// \brief Add a read to the file.
/// \param      file            The file to add the reads to.
/// \param      read_count      The number of reads to add with this call.
/// \param      read_id         The read id to use (in binary form, must be 16 bytes long).
/// \param      pore            The pore type to use for the reads.
/// \param      calibration     The calibration to use for the reads.
/// \param      read_number     The read numbers.
/// \param      start_sample    The read's start sample.
/// \param      median_before   The median signal level before the read started.
/// \param      end_reason      The end reason for the reads.
/// \param      run_info        The run info for the reads.
/// \param      signal          The signal data for the reads.
/// \param      signal_size     The number of samples in the reads signal data.
/// \deprecated Use pod5_add_reads_data instead.
POD5_FORMAT_EXPORT pod5_error_t pod5_add_reads(Pod5FileWriter_t* file,
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
                                               uint32_t const* signal_size);

/// \brief Add a read to the file, with pre compressed signal chunk sections.
/// \param      file                    The file to add the reads to.
/// \param      read_count              The number of reads to add with this call.
/// \param      read_id                 The read id to use (in binary form, must be 16 bytes long).
/// \param      pore                    The pore type to use for the reads.
/// \param      calibration             The calibration to use for the reads.
/// \param      read_number             The read numbers.
/// \param      start_sample            The read's start sample.
/// \param      median_before           The median signal level before the read started.
/// \param      end_reason              The end reason for the reads.
/// \param      run_info                The run info for the reads.
/// \param      compressed_signal       The signal chunks data for the reads.
/// \param      compressed_signal_size  The sizes (in bytes) of each signal chunk.
/// \param      sample_counts           The number of samples of each signal chunk.
/// \param      signal_chunk_count      The number of sections of compressed signal.
/// \deprecated Use pod5_add_reads_data_pre_compressed instead.
POD5_FORMAT_EXPORT pod5_error_t pod5_add_reads_pre_compressed(Pod5FileWriter_t* file,
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
                                                              size_t const* signal_chunk_count);

/// \brief Add a read to the file.
/// \param      file            The file to add the reads to.
/// \param      read_count      The number of reads to add with this call.
/// \param      struct_version  The version of the struct of [row_data] being filled, use READ_BATCH_ROW_INFO_VERSION.
/// \param      row_data        The array data for injecting into the file, should be ReadBatchRowInfoArray_t.
/// \param      signal          The signal data for the reads.
/// \param      signal_size     The number of samples in the signal data.
POD5_FORMAT_EXPORT pod5_error_t pod5_add_reads_data(Pod5FileWriter_t* file,
                                                    uint32_t read_count,
                                                    uint16_t struct_version,
                                                    void const* row_data,
                                                    int16_t const** signal,
                                                    uint32_t const* signal_size);

/// \brief Add a read to the file, with pre compressed signal chunk sections.
/// \param      file                    The file to add the read to.
/// \param      read_count              The number of reads to add with this call.
/// \param      struct_version          The version of the struct of [row_data] being filled, use READ_BATCH_ROW_INFO_VERSION.
/// \param      row_data                The array data for injecting into the file, should be ReadBatchRowInfoArray_t.
/// \param      compressed_signal       The signal chunks data for the read.
/// \param      compressed_signal_size  The sizes (in bytes) of each signal chunk.
/// \param      sample_counts           The number of samples of each signal chunk.
/// \param      signal_chunk_count      The number of sections of compressed signal.
POD5_FORMAT_EXPORT pod5_error_t
pod5_add_reads_data_pre_compressed(Pod5FileWriter_t* file,
                                   uint32_t read_count,
                                   uint16_t struct_version,
                                   void const* row_data,
                                   char const*** compressed_signal,
                                   size_t const** compressed_signal_size,
                                   uint32_t const** sample_counts,
                                   size_t const* signal_chunk_count);

/// \brief Find the max size of a compressed array of samples.
/// \param sample_count The number of samples in the source signal.
/// \return The max number of bytes required for the compressed signal.
POD5_FORMAT_EXPORT size_t pod5_vbz_compressed_signal_max_size(size_t sample_count);

/// \brief VBZ compress an array of samples.
/// \param          signal                      The signal to compress.
/// \param          signal_size                 The number of samples to compress.
/// \param[out]     compressed_signal_out       The compressed signal.
/// \param[inout]   compressed_signal_size      The number of compressed bytes, should be set to the size of compressed_signal_out on call.
POD5_FORMAT_EXPORT pod5_error_t pod5_vbz_compress_signal(int16_t const* signal,
                                                         size_t signal_size,
                                                         char* compressed_signal_out,
                                                         size_t* compressed_signal_size);

/// \brief VBZ decompress an array of samples.
/// \param          compressed_signal           The signal to compress.
/// \param          compressed_signal_size      The number of compressed bytes, should be set to the size of compressed_signal_out on call.
/// \param          sample_count                The number of samples to decompress.
/// \param[out]     signal_out                  The compressed signal.
POD5_FORMAT_EXPORT pod5_error_t pod5_vbz_decompress_signal(char const* compressed_signal,
                                                           size_t compressed_signal_size,
                                                           size_t sample_count,
                                                           short* signal_out);

//---------------------------------------------------------------------------------------------------------------------
// Global state
//---------------------------------------------------------------------------------------------------------------------

/// \brief Format a packed binary read id as a readable read id string:
/// \param          read_id           A 16 byte binary formatted UUID.
/// \param[out]     read_id_string    Output string containing the string formatted UUID (expects a string of at least 37 bytes, one null byte is written.)
POD5_FORMAT_EXPORT pod5_error_t pod5_format_read_id(uint8_t const* read_id, char* read_id_string);

#ifdef __cplusplus
}
#endif

//std::shared_ptr<arrow::Schema> pyarrow_test();