#pragma once

#include "mkr_format/mkr_format_export.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct MkrFileReader;
typedef struct MkrFileReader MkrFileReader_t;
struct MkrFileWriter;
typedef struct MkrFileWriter MkrFileWriter_t;
struct MkrReadRecordBatch;
typedef struct MkrReadRecordBatch MkrReadRecordBatch_t;

//---------------------------------------------------------------------------------------------------------------------
// Error management
//---------------------------------------------------------------------------------------------------------------------

/// \brief Integer error codes.
/// \note Taken from the arrow status enum.
enum mkr_error {
    MKR_OK = 0,
    MKR_ERROR_OUTOFMEMORY = 1,
    MKR_ERROR_KEYERROR = 2,
    MKR_ERROR_TYPEERROR = 3,
    MKR_ERROR_INVALID = 4,
    MKR_ERROR_IOERROR = 5,
    MKR_ERROR_CAPACITYERROR = 6,
    MKR_ERROR_INDEXERROR = 7,
    MKR_ERROR_CANCELLED = 8,
    MKR_ERROR_UNKNOWNERROR = 9,
    MKR_ERROR_NOTIMPLEMENTED = 10,
    MKR_ERROR_SERIALIZATIONERROR = 11,
};
typedef enum mkr_error mkr_error_t;

/// \brief Get the most recent error number from all mkr api's.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_error_no();
/// \brief Get the most recent error description string from all mkr api's.
/// \note The string's lifetime is internally managed, a caller should not free it.
MKR_FORMAT_EXPORT char const* mkr_get_error_string();

//---------------------------------------------------------------------------------------------------------------------
// Global state
//---------------------------------------------------------------------------------------------------------------------

/// \brief Initialise and register global mkr types
MKR_FORMAT_EXPORT mkr_error_t mkr_init();
/// \brief Terminate global mkr types
MKR_FORMAT_EXPORT mkr_error_t mkr_terminate();

//---------------------------------------------------------------------------------------------------------------------
// Reading files
//---------------------------------------------------------------------------------------------------------------------

/// \brief Open a split file reader
/// \param signal_filename  The filename of the signal file.
/// \param reads_filename   The filename of the reads file.
MKR_FORMAT_EXPORT MkrFileReader_t* mkr_open_split_file(char const* signal_filename,
                                                       char const* reads_filename);
/// \brief Open a combined file reader
/// \param filename         The filename of the combined mkr file.
MKR_FORMAT_EXPORT MkrFileReader_t* mkr_open_combined_file(char const* filename);

/// \brief Close a file reader, releasing all memory held by the reader.
MKR_FORMAT_EXPORT mkr_error_t mkr_close_and_free_reader(MkrFileReader_t* file);

struct EmbeddedFileData {
    size_t offset;
    size_t length;
};
typedef struct EmbeddedFileData EmbeddedFileData_t;

/// \brief Find the number of read batches in the file.
/// \param[out] file        The combined file to be queried.
/// \param      file_data   The output read table file data.
MKR_FORMAT_EXPORT mkr_error_t
mkr_get_combined_file_read_table_location(MkrFileReader_t* reader, EmbeddedFileData_t* file_data);

/// \brief Find the number of read batches in the file.
/// \param[out] file        The combined file to be queried.
/// \param      file_data   The output signal table file data.
MKR_FORMAT_EXPORT mkr_error_t
mkr_get_combined_file_signal_table_location(MkrFileReader_t* reader, EmbeddedFileData_t* file_data);

struct TraversalStep {
    /// \brief The read batch the data resides in:
    size_t batch;
    /// \brief The batch row the data resides in:
    size_t batch_row;
    /// \brief The original read_id index in the passed input data.
    size_t original_index;
};
typedef struct TraversalStep TraversalStep_t;

enum mkr_traversal_sort_type {
    // Sort the output so the reader can skip around in the file as little as possible.
    MKR_TRAV_SORT_READ_EFFICIENT = 0,
    // Sort the output traversal as the input data is.
    MKR_TRAV_SORT_ORIGINAL_ORDER = 1,
};
typedef enum mkr_traversal_sort_type mkr_traversal_sort_type_t;

/// \brief Plan the most efficient route through the data for the given read ids
/// \param      file                The file to be queried.
/// \param      read_id_array       The read id array (contiguous array, 16 bytes per id).
/// \param      read_id_count       The number of read ids.
/// \param      sort_type           Control how the sort traversal order should be organised.
/// \param[out] steps               The output steps in optimal order (must be an array of size read_id_count)
/// \param[out] find_success_count  The number of read ids that were successfully found.
/// \note The output array is sorted in file storage order, to improve read efficiency.
///       [find_success_count] is the number of successful find steps in the result [steps].
///       Failed finds are all sorted to the back of the [steps] array, and are marked with an
///       invalid batch and batch_row value.
MKR_FORMAT_EXPORT mkr_error_t mkr_plan_traversal(MkrFileReader_t* reader,
                                                 uint8_t* read_id_array,
                                                 size_t read_id_count,
                                                 mkr_traversal_sort_type_t sort_type,
                                                 TraversalStep_t* steps,
                                                 size_t* find_success_count);

/// \brief Find the number of read batches in the file.
/// \param[out] count   The number of read batches in the file
/// \param      reader  The file reader to read from
MKR_FORMAT_EXPORT mkr_error_t mkr_get_read_batch_count(size_t* count, MkrFileReader_t* reader);

/// \brief Get a read batch from the file.
/// \param[out] batch   The extracted batch.
/// \param      reader  The file reader to read from
/// \param      index   The index of the batch to read.
/// \note Batches returned from this API must be freed using #mkr_free_read_batch
MKR_FORMAT_EXPORT mkr_error_t mkr_get_read_batch(MkrReadRecordBatch_t** batch,
                                                 MkrFileReader_t* reader,
                                                 size_t index);

/// \brief Release a read batch when it is not longer used.
/// \param batch The batch to release.
MKR_FORMAT_EXPORT mkr_error_t mkr_free_read_batch(MkrReadRecordBatch_t* batch);

/// \brief Find the number of rows in a batch.
/// \param batch    The batch to query the number of rows for.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_read_batch_row_count(size_t* count, MkrReadRecordBatch_t*);

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
MKR_FORMAT_EXPORT mkr_error_t mkr_get_read_batch_row_info(MkrReadRecordBatch_t* batch,
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
/// \param      batch                       The read batch to query.
/// \param      row                         The row index to query.
/// \param      signal_row_indices_count    Number of entries in the signal_row_indices array.
/// \param[out] signal_row_indices          The signal row indices read out of the read row.
/// \note signal_row_indices_count Must equal signal_row_count returned from mkr_get_read_batch_row_info
///       or an error is generated.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_signal_row_indices(MkrReadRecordBatch_t* batch,
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
/// \note The returned pore value should be released using mkr_release_pore when it is no longer used.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_pore(MkrReadRecordBatch_t* batch,
                                           int16_t pore,
                                           PoreDictData_t** pore_data);

/// \brief Release a PoreDictData struct after use.
MKR_FORMAT_EXPORT mkr_error_t mkr_release_pore(PoreDictData_t* pore_data);

struct CalibrationDictData {
    float offset;
    float scale;
};
typedef struct CalibrationDictData CalibrationDictData_t;

/// \brief Find the calibration info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      calibration         The pore index to query.
/// \param[out] calibration_data    Output location for the calibration data.
/// \note The returned calibration value should be released using mkr_release_calibration when it is no longer used.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_calibration(MkrReadRecordBatch_t* batch,
                                                  int16_t calibration,
                                                  CalibrationDictData_t** calibration_data);

/// \brief Release a CalibrationDictData struct after use.
MKR_FORMAT_EXPORT mkr_error_t mkr_release_calibration(CalibrationDictData_t* calibration_data);

struct EndReasonDictData {
    char const* name;
    int forced;
};
typedef struct EndReasonDictData EndReasonDictData_t;

/// \brief Find the calibration info for a row in a read batch.
/// \param      batch               The read batch to query.
/// \param      end_reason          The end reason index to query.
/// \param[out] end_reason_data     Output location for the end reason data.
/// \note The returned end_reason value should be released using mkr_release_calibration when it is no longer used.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_end_reason(MkrReadRecordBatch_t* batch,
                                                 int16_t end_reason,
                                                 EndReasonDictData_t** end_reason_data);

/// \brief Release a CalibrationDictData struct after use.
MKR_FORMAT_EXPORT mkr_error_t mkr_release_end_reason(EndReasonDictData_t* end_reason_data);

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
/// \note The returned end_reason value should be released using mkr_release_calibration when it is no longer used.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_run_info(MkrReadRecordBatch_t* batch,
                                               int16_t run_info,
                                               RunInfoDictData_t** run_info_data);

/// \brief Release a CalibrationDictData struct after use.
MKR_FORMAT_EXPORT mkr_error_t mkr_release_run_info(RunInfoDictData_t* run_info_data);

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
MKR_FORMAT_EXPORT mkr_error_t mkr_get_signal_row_info(MkrFileReader_t* reader,
                                                      size_t signal_rows_count,
                                                      uint64_t* signal_rows,
                                                      SignalRowInfo_t** signal_row_info);

/// \brief Release a list of signal row infos allocated by [mkr_get_signal_row_info].
/// \param      signal_rows_count           The number of signal rows to release.
/// \param      signal_row_info             The signal row infos to release.
/// \note Calls to mkr_free_signal_row_info must be 1:1 with [mkr_get_signal_row_info], you cannot free part of the returned data.
MKR_FORMAT_EXPORT mkr_error_t mkr_free_signal_row_info(size_t signal_rows_count,
                                                       SignalRowInfo_t** signal_row_info);

/// \brief Find the info for a signal row in a reader.
/// \param      reader          The reader to query.
/// \param      row_info        The signal row info batch index to query data for.
/// \param      sample_count    The number of samples allocated in [sample_data] (must equal the length of signal data in the row).
/// \param[out] sample_data     The output location for the queried samples.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_signal(MkrFileReader_t* reader,
                                             SignalRowInfo_t* row_info,
                                             size_t sample_count,
                                             int16_t* sample_data);

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
struct MkrWriterOptions {
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
typedef struct MkrWriterOptions MkrWriterOptions_t;

/// \brief Create a new split mkr file using specified filenames and options.
/// \param signal_filename  The filename of the signal file.
/// \param reads_filename   The filename of the reads file.
/// \param writer_name      A descriptive string for the user software writing this file.
/// \param options          Options controlling how the file will be written (optional).
MKR_FORMAT_EXPORT MkrFileWriter_t* mkr_create_split_file(char const* signal_filename,
                                                         char const* reads_filename,
                                                         char const* writer_name,
                                                         MkrWriterOptions_t const* options);
/// \brief Create a new combined mkr file using specified filenames and options.
/// \param filename         The filename of the combined mkr file.
/// \param writer_name      A descriptive string for the user software writing this file.
/// \param options          Options controlling how the file will be written.
MKR_FORMAT_EXPORT MkrFileWriter_t* mkr_create_combined_file(char const* filename,
                                                            char const* writer_name,
                                                            MkrWriterOptions_t const* options);

/// \brief Close a file writer, releasing all memory held by the writer.
MKR_FORMAT_EXPORT mkr_error_t mkr_close_and_free_writer(MkrFileWriter_t* file);

/// \brief Add a new pore type to the file.
/// \param[out] pore_index  The index of the added pore.
/// \param      file        The file to add the new pore type to.
/// \param      channel     The channel the pore type uses.
/// \param      well        The well the pore type uses.
/// \param      pore_type   The pore type string for the pore.
MKR_FORMAT_EXPORT mkr_error_t mkr_add_pore(int16_t* pore_index,
                                           MkrFileWriter_t* file,
                                           uint16_t channel,
                                           uint8_t well,
                                           char const* pore_type);

enum mkr_end_reason {
    MKR_END_REASON_UNKNOWN = 0,
    MKR_END_REASON_MUX_CHANGE = 1,
    MKR_END_REASON_UNBLOCK_MUX_CHANGE = 2,
    MKR_END_REASON_DATA_SERVICE_UNBLOCK_MUX_CHANGE = 3,
    MKR_END_REASON_SIGNAL_POSITIVE = 4,
    MKR_END_REASON_SIGNAL_NEGATIVE = 5
};
typedef enum mkr_end_reason mkr_end_reason_t;

/// \brief Add a new end reason type to the file.
/// \param[out] end_reason_index  The index of the added end reason.
/// \param      file        The file to add the new pore type to.
/// \param      end_reason  The end reason enumeration type for the end reason.
/// \param      forced      Was the end reason was forced by control, false if the end reason is signal driven.
MKR_FORMAT_EXPORT mkr_error_t mkr_add_end_reason(int16_t* end_reason_index,
                                                 MkrFileWriter_t* file,
                                                 mkr_end_reason_t end_reason,
                                                 int forced);

/// \brief Add a new calibration to the file, calibrations are used to map ADC raw data units into floating point pico-amp space.
/// \param[out] end_reason_index  The index of the added end reason.
/// \param      file        The file to add the new pore type to.
/// \param      offset      The offset parameter for the calibration.
/// \param      scale       The scale parameter for the calibration.
MKR_FORMAT_EXPORT mkr_error_t mkr_add_calibration(int16_t* calibration_index,
                                                  MkrFileWriter_t* file,
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
MKR_FORMAT_EXPORT mkr_error_t mkr_add_run_info(int16_t* run_info_index,
                                               MkrFileWriter_t* file,
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

typedef uint8_t read_id_t[16];

/// \brief Add a read to the file.
/// \param      file            The file to add the read to.
/// \param      read_count      The number of reads to add with this call.
/// \param      read_id         The read id to use (in binary form, must be 16 bytes long).
/// \param      pore            The pore type to use for the read.
/// \param      calibration     The calibration to use for the read.
/// \param      read_number     The read number.
/// \param      start_sample    The read's start sample.
/// \param      median_before   The median signal level before the read started.
/// \param      end_reason      The end reason for the read.
/// \param      run_info        The run info for the read.
/// \param      signal          The signal data for the read.
/// \param      signal_size     The number of samples in the signal data.
MKR_FORMAT_EXPORT mkr_error_t mkr_add_reads(MkrFileWriter_t* file,
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
/// \param      file                    The file to add the read to.
/// \param      read_count      The number of reads to add with this call.
/// \param      read_id                 The read id to use (in binary form, must be 16 bytes long).
/// \param      pore                    The pore type to use for the read.
/// \param      calibration             The calibration to use for the read.
/// \param      read_number             The read number.
/// \param      start_sample            The read's start sample.
/// \param      median_before           The median signal level before the read started.
/// \param      end_reason              The end reason for the read.
/// \param      run_info                The run info for the read.
/// \param      compressed_signal       The signal chunks data for the read.
/// \param      compressed_signal_size  The sizes (in bytes) of each signal chunk.
/// \param      sample_counts           The number of samples of each signal chunk.
/// \param      signal_chunk_count      The number of sections of compressed signal.
MKR_FORMAT_EXPORT mkr_error_t mkr_add_reads_pre_compressed(MkrFileWriter_t* file,
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

/// \brief Find the max size of a compressed array of samples.
/// \param sample_count The number of samples in the source signal.
/// \return The max number of bytes required for the compressed signal.
MKR_FORMAT_EXPORT size_t mkr_vbz_compressed_signal_max_size(size_t sample_count);

/// \brief VBZ compress an array of samples.
/// \param          signal                      The signal to compress.
/// \param          signal_size                 The number of samples to compress.
/// \param[out]     compressed_signal_out       The compressed signal.
/// \param[inout]   compressed_signal_size      The number of compressed bytes, should be set to the size of compressed_signal_out on call.
MKR_FORMAT_EXPORT mkr_error_t mkr_vbz_compress_signal(int16_t const* signal,
                                                      size_t signal_size,
                                                      char* compressed_signal_out,
                                                      size_t* compressed_signal_size);

/// \brief VBZ decompress an array of samples.
/// \param          compressed_signal           The signal to compress.
/// \param          compressed_signal_size      The number of compressed bytes, should be set to the size of compressed_signal_out on call.
/// \param          sample_count                The number of samples to decompress.
/// \param[out]     signal_out                  The compressed signal.
MKR_FORMAT_EXPORT mkr_error_t mkr_vbz_decompress_signal(char const* compressed_signal,
                                                        size_t compressed_signal_size,
                                                        size_t sample_count,
                                                        short* signal_out);

//---------------------------------------------------------------------------------------------------------------------
// Global state
//---------------------------------------------------------------------------------------------------------------------

/// \brief Format a packed binary read id as a readable read id string:
/// \param          read_id           A 16 byte binary formatted UUID.
/// \param[out]     read_id_string    Output string containing the string formatted UUID (expects a string of at least 37 bytes, one null byte is written.)
MKR_FORMAT_EXPORT mkr_error_t mkr_format_read_id(uint8_t* read_id, char* read_id_string);

#ifdef __cplusplus
}
#endif

//std::shared_ptr<arrow::Schema> pyarrow_test();