#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/signal_table_reader.h"

extern "C" {

struct MkrFileReader;
struct MkrFileWriter;
struct MkrReadRecordBatch;

//---------------------------------------------------------------------------------------------------------------------
// Global state
//---------------------------------------------------------------------------------------------------------------------

/// \brief Initialise and register global mkr types
MKR_FORMAT_EXPORT void mkr_init();
/// \brief Terminate global mkr types
MKR_FORMAT_EXPORT void mkr_terminate();

//---------------------------------------------------------------------------------------------------------------------
// Error management
//---------------------------------------------------------------------------------------------------------------------

/// \brief Integer error codes.
/// \note Taken from the arrow status enum.
enum mkr_error_t {
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

/// \brief Get the most recent error number from all mkr api's.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_error_no();
/// \brief Get the most recent error description string from all mkr api's.
/// \note The string's lifetime is internally managed, a caller should not free it.
MKR_FORMAT_EXPORT char const* mkr_get_error_string();

//---------------------------------------------------------------------------------------------------------------------
// Reading files
//---------------------------------------------------------------------------------------------------------------------

/// \brief Open a split file reader
/// \param signal_filename  The filename of the signal file.
/// \param reads_filename   The filename of the reads file.
MKR_FORMAT_EXPORT MkrFileReader* mkr_open_split_file(char const* signal_filename,
                                                     char const* reads_filename);
/// \brief Open a combined file reader
/// \param filename         The filename of the combined mkr file.
MKR_FORMAT_EXPORT MkrFileReader* mkr_open_combined_file(char const* filename);

/// \brief Close a file reader, releasing all memory held by the reader.
MKR_FORMAT_EXPORT mkr_error_t mkr_close_and_free_reader(MkrFileReader* file);

/// \brief Find the number of read batches in the file.
/// \param[out] count   The number of read batches in the file
/// \param      reader  The file reader to read from
MKR_FORMAT_EXPORT mkr_error_t mkr_get_read_batch_count(size_t* count, MkrFileReader* reader);

/// \brief Get a read batch from the file.
/// \param[out] batch   The extracted batch.
/// \param      reader  The file reader to read from
/// \param      index   The index of the batch to read.
/// \note Batches returned from this API must be freed using #mkr_free_read_batch
MKR_FORMAT_EXPORT mkr_error_t mkr_get_read_batch(MkrReadRecordBatch** batch,
                                                 MkrFileReader* reader,
                                                 size_t index);

/// \brief Release a read batch when it is not longer used.
/// \param batch The batch to release.
MKR_FORMAT_EXPORT mkr_error_t mkr_free_read_batch(MkrReadRecordBatch* batch);

/// \brief Find the number of rows in a batch.
/// \param batch    The batch to query the number of rows for.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_read_batch_row_count(size_t* count, MkrReadRecordBatch*);

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
MKR_FORMAT_EXPORT mkr_error_t mkr_get_read_batch_row_info(MkrReadRecordBatch* batch,
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
/// \param      pore                The pore index to query.
/// \param[out] channel             Output location for the channel.
/// \param[out] well                Output location for the well.
/// \param[out] pore_type           Output location for the pore type.
/// \note The string data returned from this method is internally managed and should not be released.
MKR_FORMAT_EXPORT mkr_error_t mkr_get_pore(MkrReadRecordBatch* batch,
                                           int16_t pore,
                                           uint16_t* channel,
                                           uint8_t* well,
                                           char** pore_type);

//---------------------------------------------------------------------------------------------------------------------
// Writing files
//---------------------------------------------------------------------------------------------------------------------

// Signal compression options.
/// \brief Use the default signal compression option.
std::int8_t DEFAULT_SIGNAL_COMPRESSION = 0;
/// \brief Use vbz to compress read signals in tables.
std::int8_t VBZ_SIGNAL_COMPRESSION = 1;
/// \brief Write signals uncompressed to tables.
std::int8_t UNCOMPRESSED_SIGNAL = 2;

// Options to control how a file is written.
struct MkrWriterOptions {
    /// \brief Maximum number of samples to place in one signal record in the signals table.
    /// \note Use zero to use default value.
    std::uint32_t max_signal_chunk_size;
    /// \brief Signal type to write to the signals table.
    /// \note Use 'DEFAULT_SIGNAL_COMPRESSION' to use default value.
    std::int8_t signal_compression_type;
};

/// \brief Create a new split mkr file using specified filenames and options.
/// \param signal_filename  The filename of the signal file.
/// \param reads_filename   The filename of the reads file.
/// \param writer_name      A descriptive string for the user software writing this file.
/// \param options          Options controlling how the file will be written (optional).
MKR_FORMAT_EXPORT MkrFileWriter* mkr_create_split_file(char const* signal_filename,
                                                       char const* reads_filename,
                                                       char const* writer_name,
                                                       MkrWriterOptions const* options);
/// \brief Create a new combined mkr file using specified filenames and options.
/// \param filename         The filename of the combined mkr file.
/// \param writer_name      A descriptive string for the user software writing this file.
/// \param options          Options controlling how the file will be written.
MKR_FORMAT_EXPORT MkrFileWriter* mkr_create_combined_file(char const* filename,
                                                          char const* writer_name,
                                                          MkrWriterOptions const* options);

/// \brief Close a file writer, releasing all memory held by the writer.
MKR_FORMAT_EXPORT mkr_error_t mkr_close_and_free_writer(MkrFileWriter* file);

/// \brief Add a new pore type to the file.
/// \param[out] pore_index  The index of the added pore.
/// \param      file        The file to add the new pore type to.
/// \param      channel     The channel the pore type uses.
/// \param      well        The well the pore type uses.
/// \param      pore_type   The pore type string for the pore.
MKR_FORMAT_EXPORT mkr_error_t mkr_add_pore(int16_t* pore_index,
                                           MkrFileWriter* file,
                                           std::uint16_t channel,
                                           std::uint8_t well,
                                           char const* pore_type);

enum mkr_end_reason_t {
    MKR_END_REASON_UNKNOWN = 0,
    MKR_END_REASON_MUX_CHANGE = 1,
    MKR_END_REASON_UNBLOCK_MUX_CHANGE = 2,
    MKR_END_REASON_DATA_SERVICE_UNBLOCK_MUX_CHANGE = 3,
    MKR_END_REASON_SIGNAL_POSITIVE = 4,
    MKR_END_REASON_SIGNAL_NEGATIVE = 5
};

/// \brief Add a new end reason type to the file.
/// \param[out] end_reason_index  The index of the added end reason.
/// \param      file        The file to add the new pore type to.
/// \param      end_reason  The end reason enumeration type for the end reason.
/// \param      forced      Was the end reason was forced by control, false if the end reason is signal driven.
MKR_FORMAT_EXPORT mkr_error_t mkr_add_end_reason(int16_t* end_reason_index,
                                                 MkrFileWriter* file,
                                                 mkr_end_reason_t end_reason,
                                                 bool forced);

/// \brief Add a new calibration to the file, calibrations are used to map ADC raw data units into floating point pico-amp space.
/// \param[out] end_reason_index  The index of the added end reason.
/// \param      file        The file to add the new pore type to.
/// \param      offset      The offset parameter for the calibration.
/// \param      scale       The scale parameter for the calibration.
MKR_FORMAT_EXPORT mkr_error_t mkr_add_calibration(int16_t* calibration_index,
                                                  MkrFileWriter* file,
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
                                               MkrFileWriter* file,
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
/// \param      file            The file to add the read to (in binary form, must be 16 bytes long)
/// \param      read_id         The offset parameter for the calibration.
/// \param      pore            The pore type to use for the read.
/// \param      calibration     The calibration to use for the read.
/// \param      read_number     The read number.
/// \param      start_sample    The read's start sample.
/// \param      median_before   The median signal level before the read started.
/// \param      end_reason      The end reason for the read.
/// \param      run_info        The run info for the read.
/// \param      signal          The signal data for the read.
/// \param      signal_size     The number of samples in the signal data.
MKR_FORMAT_EXPORT mkr_error_t mkr_add_read(MkrFileWriter* file,
                                           uint8_t const* read_id,
                                           int16_t pore,
                                           int16_t calibration,
                                           uint32_t read_number,
                                           uint64_t start_sample,
                                           float median_before,
                                           int16_t end_reason,
                                           int16_t run_info,
                                           int16_t const* signal,
                                           size_t signal_size);
}

//std::shared_ptr<arrow::Schema> pyarrow_test();