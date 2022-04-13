#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/signal_table_reader.h"

extern "C" {

struct MkrFileReader;
struct MkrFileWriter;

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
mkr_error_t mkr_get_error_no();
/// \brief Get the most recent error description string from all mkr api's.
/// \note The string's lifetime is internally managed, a caller should not free it.
char const* mkr_get_error_string();


//---------------------------------------------------------------------------------------------------------------------
// Reading files
//---------------------------------------------------------------------------------------------------------------------

/// \brief Open a split file reader
/// \param signal_filename  The filename of the signal file.
/// \param reads_filename   The filename of the reads file.
MkrFileReader* mkr_open_split_file(char const* signal_filename, char const* reads_filename);
/// \brief Open a combined file reader
/// \param filename         The filename of the combined mkr file.
MkrFileReader* mkr_open_combined_file(char const* filename);

/// \brief Close a file reader, releasing all memory held by the reader.
void mkr_close_and_free_reader(MkrFileReader* file);


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
struct WriterOptions {
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
MkrFileWriter* mkr_create_split_file(char const* signal_filename, char const* reads_filename, char const* writer_name, WriterOptions const* options);
/// \brief Create a new combined mkr file using specified filenames and options.
/// \param filename         The filename of the combined mkr file.
/// \param writer_name      A descriptive string for the user software writing this file.
/// \param options          Options controlling how the file will be written.
MkrFileWriter* mkr_create_combined_file(char const* filename, char const* writer_name, WriterOptions const* options);

/// \brief Close a file writer, releasing all memory held by the writer.
void mkr_close_and_free_writer(MkrFileWriter* file);

/// \brief Add a new pore type to the file.
/// \param[out] pore_index  The index of the added pore.
/// \param      file        The file to add the new pore type to.
/// \param      channel     The channel the pore type uses.
/// \param      well        The well the pore type uses.
/// \param      pore_type   The pore type string for the pore.
mkr_error_t mkr_add_pore(int16_t* pore_index, MkrFileWriter* file, std::uint16_t channel, std::uint8_t well, char const* pore_type);

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
mkr_error_t mkr_add_end_reason(int16_t* end_reason_index, MkrFileWriter* file, mkr_end_reason_t end_reason, bool forced);
}

//std::shared_ptr<arrow::Schema> pyarrow_test();