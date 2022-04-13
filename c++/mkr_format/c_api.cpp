#include "mkr_format/c_api.h"
#include "mkr_format/file_reader.h"
#include "mkr_format/file_writer.h"

#include <arrow/type.h>

#include <iostream>

struct MkrFileReader {
    MkrFileReader(std::unique_ptr<mkr::FileReader> reader_)
    : reader(std::move(reader_))
    {
    }
    std::unique_ptr<mkr::FileReader> reader;
};

struct MkrFileWriter {
    MkrFileWriter(std::unique_ptr<mkr::FileWriter> writer_)
    : writer(std::move(writer_))
    {
    }
    std::unique_ptr<mkr::FileWriter> writer;
};

extern "C" {

//---------------------------------------------------------------------------------------------------------------------
mkr_error_t g_error_no;
std::string g_error_string;

void mkr_reset_error() {
    g_error_no = mkr_error_t::MKR_OK;
    g_error_string.clear();
}

void mkr_set_error(arrow::Status status) {
    g_error_no = (mkr_error_t)status.code();
    g_error_string = status.ToString();
}

mkr_error_t mkr_get_error_no() {
    return g_error_no;
}

char const* mkr_get_error_string() {
    return g_error_string.c_str();
}

#define MKR_C_RETURN_NOT_OK(result) \
    if (!result.ok()) { \
        mkr_set_error(result.status()); \
        return g_error_no; \
    }


#define MKR_C_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr)                              \
  auto&& result_name = (rexpr);                                                          \
  if (!(result_name).ok()) {                                                             \
      mkr_set_error((result_name).status());                                             \
      return g_error_no;                                                                 \
  }                                                                                      \
  lhs = std::move(result_name).ValueUnsafe();

#define MKR_C_ASSIGN_OR_RAISE(lhs, rexpr)                                                \
  MKR_C_ASSIGN_OR_RAISE_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__),   \
                             lhs, rexpr);

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

    if (!check_string_not_empty(signal_filename)|| !check_string_not_empty(reads_filename)) {
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

void mkr_close_and_free_reader(MkrFileReader* file) {
    mkr_reset_error();

    std::unique_ptr<MkrFileReader> ptr{ file };
    ptr.reset();
}

//---------------------------------------------------------------------------------------------------------------------
mkr::FileWriterOptions make_internal_writer_options(WriterOptions const* options) {
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

MkrFileWriter* mkr_create_split_file(char const* signal_filename, char const* reads_filename, char const* writer_name, WriterOptions const* options) {
    mkr_reset_error();

    if (!check_string_not_empty(signal_filename) || !check_string_not_empty(reads_filename) || !check_string_not_empty(writer_name)) {
        return nullptr;
    }

    auto internal_writer = mkr::create_split_file_writer(signal_filename, reads_filename, writer_name, make_internal_writer_options(options));
    if (!internal_writer.ok()) {
        mkr_set_error(internal_writer.status());
        return nullptr;
    }

    auto writer = std::make_unique<MkrFileWriter>(std::move(*internal_writer));
    return writer.release();
}

MkrFileWriter* mkr_create_combined_file(char const* filename, char const* writer_name, WriterOptions const* options) {
    mkr_reset_error();

    if (!check_string_not_empty(filename) || !check_string_not_empty(writer_name)) {
        return nullptr;
    }


    auto internal_writer = mkr::create_combined_file_writer(filename, writer_name, make_internal_writer_options(options));
    if (!internal_writer.ok()) {
        mkr_set_error(internal_writer.status());
        return nullptr;
    }

    auto writer = std::make_unique<MkrFileWriter>(std::move(*internal_writer));
    return writer.release();
}

void mkr_close_and_free_writer(MkrFileWriter* file) {
    mkr_reset_error();

    std::unique_ptr<MkrFileWriter> ptr{ file };
    ptr.reset();
}

mkr_error_t mkr_add_pore(int16_t* pore_index, MkrFileWriter* file, std::uint16_t channel, std::uint8_t well, char const* pore_type) {
    mkr_reset_error();

    if (!check_string_not_empty(pore_type) || !check_file_not_null(file) || !check_output_pointer_not_null(pore_index)) {
        return g_error_no;
    }

    MKR_C_ASSIGN_OR_RAISE(*pore_index, file->writer->add_pore({ channel, well, pore_type }));
    return MKR_OK;
}

mkr_error_t mkr_add_end_reason(int16_t* end_reason_index, MkrFileWriter* file, mkr_end_reason_t end_reason, bool forced) {
    mkr_reset_error();

    if (!check_file_not_null(file) || !check_output_pointer_not_null(end_reason_index)) {
        return g_error_no;
    }

    mkr::EndReasonData::ReadEndReason end_reason_internal = mkr::EndReasonData::ReadEndReason::unknown;
    switch(end_reason) {
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
        mkr_set_error(arrow::Status::Invalid("out of range end reason passed to mkr_add_end_reason"));
        return g_error_no;        
    }

    MKR_C_ASSIGN_OR_RAISE(*end_reason_index, file->writer->add_end_reason({ end_reason_internal, forced }));
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