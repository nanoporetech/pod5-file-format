#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/signal_table_reader.h"

extern "C" {

struct MkrSignalFileWrapper;

MkrSignalFileWrapper* mkr_open_signal_file(char const* filename);
void mkr_free_signal_file(MkrSignalFileWrapper* file);
}

std::shared_ptr<arrow::Schema> pyarrow_test();