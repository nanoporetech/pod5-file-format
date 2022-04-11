#include "mkr_format/c_api.h"

#include <arrow/type.h>

#include <iostream>

extern "C" {

struct MkrSignalFileWrapper {};

MkrSignalFileWrapper* mkr_open_signal_file(char const* filename) {
    std::cout << "wat\n";
    return (MkrSignalFileWrapper*)malloc(sizeof(MkrSignalFileWrapper));
}

void mkr_free_signal_file(MkrSignalFileWrapper* file) {
    std::cout << "free file\n";
    free(file);
}
}

std::shared_ptr<arrow::Schema> pyarrow_test() {
    return arrow::schema({
            arrow::field("signal", arrow::large_list(arrow::int16())),
            arrow::field("samples", arrow::uint32()),
    });
}