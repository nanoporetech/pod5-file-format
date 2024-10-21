#pragma once

#include <arrow/io/file.h>
#include <arrow/status.h>

namespace pod5 {

class FileOutputStream : public arrow::io::OutputStream {
public:
    virtual arrow::Status batch_complete() { return arrow::Status::OK(); }

    virtual void set_file_start_offset(std::size_t val) {}
};

}  // namespace pod5
