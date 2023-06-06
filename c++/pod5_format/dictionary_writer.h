#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"

namespace arrow {
class Array;
}

namespace pod5 {

class POD5_FORMAT_EXPORT DictionaryWriter {
public:
    virtual ~DictionaryWriter() = default;

    pod5::Result<std::shared_ptr<arrow::Array>> build_dictionary_array(
        std::shared_ptr<arrow::Array> const & indices);
    virtual pod5::Result<std::shared_ptr<arrow::Array>> get_value_array() = 0;
    virtual std::size_t item_count() = 0;

    bool is_valid(std::size_t value) { return value < item_count(); }
};

}  // namespace pod5
