#pragma once

#include "pod5_format/pod5_format_export.h"

#include <system_error>

namespace pod5 {

/// High-level POD5 errors.
///
enum class Errors : int {
    /// The writer failed to append data to a batch
    failed_to_append_data_to_batch = 1,

    /// Failed to finish building an arrow column
    failed_to_finish_building_column = 2,

    /// Failed to write record batch
    failed_to_write_record_batch = 3,
};

POD5_FORMAT_EXPORT std::error_category const & error_category();

inline std::error_code make_error_code(Errors result)
{
    return std::error_code(static_cast<int>(result), error_category());
}

/// \brief Propagate any non-successful Status to the caller
#define POD5_ARROW_RETURN_NOT_OK(status, code)                            \
    do {                                                                  \
        ::arrow::Status __s = ::arrow::internal::GenericToStatus(status); \
        if (ARROW_PREDICT_FALSE(!__s.ok())) {                             \
            return make_error_code(code);                                 \
        }                                                                 \
    } while (false)

}  // namespace pod5
