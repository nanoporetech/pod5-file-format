#pragma once

#include <arrow/result.h>

namespace pod5 {

/// pod5::Result is just an Arrow Result right now.
template <typename R>
using Result = arrow::Result<R>;
using Status = arrow::Status;

}  // namespace pod5
