#pragma once

#include <arrow/result.h>

namespace mkr {

/// mkr::Result is just an Arrow Result right now.
template <typename R>
using Result = arrow::Result<R>;
using Status = arrow::Status;

}  // namespace mkr
