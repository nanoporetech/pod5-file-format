#pragma once
#include <arrow/memory_pool.h>

namespace pod5 {

/// \brief Find a memory pool that should be used by default when opening or creating a pod5 file.
/// \note This function differs from the arrow equivalent by not using jemalloc on systems with large
///       pages, which jemalloc does not support.
arrow::MemoryPool * default_memory_pool();

}  // namespace pod5
