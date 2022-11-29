#pragma once

#include "pod5_format/result.h"

#include <memory>

namespace arrow {
class MemoryPool;
}

namespace pod5 {

class FileReader;

/// \brief Write the path [destination] with any migrated data from [source].
/// \param source The source file data to write updated.
/// \param destination The destination path to write the data to.
/// \note The destination path should not be the same file that was opened for input.
pod5::Status update_file(
    arrow::MemoryPool * pool,
    std::shared_ptr<FileReader> const & source,
    std::string destination);

}  // namespace pod5
