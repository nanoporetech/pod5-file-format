#pragma once

#include <filesystem>
#include <optional>

namespace pod5 {
std::optional<std::filesystem::path> get_migration_tmp_dir();

}  // namespace pod5
