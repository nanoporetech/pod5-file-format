#include "env_vars.h"
static std::optional<std::filesystem::path> const POD5_MIGRATION_TMP_DIR =
    []() -> std::optional<std::filesystem::path> {
    auto tmp_dir = std::getenv("POD5_MIGRATION_TMP_DIR");
    if (!tmp_dir || *tmp_dir == '\0') {
        return std::nullopt;
    }
    try {
        std::filesystem::path dir_path(tmp_dir);
        if (std::filesystem::exists(dir_path) && !std::filesystem::is_directory(dir_path)) {
            return std::nullopt;
        }
        return dir_path;
    } catch (...) {
        return std::nullopt;
    }
}();

namespace pod5 {

std::optional<std::filesystem::path> get_migration_tmp_dir() { return POD5_MIGRATION_TMP_DIR; }

}  // namespace pod5
