#include "pod5_format/migration/migration.h"

#include <random>

namespace pod5 {

static bool registered_delete_at_exit_called = false;
std::vector<arrow::internal::PlatformFilename> registered_delete_at_exit_paths;

void register_delete_at_exit(arrow::internal::PlatformFilename const & path)
{
    registered_delete_at_exit_paths.push_back(path);

    if (!registered_delete_at_exit_called) {
        std::atexit([] {
            std::size_t delete_failed = 0;
            for (auto const & path : registered_delete_at_exit_paths) {
                auto result = ::arrow::internal::DeleteDirTree(path);
                if (!result.ok()) {
                    delete_failed += 1;
                }
            }

            if (delete_failed > 0) {
                std::cerr << "Warning: Failed to remove " << delete_failed
                          << " temporary migration directories at exit.\n";
            }
        });
        registered_delete_at_exit_called = true;
    }
}

Result<std::unique_ptr<TemporaryDir>> MakeTmpDir(char const * suffix)
{
    std::default_random_engine gen(
        static_cast<std::default_random_engine::result_type>(arrow::internal::GetRandomSeed()));

    for (std::uint32_t counter = 0; counter < 5; ++counter) {
        std::string tmp_path = std::string{".tmp_"} + suffix;

        tmp_path += "_" + std::to_string(gen());

        ARROW_ASSIGN_OR_RAISE(
            auto filename, arrow::internal::PlatformFilename::FromString(tmp_path));
        ARROW_ASSIGN_OR_RAISE(auto created, CreateDir(filename));
        if (created) {
            return std::make_unique<TemporaryDir>(std::move(filename));
        }
    }

    return arrow::Status::Invalid("Failed to make temporary directory");
}

}  // namespace pod5
