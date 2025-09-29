#include "pod5_format/migration/migration.h"

#include <random>

namespace pod5 {

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
