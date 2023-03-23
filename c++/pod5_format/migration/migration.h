#pragma once

#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/result.h"
#include "pod5_format/schema_utils.h"

#include <arrow/util/io_util.h>

namespace pod5 {

class TemporaryDir {
public:
    TemporaryDir(arrow::internal::PlatformFilename && path) : m_path(path) {}

    ~TemporaryDir() { (void)::arrow::internal::DeleteDirTree(m_path); }

    arrow::internal::PlatformFilename const & path() { return m_path; };

private:
    arrow::internal::PlatformFilename m_path;
};

Result<std::unique_ptr<TemporaryDir>> MakeTmpDir(char const * suffix);

class MigrationResult {
public:
    MigrationResult(combined_file_utils::ParsedFooter const & footer) : m_footer(footer) {}

    MigrationResult(MigrationResult &&) = default;
    MigrationResult & operator=(MigrationResult &&) = default;
    MigrationResult(MigrationResult const &) = delete;
    MigrationResult & operator=(MigrationResult const &) = delete;

    combined_file_utils::ParsedFooter & footer() { return m_footer; }

    combined_file_utils::ParsedFooter const & footer() const { return m_footer; }

    void add_temp_dir(std::unique_ptr<TemporaryDir> && temp_dir)
    {
        m_temp_dirs.emplace_back(std::move(temp_dir));
    }

private:
    combined_file_utils::ParsedFooter m_footer;
    std::vector<std::unique_ptr<TemporaryDir>> m_temp_dirs;
};

arrow::Result<MigrationResult> migrate_v0_to_v1(
    MigrationResult && v0_input,
    arrow::MemoryPool * pool);
arrow::Result<MigrationResult> migrate_v1_to_v2(
    MigrationResult && v1_input,
    arrow::MemoryPool * pool);
arrow::Result<MigrationResult> migrate_v2_to_v3(
    MigrationResult && v2_input,
    arrow::MemoryPool * pool);

inline arrow::Result<MigrationResult> migrate_if_required(
    Version writer_version,
    combined_file_utils::ParsedFooter const & read_footer,
    std::shared_ptr<arrow::io::RandomAccessFile> const & source,
    arrow::MemoryPool * pool)
{
    MigrationResult result{read_footer};

    if (writer_version < Version(0, 0, 24)) {
        // Added fields for read scaling
        ARROW_ASSIGN_OR_RAISE(result, migrate_v0_to_v1(std::move(result), pool));
    }

    if (writer_version < Version(0, 0, 32)) {
        // Added num samples field
        ARROW_ASSIGN_OR_RAISE(result, migrate_v1_to_v2(std::move(result), pool));
    }

    if (writer_version < Version(0, 0, 38)) {
        // Flattening fields
        ARROW_ASSIGN_OR_RAISE(result, migrate_v2_to_v3(std::move(result), pool));
    }
    return result;
}

}  // namespace pod5
