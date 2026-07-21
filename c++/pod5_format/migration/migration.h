#pragma once

#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"
#include "pod5_format/schema_utils.h"
#include "pod5_format/version_constants.h"

#include <arrow/util/io_util.h>

#include <iostream>

namespace pod5 {

POD5_FORMAT_EXPORT void register_delete_at_exit(arrow::internal::PlatformFilename const & path);

class TemporaryDir {
public:
    TemporaryDir(arrow::internal::PlatformFilename && path) : m_path(path) {}

    ~TemporaryDir() { cleanup(); }

    arrow::internal::PlatformFilename const & path() { return m_path; };

    void cleanup()
    {
        if (m_path.ToString().empty()) {
            return;
        }

        auto result = ::arrow::internal::DeleteDirTree(m_path);
        if (!result.ok()) {
            // Push the delete of this directory off to exit, when all open file handles should be closed.
            register_delete_at_exit(m_path);
        } else {
            m_path = {};
        }
    }

private:
    arrow::internal::PlatformFilename m_path;
};

POD5_FORMAT_EXPORT Result<std::unique_ptr<TemporaryDir>> MakeTmpDir(char const * suffix);

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
    // This is first so we clean it up last, after the
    // footer and any open files it contains is destroyed.
    std::vector<std::unique_ptr<TemporaryDir>> m_temp_dirs;
    combined_file_utils::ParsedFooter m_footer;
};

POD5_FORMAT_EXPORT arrow::Result<MigrationResult> migrate_v0_to_v1(
    MigrationResult && v0_input,
    arrow::MemoryPool * pool);
POD5_FORMAT_EXPORT arrow::Result<MigrationResult> migrate_v1_to_v2(
    MigrationResult && v1_input,
    arrow::MemoryPool * pool);
POD5_FORMAT_EXPORT arrow::Result<MigrationResult> migrate_v2_to_v3(
    MigrationResult && v2_input,
    arrow::MemoryPool * pool);
POD5_FORMAT_EXPORT arrow::Result<MigrationResult> migrate_v3_to_v4(
    MigrationResult && v3_input,
    arrow::MemoryPool * pool);
POD5_FORMAT_EXPORT arrow::Result<MigrationResult> migrate_v4_to_v5(
    MigrationResult && v4_input,
    arrow::MemoryPool * pool);

/*
Perform physical file version migration to the minimum required on-disk version (schema)
which is supported by the API. Subsequent schema migrations can be applied virtually,
by allocating default value columns to avoid performing expensive physical migration.
*/
inline arrow::Result<MigrationResult> migrate_to_minimum(
    Version writer_version,
    combined_file_utils::ParsedFooter const & read_footer,
    arrow::MemoryPool * pool)
{
    MigrationResult result{read_footer};

    if (writer_version < kPod5VersionReadTableV1) {
        // V1 Added fields for read scaling
        ARROW_ASSIGN_OR_RAISE(result, migrate_v0_to_v1(std::move(result), pool));
    }

    if (writer_version < kPod5VersionReadTableV2) {
        // V2 Added num samples field
        ARROW_ASSIGN_OR_RAISE(result, migrate_v1_to_v2(std::move(result), pool));
    }

    // V3 (0.0.35) and migration threshold (<0.0.38) differ for backwards compatibility
    // because the written file version at the time was incorrect.
    if (writer_version < kPod5VersionReadTableV3MigrationThreshold) {
        // V3 Flattening fields
        ARROW_ASSIGN_OR_RAISE(result, migrate_v2_to_v3(std::move(result), pool));
    }
    return result;
}

/*
Perform physical file migration to the latest read table schema version
*/
inline arrow::Result<MigrationResult> migrate_to_latest(
    MigrationResult && input,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(auto result, migrate_v3_to_v4(std::move(input), pool));
    return migrate_v4_to_v5(std::move(result), pool);
}

/*
Perform physical file migration to the latest read table schema version
*/
inline arrow::Result<MigrationResult> migrate_to_latest(
    Version writer_version,
    combined_file_utils::ParsedFooter const & read_footer,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(auto result, migrate_to_minimum(writer_version, read_footer, pool));
    return migrate_to_latest(std::move(result), pool);
}

}  // namespace pod5
