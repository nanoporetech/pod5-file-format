#include "pod5_format/file_updater.h"

#include "pod5_format/file_reader.h"
#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/migration/migration.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/uuid.h"

#include <arrow/io/file.h>

namespace pod5 {

namespace {

FileLocation file_location_from_parsed_file_info(
    combined_file_utils::ParsedFileInfo const & file_info)
{
    return FileLocation{
        file_info.file_path,
        static_cast<std::size_t>(file_info.file_start_offset),
        static_cast<std::size_t>(file_info.file_length)};
}

}  // namespace

pod5::Status update_file(
    arrow::MemoryPool * pool,
    std::shared_ptr<FileReader> const & source,
    std::string destination)
{
    ARROW_ASSIGN_OR_RAISE(auto main_file, arrow::io::FileOutputStream::Open(destination, false));

    std::random_device gen;
    auto uuid_gen = BasicUuidRandomGenerator<std::random_device>{gen};
    auto const section_marker = uuid_gen();

    ARROW_ASSIGN_OR_RAISE(
        auto migration_result, migrate_to_latest(MigrationResult{source->parsed_footer()}, pool));

    // Write the initial header to the combined file:
    ARROW_RETURN_NOT_OK(combined_file_utils::write_combined_header(main_file, section_marker));

    ARROW_ASSIGN_OR_RAISE(
        auto signal_info_table,
        combined_file_utils::write_file_and_marker(
            pool,
            main_file,
            source->signal_table_location(),
            combined_file_utils::SubFileCleanup::LeaveOrignalFile,
            section_marker));
    ARROW_ASSIGN_OR_RAISE(
        auto run_info_info_table,
        combined_file_utils::write_file_and_marker(
            pool,
            main_file,
            source->run_info_table_location(),
            combined_file_utils::SubFileCleanup::LeaveOrignalFile,
            section_marker));
    ARROW_ASSIGN_OR_RAISE(
        auto reads_info_table,
        combined_file_utils::write_file_and_marker(
            pool,
            main_file,
            file_location_from_parsed_file_info(migration_result.footer().reads_table),
            combined_file_utils::SubFileCleanup::LeaveOrignalFile,
            section_marker));

    auto metadata = source->logical_schema_metadata();
    // Write full file footer:
    ARROW_RETURN_NOT_OK(
        combined_file_utils::write_footer(
            main_file,
            section_marker,
            metadata.file_identifier,
            metadata.writing_software,
            signal_info_table,
            run_info_info_table,
            reads_info_table));

    return main_file->Close();
}

}  // namespace pod5
