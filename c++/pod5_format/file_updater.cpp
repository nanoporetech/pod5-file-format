#include "pod5_format/file_updater.h"

#include "pod5_format/file_reader.h"
#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/schema_metadata.h"

#include <arrow/io/file.h>
#include <boost/uuid/random_generator.hpp>

namespace pod5 {

pod5::Status update_file(
    arrow::MemoryPool * pool,
    std::shared_ptr<FileReader> const & source,
    std::string destination)
{
    ARROW_ASSIGN_OR_RAISE(auto main_file, arrow::io::FileOutputStream::Open(destination, false));

    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto const section_marker = uuid_gen();

    auto metadata = source->schema_metadata();

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
            source->read_table_location(),
            combined_file_utils::SubFileCleanup::LeaveOrignalFile,
            section_marker));

    // Write full file footer:
    ARROW_RETURN_NOT_OK(combined_file_utils::write_footer(
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
