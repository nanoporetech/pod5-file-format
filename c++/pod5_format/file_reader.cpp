#include "pod5_format/file_reader.h"

#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/migration/migration.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/run_info_table_reader.h"
#include "pod5_format/signal_table_reader.h"

#include <arrow/io/concurrency.h>
#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <boost/uuid/uuid_io.hpp>

namespace pod5 {

FileReaderOptions::FileReaderOptions()
: m_memory_pool(arrow::default_memory_pool())
, m_max_cached_signal_table_batches(DEFAULT_MAX_CACHED_SIGNAL_TABLE_BATCHES)
{
}

void FileReaderOptions::set_max_cached_signal_table_batches(
    std::size_t max_cached_signal_table_batches)
{
    m_max_cached_signal_table_batches = max_cached_signal_table_batches;
}

inline FileLocation make_file_locaton(combined_file_utils::ParsedFileInfo const & parsed_file_info)
{
    return FileLocation{
        parsed_file_info.file_path,
        std::size_t(parsed_file_info.file_start_offset),
        std::size_t(parsed_file_info.file_length)};
}

class FileReaderImpl : public FileReader {
public:
    FileReaderImpl(
        Version const & file_version_pre_migration,
        MigrationResult && migration_result,
        RunInfoTableReader && run_info_table_reader,
        ReadTableReader && read_table_reader,
        SignalTableReader && signal_table_reader)
    : m_file_version_pre_migration(file_version_pre_migration)
    , m_migration_result(std::move(migration_result))
    , m_run_info_table_location(make_file_locaton(m_migration_result.footer().run_info_table))
    , m_read_table_location(make_file_locaton(m_migration_result.footer().reads_table))
    , m_signal_table_location(make_file_locaton(m_migration_result.footer().signal_table))
    , m_run_info_table_reader(std::move(run_info_table_reader))
    , m_read_table_reader(std::move(read_table_reader))
    , m_signal_table_reader(std::move(signal_table_reader))
    {
    }

    SchemaMetadataDescription schema_metadata() const override
    {
        return m_read_table_reader.schema_metadata();
    }

    virtual Result<std::size_t> read_count() const override
    {
        auto const batch_count = num_read_record_batches();
        if (batch_count == 0) {
            return 0;
        }

        ARROW_ASSIGN_OR_RAISE(auto const first_batch, read_read_record_batch(0));
        ARROW_ASSIGN_OR_RAISE(auto const last_batch, read_read_record_batch(batch_count - 1));

        return (batch_count - 1) * first_batch.num_rows() + last_batch.num_rows();
    }

    Result<ReadTableRecordBatch> read_read_record_batch(std::size_t i) const override
    {
        return m_read_table_reader.read_record_batch(i);
    }

    std::size_t num_read_record_batches() const override
    {
        return m_read_table_reader.num_record_batches();
    }

    Result<std::size_t> search_for_read_ids(
        ReadIdSearchInput const & search_input,
        gsl::span<uint32_t> const & batch_counts,
        gsl::span<uint32_t> const & batch_rows) override
    {
        return m_read_table_reader.search_for_read_ids(search_input, batch_counts, batch_rows);
    }

    Result<SignalTableRecordBatch> read_signal_record_batch(std::size_t i) const override
    {
        return m_signal_table_reader.read_record_batch(i);
    }

    std::size_t num_signal_record_batches() const override
    {
        return m_signal_table_reader.num_record_batches();
    }

    Result<std::size_t> signal_batch_for_row_id(std::size_t row, std::size_t * batch_row)
        const override
    {
        return m_signal_table_reader.signal_batch_for_row_id(row, batch_row);
    }

    Result<std::size_t> extract_sample_count(
        gsl::span<std::uint64_t const> const & row_indices) const override
    {
        return m_signal_table_reader.extract_sample_count(row_indices);
    }

    Status extract_samples(
        gsl::span<std::uint64_t const> const & row_indices,
        gsl::span<std::int16_t> const & output_samples) const override
    {
        return m_signal_table_reader.extract_samples(row_indices, output_samples);
    }

    Result<std::vector<std::shared_ptr<arrow::Buffer>>> extract_samples_inplace(
        gsl::span<std::uint64_t const> const & row_indices,
        std::vector<std::uint32_t> & sample_count) const override
    {
        return m_signal_table_reader.extract_samples_inplace(row_indices, sample_count);
    }

    FileLocation const & run_info_table_location() const override
    {
        return m_run_info_table_location;
    }

    FileLocation const & read_table_location() const override { return m_read_table_location; }

    FileLocation const & signal_table_location() const override { return m_signal_table_location; }

    Version file_version_pre_migration() const override { return m_file_version_pre_migration; }

    SignalType signal_type() const override { return m_signal_table_reader.signal_type(); }

    Result<std::shared_ptr<RunInfoData const>> find_run_info(
        std::string const & acquisition_id) const override
    {
        return m_run_info_table_reader.find_run_info(acquisition_id);
    }

    Result<std::shared_ptr<RunInfoData const>> get_run_info(std::size_t index) const override
    {
        return m_run_info_table_reader.get_run_info(index);
    }

    Result<std::size_t> get_run_info_count() const override
    {
        return m_run_info_table_reader.get_run_info_count();
    }

private:
    Version m_file_version_pre_migration;
    MigrationResult m_migration_result;
    FileLocation m_run_info_table_location;
    FileLocation m_read_table_location;
    FileLocation m_signal_table_location;
    RunInfoTableReader m_run_info_table_reader;
    ReadTableReader m_read_table_reader;
    SignalTableReader m_signal_table_reader;
};

pod5::Result<std::shared_ptr<FileReader>> open_file_reader(
    std::string const & path,
    FileReaderOptions const & options)
{
    auto pool = options.memory_pool();
    if (!pool) {
        return Status::Invalid("Invalid memory pool specified for file writer");
    }

    std::shared_ptr<arrow::io::RandomAccessFile> file;
    if (!options.force_disable_file_mapping() && getenv("POD5_DISABLE_MMAP_OPEN") == nullptr) {
        // Try to open the file with mmap, if we fail fall back to a traditional open.
        auto file_opt = arrow::io::MemoryMappedFile::Open(path, arrow::io::FileMode::READ);
        if (file_opt.ok()) {
            file = *file_opt;
        }
    }

    if (!file) {
        ARROW_ASSIGN_OR_RAISE(auto file_reader, arrow::io::ReadableFile::Open(path, pool));
        file = file_reader;
    }

    ARROW_ASSIGN_OR_RAISE(
        auto original_footer_metadata, combined_file_utils::read_footer(path, file));

    ARROW_ASSIGN_OR_RAISE(
        auto const original_writer_version,
        parse_version_number(original_footer_metadata.writer_pod5_version));
    ARROW_ASSIGN_OR_RAISE(
        auto migration_result,
        migrate_if_required(original_writer_version, original_footer_metadata, file, pool));

    // Files are written standalone, and so needs to be treated with a file offset - it wants to seek around as if the reads file is standalone:

    ARROW_ASSIGN_OR_RAISE(
        auto run_info_sub_file, open_sub_file(migration_result.footer().run_info_table));
    ARROW_ASSIGN_OR_RAISE(
        auto run_info_table_reader, make_run_info_table_reader(run_info_sub_file, pool));

    ARROW_ASSIGN_OR_RAISE(
        auto reads_sub_file, open_sub_file(migration_result.footer().reads_table));
    ARROW_ASSIGN_OR_RAISE(auto read_table_reader, make_read_table_reader(reads_sub_file, pool));

    ARROW_ASSIGN_OR_RAISE(
        auto signal_sub_file, open_sub_file(migration_result.footer().signal_table));
    ARROW_ASSIGN_OR_RAISE(
        auto signal_table_reader,
        make_signal_table_reader(signal_sub_file, options.max_cached_signal_table_batches(), pool));

    auto signal_metadata = signal_table_reader.schema_metadata();
    auto reads_metadata = read_table_reader.schema_metadata();
    if (signal_metadata.file_identifier != reads_metadata.file_identifier) {
        return Status::Invalid(
            "Invalid read and signal file pair signal identifier: ",
            signal_metadata.file_identifier,
            ", reads identifier: ",
            reads_metadata.file_identifier);
    }

    return std::make_shared<FileReaderImpl>(
        original_writer_version,
        std::move(migration_result),
        std::move(run_info_table_reader),
        std::move(read_table_reader),
        std::move(signal_table_reader));
}

}  // namespace pod5
