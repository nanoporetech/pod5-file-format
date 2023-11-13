#include "pod5_format/file_writer.h"

#include "pod5_format/file_recovery.h"
#include "pod5_format/internal/async_output_stream.h"
#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/read_table_writer.h"
#include "pod5_format/read_table_writer_utils.h"
#include "pod5_format/run_info_table_writer.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/signal_table_writer.h"
#include "pod5_format/thread_pool.h"
#include "pod5_format/version.h"

#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/util/future.h>
#include <arrow/util/key_value_metadata.h>
#include <boost/optional/optional.hpp>
#include <boost/uuid/random_generator.hpp>

#include <iostream>

#ifdef __linux__
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {
/// Open a file using the specified path and return it
std::shared_ptr<arrow::io::OutputStream>
Open(std::string const & path, bool append, bool use_directio = true)
{
#ifdef __linux__
    auto flags = use_directio ? O_RDWR | O_DIRECT : O_RDWR;

    flags |= (append == true ? O_APPEND : O_CREAT);

    int fd = open(path.c_str(), flags, 0644);

    if (fd < 0) {
        // add logging
        throw std::runtime_error{"Failed to open file"};
    }

    auto res = arrow::io::FileOutputStream::Open(fd);

    if (!res.ok()) {
        throw std::runtime_error{"Failed to open arrow file"};
    }

    return res.ValueUnsafe();
#else
    auto res = arrow::io::FileOutputStream::Open(path, append);
    return res.ValueUnsafe();
#endif
}

std::shared_ptr<arrow::io::OutputStream> makeAsyncStream(
    std::shared_ptr<arrow::io::OutputStream> const & io_stream,
    std::shared_ptr<pod5::ThreadPool> thread_pool,
    bool use_directio = true)
{
#ifdef __linux__
    if (use_directio) {
        return std::make_shared<pod5::AsyncOutputStreamDirectIO>(io_stream, thread_pool);
    } else {
        return std::make_shared<pod5::AsyncOutputStream>(io_stream, thread_pool);
    }
#else
    return std::make_shared<pod5::AsyncOutputStream>(io_stream, thread_pool);
#endif
}
}  // namespace

namespace pod5 {

FileWriterOptions::FileWriterOptions()
: m_max_signal_chunk_size(DEFAULT_SIGNAL_CHUNK_SIZE)
, m_memory_pool(arrow::default_memory_pool())
, m_signal_type(DEFAULT_SIGNAL_TYPE)
, m_signal_table_batch_size(DEFAULT_SIGNAL_TABLE_BATCH_SIZE)
, m_read_table_batch_size(DEFAULT_READ_TABLE_BATCH_SIZE)
, m_run_info_table_batch_size(DEFAULT_RUN_INFO_TABLE_BATCH_SIZE)
, m_use_directio{DEFAULT_USE_DIRECTIO}
{
}

class FileWriterImpl {
public:
    class WriterTypeImpl;

    struct DictionaryWriters {
        std::shared_ptr<EndReasonWriter> end_reason_writer;
        std::shared_ptr<PoreWriter> pore_writer;
        std::shared_ptr<RunInfoWriter> run_info_writer;
    };

    FileWriterImpl(
        DictionaryWriters && read_table_dict_writers,
        RunInfoTableWriter && run_info_table_writer,
        ReadTableWriter && read_table_writer,
        SignalTableWriter && signal_table_writer,
        std::uint32_t signal_chunk_size,
        arrow::MemoryPool * pool)
    : m_read_table_dict_writers(std::move(read_table_dict_writers))
    , m_run_info_table_writer(std::move(run_info_table_writer))
    , m_read_table_writer(std::move(read_table_writer))
    , m_signal_table_writer(std::move(signal_table_writer))
    , m_signal_chunk_size(signal_chunk_size)
    , m_pool(pool)
    {
    }

    virtual ~FileWriterImpl() = default;

    virtual std::string path() const = 0;

    pod5::Result<EndReasonDictionaryIndex> lookup_end_reason(ReadEndReason end_reason)
    {
        return m_read_table_dict_writers.end_reason_writer->lookup(end_reason);
    }

    pod5::Result<PoreDictionaryIndex> add_pore_type(std::string const & pore_type_data)
    {
        return m_read_table_dict_writers.pore_writer->add(pore_type_data);
    }

    pod5::Result<RunInfoDictionaryIndex> add_run_info(RunInfoData const & run_info_data)
    {
        ARROW_RETURN_NOT_OK(m_run_info_table_writer->add_run_info(run_info_data));
        return m_read_table_dict_writers.run_info_writer->add(run_info_data.acquisition_id);
    }

    pod5::Status add_complete_read(
        ReadData const & read_data,
        gsl::span<std::int16_t const> const & signal)
    {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        ARROW_RETURN_NOT_OK(check_read(read_data));

        ARROW_ASSIGN_OR_RAISE(
            std::vector<std::uint64_t> signal_rows, add_signal(read_data.read_id, signal));

        // Write read data and signal row entries:
        auto read_table_row = m_read_table_writer->add_read(
            read_data, gsl::make_span(signal_rows.data(), signal_rows.size()), signal.size());
        return read_table_row.status();
    }

    pod5::Status add_complete_read(
        ReadData const & read_data,
        gsl::span<std::uint64_t const> const & signal_rows,
        std::uint64_t signal_duration)
    {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        ARROW_RETURN_NOT_OK(check_read(read_data));

        // Write read data and signal row entries:
        auto read_table_row =
            m_read_table_writer->add_read(read_data, signal_rows, signal_duration);
        return read_table_row.status();
    }

    arrow::Status check_read(ReadData const & read_data)
    {
        if (!m_read_table_dict_writers.run_info_writer->is_valid(read_data.run_info)) {
            return arrow::Status::Invalid("Invalid run info passed to add_read");
        }

        if (!m_read_table_dict_writers.pore_writer->is_valid(read_data.pore_type)) {
            return arrow::Status::Invalid("Invalid pore type passed to add_read");
        }

        if (!m_read_table_dict_writers.end_reason_writer->is_valid(read_data.end_reason)) {
            return arrow::Status::Invalid("Invalid end reason passed to add_read");
        }

        return arrow::Status::OK();
    }

    pod5::Result<std::vector<SignalTableRowIndex>> add_signal(
        boost::uuids::uuid const & read_id,
        gsl::span<std::int16_t const> const & signal)
    {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        std::vector<SignalTableRowIndex> signal_rows;
        signal_rows.reserve((signal.size() / m_signal_chunk_size) + 1);

        // Chunk and write each piece of signal to the file:
        for (std::size_t chunk_start = 0; chunk_start < signal.size();
             chunk_start += m_signal_chunk_size) {
            std::size_t chunk_size =
                std::min<std::size_t>(signal.size() - chunk_start, m_signal_chunk_size);

            auto const chunk_span = signal.subspan(chunk_start, chunk_size);

            ARROW_ASSIGN_OR_RAISE(
                auto row_index, m_signal_table_writer->add_signal(read_id, chunk_span));
            signal_rows.push_back(row_index);
        }
        return signal_rows;
    }

    pod5::Result<SignalTableRowIndex> add_pre_compressed_signal(
        boost::uuids::uuid const & read_id,
        gsl::span<std::uint8_t const> const & signal_bytes,
        std::uint32_t sample_count)
    {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        return m_signal_table_writer->add_pre_compressed_signal(
            read_id, signal_bytes, sample_count);
    }

    pod5::Result<std::pair<SignalTableRowIndex, SignalTableRowIndex>> add_signal_batch(
        std::size_t row_count,
        std::vector<std::shared_ptr<arrow::Array>> && columns,
        bool final_batch)
    {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        return m_signal_table_writer->add_signal_batch(row_count, std::move(columns), final_batch);
    }

    SignalType signal_type() const { return m_signal_table_writer->signal_type(); }

    std::size_t signal_table_batch_size() const
    {
        return m_signal_table_writer->table_batch_size();
    }

    pod5::Status close_run_info_table_writer()
    {
        if (m_run_info_table_writer) {
            ARROW_RETURN_NOT_OK(m_run_info_table_writer->close());
            m_run_info_table_writer = boost::none;
        }
        return pod5::Status::OK();
    }

    pod5::Status close_read_table_writer()
    {
        if (m_read_table_writer) {
            ARROW_RETURN_NOT_OK(m_read_table_writer->close());
            m_read_table_writer = boost::none;
        }
        return pod5::Status::OK();
    }

    pod5::Status close_signal_table_writer()
    {
        if (m_signal_table_writer) {
            ARROW_RETURN_NOT_OK(m_signal_table_writer->close());
            m_signal_table_writer = boost::none;
        }
        return pod5::Status::OK();
    }

    virtual arrow::Status close() = 0;

    bool is_closed() const
    {
        assert(!!m_read_table_writer == !!m_signal_table_writer);
        return !m_signal_table_writer;
    }

    arrow::MemoryPool * pool() const { return m_pool; }

    RunInfoTableWriter * run_info_table_writer()
    {
        if (is_closed()) {
            return nullptr;
        }
        return m_run_info_table_writer.get_ptr();
    }

    ReadTableWriter * read_table_writer()
    {
        if (is_closed()) {
            return nullptr;
        }
        return m_read_table_writer.get_ptr();
    }

    SignalTableWriter * signal_table_writer()
    {
        if (is_closed()) {
            return nullptr;
        }
        return m_signal_table_writer.get_ptr();
    }

private:
    DictionaryWriters m_read_table_dict_writers;
    boost::optional<RunInfoTableWriter> m_run_info_table_writer;
    boost::optional<ReadTableWriter> m_read_table_writer;
    boost::optional<SignalTableWriter> m_signal_table_writer;
    std::uint32_t m_signal_chunk_size;
    arrow::MemoryPool * m_pool;
};

class CombinedFileWriterImpl : public FileWriterImpl {
public:
    CombinedFileWriterImpl(
        std::string const & path,
        std::string const & run_info_tmp_path,
        std::string const & reads_tmp_path,
        std::int64_t signal_file_start_offset,
        boost::uuids::uuid const & section_marker,
        boost::uuids::uuid const & file_identifier,
        std::string const & software_name,
        DictionaryWriters && dict_writers,
        RunInfoTableWriter && run_info_table_writer,
        ReadTableWriter && read_table_writer,
        SignalTableWriter && signal_table_writer,
        std::uint32_t signal_chunk_size,
        arrow::MemoryPool * pool)
    : FileWriterImpl(
        std::move(dict_writers),
        std::move(run_info_table_writer),
        std::move(read_table_writer),
        std::move(signal_table_writer),
        signal_chunk_size,
        pool)
    , m_path(path)
    , m_run_info_tmp_path(run_info_tmp_path)
    , m_reads_tmp_path(reads_tmp_path)
    , m_signal_file_start_offset(signal_file_start_offset)
    , m_section_marker(section_marker)
    , m_file_identifier(file_identifier)
    , m_software_name(software_name)
    {
    }

    std::string path() const override { return m_path; }

    arrow::Status close() override
    {
        if (is_closed()) {
            return arrow::Status::OK();
        }
        ARROW_RETURN_NOT_OK(close_run_info_table_writer());
        ARROW_RETURN_NOT_OK(close_read_table_writer());
        ARROW_RETURN_NOT_OK(close_signal_table_writer());

        // Open main path with append set:
        ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::FileOutputStream::Open(m_path, true));

        // Record signal table length:
        combined_file_utils::FileInfo signal_table;
        signal_table.file_start_offset = m_signal_file_start_offset;
        ARROW_ASSIGN_OR_RAISE(signal_table.file_length, file->Tell());
        signal_table.file_length -= signal_table.file_start_offset;

        // pad file to 8 bytes and mark section:
        ARROW_RETURN_NOT_OK(combined_file_utils::pad_file(file, 8));
        ARROW_RETURN_NOT_OK(combined_file_utils::write_section_marker(file, m_section_marker));

        auto file_location_for_full_file =
            [&](std::string const & filename) -> arrow::Result<FileLocation> {
            ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(filename, pool()));
            ARROW_ASSIGN_OR_RAISE(auto size, file->GetSize());
            return FileLocation{filename, 0, std::size_t(size)};
        };

        // Write in run_info table:
        ARROW_ASSIGN_OR_RAISE(
            auto run_info_location, file_location_for_full_file(m_run_info_tmp_path));
        ARROW_ASSIGN_OR_RAISE(
            auto run_info_info_table,
            combined_file_utils::write_file_and_marker(
                pool(),
                file,
                run_info_location,
                combined_file_utils::SubFileCleanup::CleanupOriginalFile,
                m_section_marker));

        // Write in read table:
        ARROW_ASSIGN_OR_RAISE(auto reads_location, file_location_for_full_file(m_reads_tmp_path));
        ARROW_ASSIGN_OR_RAISE(
            auto reads_info_table,
            combined_file_utils::write_file_and_marker(
                pool(),
                file,
                reads_location,
                combined_file_utils::SubFileCleanup::CleanupOriginalFile,
                m_section_marker));

        // Write full file footer:
        ARROW_RETURN_NOT_OK(combined_file_utils::write_footer(
            file,
            m_section_marker,
            m_file_identifier,
            m_software_name,
            signal_table,
            run_info_info_table,
            reads_info_table));
        return arrow::Status::OK();
    }

private:
    std::string m_path;
    std::string m_run_info_tmp_path;
    std::string m_reads_tmp_path;
    std::int64_t m_signal_file_start_offset;
    boost::uuids::uuid m_section_marker;
    boost::uuids::uuid m_file_identifier;
    std::string m_software_name;
};

FileWriter::FileWriter(std::unique_ptr<FileWriterImpl> && impl) : m_impl(std::move(impl)) {}

FileWriter::~FileWriter() { (void)close(); }

std::string FileWriter::path() const { return m_impl->path(); }

arrow::Status FileWriter::close() { return m_impl->close(); }

arrow::Status FileWriter::add_complete_read(
    ReadData const & read_data,
    gsl::span<std::int16_t const> const & signal)
{
    return m_impl->add_complete_read(read_data, signal);
}

arrow::Status FileWriter::add_complete_read(
    ReadData const & read_data,
    gsl::span<std::uint64_t const> const & signal_rows,
    std::uint64_t signal_duration)
{
    return m_impl->add_complete_read(read_data, signal_rows, signal_duration);
}

pod5::Result<std::vector<SignalTableRowIndex>> FileWriter::add_signal(
    boost::uuids::uuid const & read_id,
    gsl::span<std::int16_t const> const & signal)
{
    return m_impl->add_signal(read_id, signal);
}

pod5::Result<SignalTableRowIndex> FileWriter::add_pre_compressed_signal(
    boost::uuids::uuid const & read_id,
    gsl::span<std::uint8_t const> const & signal_bytes,
    std::uint32_t sample_count)
{
    return m_impl->add_pre_compressed_signal(read_id, signal_bytes, sample_count);
}

pod5::Result<std::pair<SignalTableRowIndex, SignalTableRowIndex>> FileWriter::add_signal_batch(
    std::size_t row_count,
    std::vector<std::shared_ptr<arrow::Array>> && columns,
    bool final_batch)
{
    return m_impl->add_signal_batch(row_count, std::move(columns), final_batch);
}

pod5::Result<EndReasonDictionaryIndex> FileWriter::lookup_end_reason(ReadEndReason end_reason) const
{
    return m_impl->lookup_end_reason(end_reason);
}

pod5::Result<PoreDictionaryIndex> FileWriter::add_pore_type(std::string const & pore_type_data)
{
    return m_impl->add_pore_type(pore_type_data);
}

pod5::Result<RunInfoDictionaryIndex> FileWriter::add_run_info(RunInfoData const & run_info_data)
{
    return m_impl->add_run_info(run_info_data);
}

SignalType FileWriter::signal_type() const { return m_impl->signal_type(); }

std::size_t FileWriter::signal_table_batch_size() const
{
    return m_impl->signal_table_batch_size();
}

pod5::Result<FileWriterImpl::DictionaryWriters> make_dictionary_writers(arrow::MemoryPool * pool)
{
    FileWriterImpl::DictionaryWriters writers;

    ARROW_ASSIGN_OR_RAISE(writers.end_reason_writer, pod5::make_end_reason_writer(pool));
    ARROW_ASSIGN_OR_RAISE(writers.pore_writer, pod5::make_pore_writer(pool));
    ARROW_ASSIGN_OR_RAISE(writers.run_info_writer, pod5::make_run_info_writer(pool));

    return writers;
}

std::string make_reads_tmp_path(
    ::arrow::internal::PlatformFilename const & arrow_path,
    boost::uuids::uuid const & file_identifier)
{
    return arrow_path.Parent().ToString() + "/"
           + ("." + boost::uuids::to_string(file_identifier) + ".tmp-reads");
}

std::string make_run_info_tmp_path(
    ::arrow::internal::PlatformFilename const & arrow_path,
    boost::uuids::uuid const & file_identifier)
{
    return arrow_path.Parent().ToString() + "/"
           + ("." + boost::uuids::to_string(file_identifier) + ".tmp-run-info");
}

pod5::Result<std::unique_ptr<FileWriter>> create_file_writer(
    std::string const & path,
    std::string const & writing_software_name,
    FileWriterOptions const & options)
{
    auto pool = options.memory_pool();
    if (!pool) {
        return Status::Invalid("Invalid memory pool specified for file writer");
    }

    auto thread_pool = options.thread_pool();
    if (!thread_pool) {
        thread_pool = make_thread_pool(1);
    }

    ARROW_ASSIGN_OR_RAISE(auto arrow_path, ::arrow::internal::PlatformFilename::FromString(path));
    ARROW_ASSIGN_OR_RAISE(bool file_exists, arrow::internal::FileExists(arrow_path));
    if (file_exists) {
        return Status::Invalid("Unable to create new file '", path, "', already exists");
    }

    // Open dictionary writers:
    ARROW_ASSIGN_OR_RAISE(auto dict_writers, make_dictionary_writers(pool));

    // Prep file metadata:
    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto const section_marker = uuid_gen();
    auto const file_identifier = uuid_gen();

    ARROW_ASSIGN_OR_RAISE(auto current_version, parse_version_number(Pod5Version));
    ARROW_ASSIGN_OR_RAISE(
        auto file_schema_metadata,
        make_schema_key_value_metadata({file_identifier, writing_software_name, current_version}));

    auto reads_tmp_path = make_reads_tmp_path(arrow_path, file_identifier);
    auto run_info_tmp_path = make_run_info_tmp_path(arrow_path, file_identifier);

    bool const use_directio = options.use_directio();

    // Prepare the temporary reads file:
    auto read_table_file_async =
        ::makeAsyncStream(::Open(reads_tmp_path, false, use_directio), thread_pool, use_directio);
    ARROW_ASSIGN_OR_RAISE(
        auto read_table_tmp_writer,
        make_read_table_writer(
            read_table_file_async,
            file_schema_metadata,
            options.read_table_batch_size(),
            dict_writers.pore_writer,
            dict_writers.end_reason_writer,
            dict_writers.run_info_writer,
            pool));

    // Prepare the temporary run_info file:
    auto run_info_table_file_async = ::makeAsyncStream(
        ::Open(run_info_tmp_path, false, use_directio), thread_pool, use_directio);

    ARROW_ASSIGN_OR_RAISE(
        auto run_info_table_tmp_writer,
        make_run_info_table_writer(
            run_info_table_file_async,
            file_schema_metadata,
            options.run_info_table_batch_size(),
            pool));

    // Prepare the main file - and set up the signal table to write here:
    auto signal_file =
        ::makeAsyncStream(::Open(path, false, use_directio), thread_pool, use_directio);

    // Write the initial header to the combined file:
    ARROW_RETURN_NOT_OK(combined_file_utils::write_combined_header(signal_file, section_marker));

    ARROW_ASSIGN_OR_RAISE(size_t const signal_table_start, signal_file->Tell());

    static_cast<AsyncOutputStream *>(signal_file.get())->set_file_start_offset(signal_table_start);

    // Then place the signal file directly after that:
    ARROW_ASSIGN_OR_RAISE(
        auto signal_table_writer,
        make_signal_table_writer(
            signal_file,
            file_schema_metadata,
            options.signal_table_batch_size(),
            options.signal_type(),
            pool));

    // Throw it all together into a writer object:
    return std::make_unique<FileWriter>(std::make_unique<CombinedFileWriterImpl>(
        path,
        run_info_tmp_path,
        reads_tmp_path,
        signal_table_start,
        section_marker,
        file_identifier,
        writing_software_name,
        std::move(dict_writers),
        std::move(run_info_table_tmp_writer),
        std::move(read_table_tmp_writer),
        std::move(signal_table_writer),
        options.max_signal_chunk_size(),
        pool));
}

pod5::Result<std::unique_ptr<FileWriter>> recover_file_writer(
    std::string const & src_path,
    std::string const & dest_path,
    FileWriterOptions const & options)
{
    // Create a file to push recovered data into:
    ARROW_ASSIGN_OR_RAISE(
        auto dest_file, create_file_writer(dest_path, "pod5_file_recovery", options));

    auto pool = arrow::default_memory_pool();
    ARROW_ASSIGN_OR_RAISE(
        auto arrow_path, ::arrow::internal::PlatformFilename::FromString(src_path));
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(src_path, pool));

    // Signature should be right at 0:
    ARROW_RETURN_NOT_OK(combined_file_utils::check_signature(file, 0));

    auto null_metadata = arrow::KeyValueMetadata::Make({}, {});

    // Recover the signal data into [dest_file]:
    RecoveredData recovered_raw_data;
    {
        ARROW_ASSIGN_OR_RAISE(
            auto raw_sub_file,
            combined_file_utils::open_sub_file(file, combined_file_utils::header_size));
        ARROW_ASSIGN_OR_RAISE(
            recovered_raw_data,
            recover_arrow_file(raw_sub_file, dest_file->impl()->signal_table_writer()));
    }

    auto file_identifier = recovered_raw_data.metadata.file_identifier;
    auto reads_tmp_path = make_reads_tmp_path(arrow_path, file_identifier);
    auto run_info_tmp_path = make_run_info_tmp_path(arrow_path, file_identifier);

    // Recover the run info data into [dest_file]:
    {
        ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(run_info_tmp_path, pool));
        ARROW_ASSIGN_OR_RAISE(auto size, file->GetSize());
        if (size > 0) {
            ARROW_ASSIGN_OR_RAISE(
                recovered_raw_data,
                recover_arrow_file(file, dest_file->impl()->run_info_table_writer()));
        }
    }

    // Recover the read data into [dest_file]:
    {
        ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(reads_tmp_path, pool));
        ARROW_ASSIGN_OR_RAISE(auto size, file->GetSize());
        if (size > 0) {
            ARROW_ASSIGN_OR_RAISE(
                recovered_raw_data,
                recover_arrow_file(file, dest_file->impl()->read_table_writer()));
        }
    }

    return dest_file;
}

}  // namespace pod5
