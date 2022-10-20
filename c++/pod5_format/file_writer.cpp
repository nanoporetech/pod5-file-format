#include "pod5_format/file_writer.h"

#include "pod5_format/internal/async_output_stream.h"
#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/read_table_writer.h"
#include "pod5_format/read_table_writer_utils.h"
#include "pod5_format/run_info_table_writer.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/signal_table_writer.h"
#include "pod5_format/version.h"

#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <arrow/util/future.h>
#include <arrow/util/io_util.h>
#include <boost/optional/optional.hpp>
#include <boost/uuid/random_generator.hpp>

#include <iostream>

namespace pod5 {

FileWriterOptions::FileWriterOptions()
        : m_max_signal_chunk_size(DEFAULT_SIGNAL_CHUNK_SIZE),
          m_memory_pool(arrow::default_memory_pool()),
          m_signal_type(DEFAULT_SIGNAL_TYPE),
          m_signal_table_batch_size(DEFAULT_SIGNAL_TABLE_BATCH_SIZE),
          m_read_table_batch_size(DEFAULT_READ_TABLE_BATCH_SIZE),
          m_run_info_table_batch_size(DEFAULT_RUN_INFO_TABLE_BATCH_SIZE) {}

class FileWriterImpl {
public:
    class WriterTypeImpl;
    struct DictionaryWriters {
        std::shared_ptr<EndReasonWriter> end_reason_writer;
        std::shared_ptr<PoreWriter> pore_writer;
        std::shared_ptr<RunInfoWriter> run_info_writer;
    };

    FileWriterImpl(DictionaryWriters&& read_table_dict_writers,
                   RunInfoTableWriter&& run_info_table_writer,
                   ReadTableWriter&& read_table_writer,
                   SignalTableWriter&& signal_table_writer,
                   std::uint32_t signal_chunk_size,
                   arrow::MemoryPool* pool)
            : m_read_table_dict_writers(std::move(read_table_dict_writers)),
              m_run_info_table_writer(std::move(run_info_table_writer)),
              m_read_table_writer(std::move(read_table_writer)),
              m_signal_table_writer(std::move(signal_table_writer)),
              m_signal_chunk_size(signal_chunk_size),
              m_pool(pool) {}

    virtual ~FileWriterImpl() = default;

    pod5::Result<EndReasonDictionaryIndex> lookup_end_reason(ReadEndReason end_reason) {
        return m_read_table_dict_writers.end_reason_writer->lookup(end_reason);
    }

    pod5::Result<PoreDictionaryIndex> add_pore_type(std::string const& pore_type_data) {
        return m_read_table_dict_writers.pore_writer->add(pore_type_data);
    }

    pod5::Result<RunInfoDictionaryIndex> add_run_info(RunInfoData const& run_info_data) {
        ARROW_RETURN_NOT_OK(m_run_info_table_writer->add_run_info(run_info_data));
        return m_read_table_dict_writers.run_info_writer->add(run_info_data.acquisition_id);
    }

    pod5::Status add_complete_read(ReadData const& read_data,
                                   gsl::span<std::int16_t const> const& signal) {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        ARROW_ASSIGN_OR_RAISE(std::vector<std::uint64_t> signal_rows,
                              add_signal(read_data.read_id, signal));

        // Write read data and signal row entries:
        auto read_table_row = m_read_table_writer->add_read(
                read_data, gsl::make_span(signal_rows.data(), signal_rows.size()), signal.size());
        return read_table_row.status();
    }

    pod5::Status add_complete_read(ReadData const& read_data,
                                   gsl::span<std::uint64_t const> const& signal_rows,
                                   std::uint64_t signal_duration) {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        // Write read data and signal row entries:
        auto read_table_row =
                m_read_table_writer->add_read(read_data, signal_rows, signal_duration);
        return read_table_row.status();
    }

    pod5::Result<std::vector<SignalTableRowIndex>> add_signal(
            boost::uuids::uuid const& read_id,
            gsl::span<std::int16_t const> const& signal) {
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

            ARROW_ASSIGN_OR_RAISE(auto row_index,
                                  m_signal_table_writer->add_signal(read_id, chunk_span));
            signal_rows.push_back(row_index);
        }
        return signal_rows;
    }

    pod5::Result<std::uint64_t> add_pre_compressed_signal(
            boost::uuids::uuid const& read_id,
            gsl::span<std::uint8_t const> const& signal_bytes,
            std::uint32_t sample_count) {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        return m_signal_table_writer->add_pre_compressed_signal(read_id, signal_bytes,
                                                                sample_count);
    }

    SignalType signal_type() const { return m_signal_table_writer->signal_type(); }

    pod5::Status close_run_info_table_writer() {
        if (m_run_info_table_writer) {
            ARROW_RETURN_NOT_OK(m_run_info_table_writer->close());
            m_run_info_table_writer = boost::none;
        }
        return pod5::Status::OK();
    }
    pod5::Status close_read_table_writer() {
        if (m_read_table_writer) {
            ARROW_RETURN_NOT_OK(m_read_table_writer->close());
            m_read_table_writer = boost::none;
        }
        return pod5::Status::OK();
    }
    pod5::Status close_signal_table_writer() {
        if (m_signal_table_writer) {
            ARROW_RETURN_NOT_OK(m_signal_table_writer->close());
            m_signal_table_writer = boost::none;
        }
        return pod5::Status::OK();
    }

    virtual arrow::Status close() {
        ARROW_RETURN_NOT_OK(close_read_table_writer());
        ARROW_RETURN_NOT_OK(close_signal_table_writer());
        return arrow::Status::OK();
    }

    bool is_closed() const {
        assert(!!m_read_table_writer == !!m_signal_table_writer);
        return !m_signal_table_writer;
    }

    arrow::MemoryPool* pool() const { return m_pool; }

private:
    DictionaryWriters m_read_table_dict_writers;
    boost::optional<RunInfoTableWriter> m_run_info_table_writer;
    boost::optional<ReadTableWriter> m_read_table_writer;
    boost::optional<SignalTableWriter> m_signal_table_writer;
    std::uint32_t m_signal_chunk_size;
    arrow::MemoryPool* m_pool;
};

class CombinedFileWriterImpl : public FileWriterImpl {
public:
    CombinedFileWriterImpl(std::string const& path,
                           std::string const& run_info_tmp_path,
                           std::string const& reads_tmp_path,
                           std::int64_t signal_file_start_offset,
                           boost::uuids::uuid const& section_marker,
                           boost::uuids::uuid const& file_identifier,
                           std::string const& software_name,
                           DictionaryWriters&& dict_writers,
                           RunInfoTableWriter&& run_info_table_writer,
                           ReadTableWriter&& read_table_writer,
                           SignalTableWriter&& signal_table_writer,
                           std::uint32_t signal_chunk_size,
                           arrow::MemoryPool* pool)
            : FileWriterImpl(std::move(dict_writers),
                             std::move(run_info_table_writer),
                             std::move(read_table_writer),
                             std::move(signal_table_writer),
                             signal_chunk_size,
                             pool),
              m_path(path),
              m_run_info_tmp_path(run_info_tmp_path),
              m_reads_tmp_path(reads_tmp_path),
              m_signal_file_start_offset(signal_file_start_offset),
              m_section_marker(section_marker),
              m_file_identifier(file_identifier),
              m_software_name(software_name) {}

    arrow::Status close() override {
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

        // Padd file to 8 bytes and mark section:
        ARROW_RETURN_NOT_OK(combined_file_utils::padd_file(file, 8));
        ARROW_RETURN_NOT_OK(combined_file_utils::write_section_marker(file, m_section_marker));

        // Write in run_info table:
        ARROW_ASSIGN_OR_RAISE(auto run_info_info_table, write_sub_file(file, m_run_info_tmp_path));
        // Write in read table:
        ARROW_ASSIGN_OR_RAISE(auto reads_info_table, write_sub_file(file, m_reads_tmp_path));

        // Padd file to 8 bytes and mark section:
        ARROW_RETURN_NOT_OK(combined_file_utils::padd_file(file, 8));
        ARROW_RETURN_NOT_OK(combined_file_utils::write_section_marker(file, m_section_marker));

        // Write full file footer:
        ARROW_RETURN_NOT_OK(combined_file_utils::write_footer(
                file, m_section_marker, m_file_identifier, m_software_name, signal_table,
                run_info_info_table, reads_info_table));
        return arrow::Status::OK();
    }

private:
    arrow::Result<combined_file_utils::FileInfo> write_sub_file(
            std::shared_ptr<arrow::io::FileOutputStream> const& file,
            std::string const& filename) {
        combined_file_utils::FileInfo table_data;
        // Record file start location in bytes within the main file:
        ARROW_ASSIGN_OR_RAISE(table_data.file_start_offset, file->Tell());

        {
            // Stream out the reads table into the main file:
            ARROW_ASSIGN_OR_RAISE(auto reads_table_file_in,
                                  arrow::io::ReadableFile::Open(filename, pool()));
            ARROW_ASSIGN_OR_RAISE(auto file_size, reads_table_file_in->GetSize());
            std::int64_t copied_bytes = 0;
            std::int64_t target_chunk_size = 10 * 1024 * 1024;  // Read in 10MB of data at a time
            std::vector<char> read_data(target_chunk_size);
            while (copied_bytes < file_size) {
                ARROW_ASSIGN_OR_RAISE(
                        auto const read_bytes,
                        reads_table_file_in->Read(target_chunk_size, read_data.data()));
                copied_bytes += read_bytes;
                ARROW_RETURN_NOT_OK(file->Write(read_data.data(), read_bytes));
            }

            // Store the reads file length for later reading:
            ARROW_ASSIGN_OR_RAISE(table_data.file_length, file->Tell());
            table_data.file_length -= table_data.file_start_offset;
        }

        // Clean up the tmp read path:
        ARROW_ASSIGN_OR_RAISE(auto arrow_path,
                              ::arrow::internal::PlatformFilename::FromString(filename));
        ARROW_RETURN_NOT_OK(arrow::internal::DeleteFile(arrow_path));

        return table_data;
    }

    std::string m_path;
    std::string m_run_info_tmp_path;
    std::string m_reads_tmp_path;
    std::int64_t m_signal_file_start_offset;
    boost::uuids::uuid m_section_marker;
    boost::uuids::uuid m_file_identifier;
    std::string m_software_name;
};

FileWriter::FileWriter(std::unique_ptr<FileWriterImpl>&& impl) : m_impl(std::move(impl)) {}

FileWriter::~FileWriter() { (void)close(); }

arrow::Status FileWriter::close() { return m_impl->close(); }

arrow::Status FileWriter::add_complete_read(ReadData const& read_data,
                                            gsl::span<std::int16_t const> const& signal) {
    return m_impl->add_complete_read(read_data, signal);
}

arrow::Status FileWriter::add_complete_read(ReadData const& read_data,
                                            gsl::span<std::uint64_t const> const& signal_rows,
                                            std::uint64_t signal_duration) {
    return m_impl->add_complete_read(read_data, signal_rows, signal_duration);
}

pod5::Result<std::vector<SignalTableRowIndex>> FileWriter::add_signal(
        boost::uuids::uuid const& read_id,
        gsl::span<std::int16_t const> const& signal) {
    return m_impl->add_signal(read_id, signal);
}

pod5::Result<SignalTableRowIndex> FileWriter::add_pre_compressed_signal(
        boost::uuids::uuid const& read_id,
        gsl::span<std::uint8_t const> const& signal_bytes,
        std::uint32_t sample_count) {
    return m_impl->add_pre_compressed_signal(read_id, signal_bytes, sample_count);
}

pod5::Result<EndReasonDictionaryIndex> FileWriter::lookup_end_reason(
        ReadEndReason end_reason) const {
    return m_impl->lookup_end_reason(end_reason);
}

pod5::Result<PoreDictionaryIndex> FileWriter::add_pore_type(std::string const& pore_type_data) {
    return m_impl->add_pore_type(pore_type_data);
}

pod5::Result<RunInfoDictionaryIndex> FileWriter::add_run_info(RunInfoData const& run_info_data) {
    return m_impl->add_run_info(run_info_data);
}

SignalType FileWriter::signal_type() const { return m_impl->signal_type(); }

pod5::Result<FileWriterImpl::DictionaryWriters> make_dictionary_writers(arrow::MemoryPool* pool) {
    FileWriterImpl::DictionaryWriters writers;

    ARROW_ASSIGN_OR_RAISE(writers.end_reason_writer, pod5::make_end_reason_writer(pool));
    ARROW_ASSIGN_OR_RAISE(writers.pore_writer, pod5::make_pore_writer(pool));
    ARROW_ASSIGN_OR_RAISE(writers.run_info_writer, pod5::make_run_info_writer(pool));

    return writers;
}

pod5::Result<std::unique_ptr<FileWriter>> create_split_file_writer(
        std::string const& signal_path,
        std::string const& reads_path,
        std::string const& writing_software_name,
        FileWriterOptions const& options) {
    auto pool = options.memory_pool();
    if (!pool) {
        return Status::Invalid("Invalid memory pool specified for file writer");
    }

    ARROW_ASSIGN_OR_RAISE(auto arrow_reads_path,
                          ::arrow::internal::PlatformFilename::FromString(reads_path));
    ARROW_ASSIGN_OR_RAISE(bool file_exists, arrow::internal::FileExists(arrow_reads_path));
    if (file_exists) {
        return Status::Invalid("Unable to create new file '", reads_path, "', already exists");
    }

    ARROW_ASSIGN_OR_RAISE(auto arrow_signal_path,
                          ::arrow::internal::PlatformFilename::FromString(signal_path));
    ARROW_ASSIGN_OR_RAISE(file_exists, arrow::internal::FileExists(arrow_signal_path));
    if (file_exists) {
        return Status::Invalid("Unable to create new file '", signal_path, "', already exists");
    }

    // Open dictionary writrs:
    ARROW_ASSIGN_OR_RAISE(auto dict_writers, make_dictionary_writers(pool));

    // Prep file metadata:
    auto file_identifier = boost::uuids::random_generator_mt19937()();

    ARROW_ASSIGN_OR_RAISE(auto file_schema_metadata,
                          make_schema_key_value_metadata({file_identifier, writing_software_name,
                                                          *parse_version_number(Pod5Version)}));

    // Open run info file table:
    auto run_info_path = reads_path + ".run_info";
    ARROW_ASSIGN_OR_RAISE(auto run_info_table_file,
                          arrow::io::FileOutputStream::Open(run_info_path, false));
    ARROW_ASSIGN_OR_RAISE(auto run_info_table_writer,
                          make_run_info_table_writer(run_info_table_file, file_schema_metadata,
                                                     options.read_table_batch_size(), pool));

    // Open read file table:
    ARROW_ASSIGN_OR_RAISE(auto read_table_file,
                          arrow::io::FileOutputStream::Open(reads_path, false));
    ARROW_ASSIGN_OR_RAISE(
            auto read_table_writer,
            make_read_table_writer(read_table_file, file_schema_metadata,
                                   options.read_table_batch_size(), dict_writers.pore_writer,
                                   dict_writers.end_reason_writer, dict_writers.run_info_writer,
                                   pool));

    // Open signal file table:
    ARROW_ASSIGN_OR_RAISE(auto signal_table_file,
                          arrow::io::FileOutputStream::Open(signal_path, false));
    ARROW_ASSIGN_OR_RAISE(auto signal_table_writer,
                          make_signal_table_writer(signal_table_file, file_schema_metadata,
                                                   options.signal_table_batch_size(),
                                                   options.signal_type(), pool));

    // Throw it all together into a writer object:
    return std::make_unique<FileWriter>(std::make_unique<FileWriterImpl>(
            std::move(dict_writers), std::move(run_info_table_writer), std::move(read_table_writer),
            std::move(signal_table_writer), options.max_signal_chunk_size(), pool));
}

pod5::Result<std::unique_ptr<FileWriter>> create_combined_file_writer(
        std::string const& path,
        std::string const& writing_software_name,
        FileWriterOptions const& options) {
    auto pool = options.memory_pool();
    if (!pool) {
        return Status::Invalid("Invalid memory pool specified for file writer");
    }

    ARROW_ASSIGN_OR_RAISE(auto arrow_path, ::arrow::internal::PlatformFilename::FromString(path));
    ARROW_ASSIGN_OR_RAISE(bool file_exists, arrow::internal::FileExists(arrow_path));
    if (file_exists) {
        return Status::Invalid("Unable to create new file '", path, "', already exists");
    }

    // Open dictionary writrs:
    ARROW_ASSIGN_OR_RAISE(auto dict_writers, make_dictionary_writers(pool));

    // Prep file metadata:
    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto const section_marker = uuid_gen();
    auto const file_identifier = uuid_gen();

    ARROW_ASSIGN_OR_RAISE(auto file_schema_metadata,
                          make_schema_key_value_metadata({file_identifier, writing_software_name,
                                                          *parse_version_number(Pod5Version)}));

    auto reads_tmp_path = arrow_path.Parent().ToString() + "/" +
                          ("." + boost::uuids::to_string(file_identifier) + ".tmp-reads");
    auto run_info_tmp_path = arrow_path.Parent().ToString() + "/" +
                             ("." + boost::uuids::to_string(file_identifier) + ".tmp-run-info");

    // Prepare the temporary reads file:
    ARROW_ASSIGN_OR_RAISE(auto read_table_file,
                          arrow::io::FileOutputStream::Open(reads_tmp_path, false));
    auto read_table_file_async = std::make_shared<AsyncOutputStream>(read_table_file);
    ARROW_ASSIGN_OR_RAISE(
            auto read_table_tmp_writer,
            make_read_table_writer(read_table_file_async, file_schema_metadata,
                                   options.read_table_batch_size(), dict_writers.pore_writer,
                                   dict_writers.end_reason_writer, dict_writers.run_info_writer,
                                   pool));

    // Prepare the temporary run_info file:
    ARROW_ASSIGN_OR_RAISE(auto run_info_table_file,
                          arrow::io::FileOutputStream::Open(run_info_tmp_path, false));
    auto run_info_table_file_async = std::make_shared<AsyncOutputStream>(run_info_table_file);
    ARROW_ASSIGN_OR_RAISE(
            auto run_info_table_tmp_writer,
            make_run_info_table_writer(run_info_table_file_async, file_schema_metadata,
                                       options.run_info_table_batch_size(), pool));

    // Prepare the main file - and set up the signal table to write here:
    ARROW_ASSIGN_OR_RAISE(auto main_file, arrow::io::FileOutputStream::Open(path, false));

    // Write the initial header to the combined file:
    ARROW_RETURN_NOT_OK(combined_file_utils::write_combined_header(main_file, section_marker));

    // Then place the signal file directly after that:
    ARROW_ASSIGN_OR_RAISE(auto const signal_table_start, main_file->Tell());
    auto signal_file = std::make_shared<AsyncOutputStream>(main_file);
    ARROW_ASSIGN_OR_RAISE(auto signal_table_writer,
                          make_signal_table_writer(signal_file, file_schema_metadata,
                                                   options.signal_table_batch_size(),
                                                   options.signal_type(), pool));

    // Throw it all together into a writer object:
    return std::make_unique<FileWriter>(std::make_unique<CombinedFileWriterImpl>(
            path, run_info_tmp_path, reads_tmp_path, signal_table_start, section_marker,
            file_identifier, writing_software_name, std::move(dict_writers),
            std::move(run_info_table_tmp_writer), std::move(read_table_tmp_writer),
            std::move(signal_table_writer), options.max_signal_chunk_size(), pool));
}

}  // namespace pod5