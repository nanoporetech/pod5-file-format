#include "mkr_format/file_writer.h"

#include "mkr_format/internal/combined_file_utils.h"
#include "mkr_format/read_table_writer.h"
#include "mkr_format/read_table_writer_utils.h"
#include "mkr_format/schema_metadata.h"
#include "mkr_format/signal_table_writer.h"
#include "mkr_format/version.h"

#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <boost/filesystem.hpp>
#include <boost/optional/optional.hpp>
#include <boost/uuid/random_generator.hpp>

namespace mkr {

FileWriterOptions::FileWriterOptions()
        : m_max_signal_chunk_size(DEFAULT_SIGNAL_CHUNK_SIZE),
          m_memory_pool(arrow::system_memory_pool()),
          m_signal_type(DEFAULT_SIGNAL_TYPE) {}

class FileWriterImpl {
public:
    class WriterTypeImpl;
    struct DictionaryWriters {
        std::shared_ptr<PoreWriter> pore_writer;
        std::shared_ptr<EndReasonWriter> end_reason_writer;
        std::shared_ptr<CalibrationWriter> calibration_writer;
        std::shared_ptr<RunInfoWriter> run_info_writer;
    };

    FileWriterImpl(DictionaryWriters&& dict_writers,
                   ReadTableWriter&& read_table_writer,
                   SignalTableWriter&& signal_table_writer,
                   std::uint32_t signal_chunk_size,
                   arrow::MemoryPool* pool)
            : m_dict_writers(std::move(dict_writers)),
              m_read_table_writer(std::move(read_table_writer)),
              m_signal_table_writer(std::move(signal_table_writer)),
              m_signal_chunk_size(signal_chunk_size),
              m_pool(pool) {}

    ~FileWriterImpl() = default;

    mkr::Result<PoreDictionaryIndex> add_pore(PoreData const& pore_data) {
        return m_dict_writers.pore_writer->add(pore_data);
    }

    mkr::Result<CalibrationDictionaryIndex> add_calibration(
            CalibrationData const& calibration_data) {
        return m_dict_writers.calibration_writer->add(calibration_data);
    }

    mkr::Result<EndReasonDictionaryIndex> add_end_reason(EndReasonData const& end_reason_data) {
        return m_dict_writers.end_reason_writer->add(end_reason_data);
    }

    mkr::Result<RunInfoDictionaryIndex> add_run_info(RunInfoData const& run_info_data) {
        return m_dict_writers.run_info_writer->add(run_info_data);
    }

    mkr::Status add_complete_read(ReadData const& read_data,
                                  gsl::span<std::int16_t const> const& signal) {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        arrow::TypedBufferBuilder<std::uint64_t> signal_row_builder(m_pool);

        // Chunk and write each piece of signal to the file:
        for (std::size_t chunk_start = 0; chunk_start < signal.size();
             chunk_start += m_signal_chunk_size) {
            std::size_t chunk_size =
                    std::min<std::size_t>(signal.size() - chunk_start, m_signal_chunk_size);

            auto const chunk_span = signal.subspan(chunk_start, chunk_size);

            ARROW_ASSIGN_OR_RAISE(auto row_index,
                                  m_signal_table_writer->add_signal(read_data.read_id, chunk_span));
            ARROW_RETURN_NOT_OK(signal_row_builder.Append(row_index));
        }

        // Write read data and signal row entries:
        auto read_table_row = m_read_table_writer->add_read(
                read_data, gsl::make_span(signal_row_builder.data(), signal_row_builder.length()));
        return read_table_row.status();
    }

    mkr::Status add_complete_read(ReadData const& read_data,
                                  gsl::span<std::uint64_t const> const& signal_rows) {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        // Write read data and signal row entries:
        auto read_table_row = m_read_table_writer->add_read(read_data, signal_rows);
        return read_table_row.status();
    }

    mkr::Result<std::uint64_t> add_pre_compressed_signal(
            boost::uuids::uuid const& read_id,
            gsl::span<std::uint8_t const> const& signal_bytes,
            std::uint32_t sample_count) {
        if (!m_signal_table_writer || !m_read_table_writer) {
            return arrow::Status::Invalid("File writer closed, cannot write further data");
        }

        return m_signal_table_writer->add_pre_compressed_signal(read_id, signal_bytes,
                                                                sample_count);
    }

    mkr::Status flush_signal_table() { return m_signal_table_writer->flush(); }

    mkr::Status flush_reads_table() { return m_read_table_writer->flush(); }

    void close_read_table_writer() { m_read_table_writer = boost::none; }
    void close_signal_table_writer() { m_signal_table_writer = boost::none; }

    virtual arrow::Status close() {
        close_read_table_writer();
        close_signal_table_writer();
        return arrow::Status::OK();
    }

    arrow::MemoryPool* pool() const { return m_pool; }

private:
    DictionaryWriters m_dict_writers;
    boost::optional<ReadTableWriter> m_read_table_writer;
    boost::optional<SignalTableWriter> m_signal_table_writer;
    std::uint32_t m_signal_chunk_size;
    arrow::MemoryPool* m_pool;
};

class CombinedFileWriterImpl : public FileWriterImpl {
public:
    CombinedFileWriterImpl(boost::filesystem::path const& path,
                           boost::filesystem::path const& reads_tmp_path,
                           std::int64_t signal_file_start_offset,
                           boost::uuids::uuid const& section_marker,
                           boost::uuids::uuid const& file_identifier,
                           std::string const& software_name,
                           DictionaryWriters&& dict_writers,
                           ReadTableWriter&& read_table_writer,
                           SignalTableWriter&& signal_table_writer,
                           std::uint32_t signal_chunk_size,
                           arrow::MemoryPool* pool)
            : FileWriterImpl(std::move(dict_writers),
                             std::move(read_table_writer),
                             std::move(signal_table_writer),
                             signal_chunk_size,
                             pool),
              m_path(path),
              m_reads_tmp_path(reads_tmp_path),
              m_signal_file_start_offset(signal_file_start_offset),
              m_section_marker(section_marker),
              m_file_identifier(file_identifier),
              m_software_name(software_name) {}

    arrow::Status close() override {
        close_read_table_writer();
        close_signal_table_writer();

        // Open main path with append set:
        ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::FileOutputStream::Open(m_path.string(), true));

        // Record signal table length:
        combined_file_utils::FileInfo signal_table;
        signal_table.file_start_offset = m_signal_file_start_offset;
        ARROW_ASSIGN_OR_RAISE(signal_table.file_length, file->Tell());
        signal_table.file_length -= signal_table.file_start_offset;

        // Padd file to 8 bytes and mark section:
        combined_file_utils::padd_file(file, 8);
        combined_file_utils::write_section_marker(file, m_section_marker);

        // Write in read table:
        combined_file_utils::FileInfo read_info_table;
        {
            // Record file start location in bytes within the main file:
            ARROW_ASSIGN_OR_RAISE(read_info_table.file_start_offset, file->Tell());

            // Stream out the reads table into the main file:
            ARROW_ASSIGN_OR_RAISE(auto reads_table_file_in,
                                  arrow::io::ReadableFile::Open(m_reads_tmp_path.string(), pool()));
            std::int64_t read_bytes = 0;
            std::int64_t target_chunk_size = 10 * 1024 * 1024;  // Read in 10MB of data at a time
            std::vector<char> read_data(target_chunk_size);
            do {
                ARROW_ASSIGN_OR_RAISE(
                        auto const read_bytes,
                        reads_table_file_in->Read(target_chunk_size, read_data.data()));
                ARROW_RETURN_NOT_OK(file->Write(read_data.data(), read_bytes));
            } while (read_bytes == target_chunk_size);

            // Store the reads file length for later reading:
            ARROW_ASSIGN_OR_RAISE(read_info_table.file_length, file->Tell());
            read_info_table.file_length -= read_info_table.file_start_offset;

            // Clean up the tmp read path:
            boost::system::error_code ec;
            boost::filesystem::remove(m_reads_tmp_path, ec);
            if (ec) {
                return arrow::Status::Invalid("Failed to remove temporary file");
            }
        }
        // Padd file to 8 bytes and mark section:
        ARROW_RETURN_NOT_OK(combined_file_utils::padd_file(file, 8));
        ARROW_RETURN_NOT_OK(combined_file_utils::write_section_marker(file, m_section_marker));

        // Write full file footer:
        ARROW_RETURN_NOT_OK(combined_file_utils::write_footer(file, m_section_marker,
                                                              m_file_identifier, m_software_name,
                                                              signal_table, read_info_table));

        close_signal_table_writer();
        return arrow::Status::OK();
    }

private:
    boost::filesystem::path m_path;
    boost::filesystem::path m_reads_tmp_path;
    std::int64_t m_signal_file_start_offset;
    boost::uuids::uuid m_section_marker;
    boost::uuids::uuid m_file_identifier;
    std::string m_software_name;
};

FileWriter::FileWriter(std::unique_ptr<FileWriterImpl>&& impl) : m_impl(std::move(impl)) {}

FileWriter::~FileWriter() { close(); }

arrow::Status FileWriter::close() { return m_impl->close(); }

arrow::Status FileWriter::add_complete_read(ReadData const& read_data,
                                            gsl::span<std::int16_t const> const& signal) {
    return m_impl->add_complete_read(read_data, signal);
}

arrow::Status FileWriter::add_complete_read(ReadData const& read_data,
                                            gsl::span<std::uint64_t const> const& signal_rows) {
    return m_impl->add_complete_read(read_data, signal_rows);
}

mkr::Result<SignalTableRowIndex> FileWriter::add_pre_compressed_signal(
        boost::uuids::uuid const& read_id,
        gsl::span<std::uint8_t const> const& signal_bytes,
        std::uint32_t sample_count) {
    return m_impl->add_pre_compressed_signal(read_id, signal_bytes, sample_count);
}

mkr::Status FileWriter::flush_signal_table() { return m_impl->flush_signal_table(); }

mkr::Status FileWriter::flush_reads_table() { return m_impl->flush_reads_table(); }

mkr::Result<PoreDictionaryIndex> FileWriter::add_pore(PoreData const& pore_data) {
    return m_impl->add_pore(pore_data);
}

mkr::Result<CalibrationDictionaryIndex> FileWriter::add_calibration(
        CalibrationData const& calibration_data) {
    return m_impl->add_calibration(calibration_data);
}

mkr::Result<EndReasonDictionaryIndex> FileWriter::add_end_reason(
        EndReasonData const& end_reason_data) {
    return m_impl->add_end_reason(end_reason_data);
}

mkr::Result<RunInfoDictionaryIndex> FileWriter::add_run_info(RunInfoData const& run_info_data) {
    return m_impl->add_run_info(run_info_data);
}

mkr::Result<FileWriterImpl::DictionaryWriters> make_dictionary_writers(arrow::MemoryPool* pool) {
    FileWriterImpl::DictionaryWriters writers;
    ARROW_ASSIGN_OR_RAISE(writers.pore_writer, mkr::make_pore_writer(pool));
    ARROW_ASSIGN_OR_RAISE(writers.calibration_writer, mkr::make_calibration_writer(pool));
    ARROW_ASSIGN_OR_RAISE(writers.end_reason_writer, mkr::make_end_reason_writer(pool));
    ARROW_ASSIGN_OR_RAISE(writers.run_info_writer, mkr::make_run_info_writer(pool));

    return writers;
}

mkr::Result<std::unique_ptr<FileWriter>> create_split_file_writer(
        boost::filesystem::path const& signal_path,
        boost::filesystem::path const& reads_path,
        std::string const& writing_software_name,
        FileWriterOptions const& options) {
    auto pool = options.memory_pool();
    if (!pool) {
        return Status::Invalid("Invalid memory pool specified for file writer");
    }

    // Open dictionary writrs:
    ARROW_ASSIGN_OR_RAISE(auto dict_writers, make_dictionary_writers(pool));

    // Prep file metadata:
    auto file_identifier = boost::uuids::random_generator_mt19937()();

    ARROW_ASSIGN_OR_RAISE(
            auto file_schema_metadata,
            make_schema_key_value_metadata({file_identifier, writing_software_name, MkrVersion}));

    // Open read file table:
    ARROW_ASSIGN_OR_RAISE(auto read_table_file,
                          arrow::io::FileOutputStream::Open(reads_path.string(), false));
    ARROW_ASSIGN_OR_RAISE(
            auto read_table_writer,
            make_read_table_writer(read_table_file, file_schema_metadata, dict_writers.pore_writer,
                                   dict_writers.calibration_writer, dict_writers.end_reason_writer,
                                   dict_writers.run_info_writer, pool));

    // Open signal file table:
    ARROW_ASSIGN_OR_RAISE(auto signal_table_file,
                          arrow::io::FileOutputStream::Open(signal_path.string(), false));
    ARROW_ASSIGN_OR_RAISE(auto signal_table_writer,
                          make_signal_table_writer(signal_table_file, file_schema_metadata,
                                                   options.signal_type(), pool));

    // Throw it all together into a writer object:
    return std::make_unique<FileWriter>(std::make_unique<FileWriterImpl>(
            std::move(dict_writers), std::move(read_table_writer), std::move(signal_table_writer),
            options.max_signal_chunk_size(), pool));
}

mkr::Result<std::unique_ptr<FileWriter>> create_combined_file_writer(
        boost::filesystem::path const& path,
        std::string const& writing_software_name,
        FileWriterOptions const& options) {
    auto pool = options.memory_pool();
    if (!pool) {
        return Status::Invalid("Invalid memory pool specified for file writer");
    }

    // Open dictionary writrs:
    ARROW_ASSIGN_OR_RAISE(auto dict_writers, make_dictionary_writers(pool));

    // Prep file metadata:
    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto const section_marker = uuid_gen();
    auto const file_identifier = uuid_gen();

    ARROW_ASSIGN_OR_RAISE(
            auto file_schema_metadata,
            make_schema_key_value_metadata({file_identifier, writing_software_name, MkrVersion}));

    auto reads_tmp_path = path.parent_path() / ("." + path.filename().string() + ".tmp-reads");

    // Prepare the temporary reads file:
    ARROW_ASSIGN_OR_RAISE(auto read_table_file,
                          arrow::io::FileOutputStream::Open(reads_tmp_path.string(), false));
    ARROW_ASSIGN_OR_RAISE(
            auto read_table_tmp_writer,
            make_read_table_writer(read_table_file, file_schema_metadata, dict_writers.pore_writer,
                                   dict_writers.calibration_writer, dict_writers.end_reason_writer,
                                   dict_writers.run_info_writer, pool));

    // Prepare the main file - and set up the signal table to write here:
    ARROW_ASSIGN_OR_RAISE(auto main_file, arrow::io::FileOutputStream::Open(path.string(), false));

    // Write the initial header to the combined file:
    ARROW_RETURN_NOT_OK(combined_file_utils::write_combined_header(main_file, section_marker));

    // Then place the signal file directly after that:
    ARROW_ASSIGN_OR_RAISE(auto signal_table_start, main_file->Tell());
    ARROW_ASSIGN_OR_RAISE(
            auto signal_table_writer,
            make_signal_table_writer(main_file, file_schema_metadata, options.signal_type(), pool));

    // Throw it all together into a writer object:
    return std::make_unique<FileWriter>(std::make_unique<CombinedFileWriterImpl>(
            path, reads_tmp_path, signal_table_start, section_marker, file_identifier,
            writing_software_name, std::move(dict_writers), std::move(read_table_tmp_writer),
            std::move(signal_table_writer), options.max_signal_chunk_size(), pool));
}

}  // namespace mkr