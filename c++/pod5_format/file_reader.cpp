#include "pod5_format/file_reader.h"

#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/signal_table_reader.h"

#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <boost/uuid/uuid_io.hpp>

namespace pod5 {

FileReaderOptions::FileReaderOptions() : m_memory_pool(arrow::system_memory_pool()) {}

class FileReaderImpl : public FileReader {
public:
    FileReaderImpl(FileLocation&& read_table_location,
                   ReadTableReader&& read_table_reader,
                   FileLocation&& signal_table_location,
                   SignalTableReader&& signal_table_reader)
            : m_read_table_location(std::move(read_table_location)),
              m_signal_table_location(std::move(signal_table_location)),
              m_read_table_reader(std::move(read_table_reader)),
              m_signal_table_reader(std::move(signal_table_reader)) {}

    Result<ReadTableRecordBatch> read_read_record_batch(std::size_t i) const override {
        return m_read_table_reader.read_record_batch(i);
    }

    std::size_t num_read_record_batches() const override {
        return m_read_table_reader.num_record_batches();
    }

    Result<std::size_t> search_for_read_ids(ReadIdSearchInput const& search_input,
                                            gsl::span<uint32_t> const& batch_counts,
                                            gsl::span<uint32_t> const& batch_rows) override {
        return m_read_table_reader.search_for_read_ids(search_input, batch_counts, batch_rows);
    }

    Result<SignalTableRecordBatch> read_signal_record_batch(std::size_t i) const override {
        return m_signal_table_reader.read_record_batch(i);
    }

    std::size_t num_signal_record_batches() const override {
        return m_signal_table_reader.num_record_batches();
    }

    Result<std::size_t> signal_batch_for_row_id(std::size_t row,
                                                std::size_t* batch_start_row) const override {
        return m_signal_table_reader.signal_batch_for_row_id(row, batch_start_row);
    }

    Result<FileLocation> read_table_location() const override { return m_read_table_location; }

    Result<FileLocation> signal_table_location() const override { return m_signal_table_location; }

private:
    FileLocation m_read_table_location;
    FileLocation m_signal_table_location;
    ReadTableReader m_read_table_reader;
    SignalTableReader m_signal_table_reader;
};

pod5::Result<std::unique_ptr<FileReader>> open_split_file_reader(
        boost::filesystem::path const& signal_path,
        boost::filesystem::path const& reads_path,
        FileReaderOptions const& options) {
    auto pool = options.memory_pool();
    if (!pool) {
        return Status::Invalid("Invalid memory pool specified for file writer");
    }

    ARROW_ASSIGN_OR_RAISE(auto read_table_file,
                          arrow::io::ReadableFile::Open(reads_path.string(), pool));
    ARROW_ASSIGN_OR_RAISE(auto read_table_reader, make_read_table_reader(read_table_file, pool));

    ARROW_ASSIGN_OR_RAISE(auto signal_table_file,
                          arrow::io::ReadableFile::Open(signal_path.string(), pool));
    ARROW_ASSIGN_OR_RAISE(auto signal_table_reader,
                          make_signal_table_reader(signal_table_file, pool));

    auto signal_metadata = signal_table_reader.schema_metadata();
    auto reads_metadata = read_table_reader.schema_metadata();
    if (signal_metadata.file_identifier != reads_metadata.file_identifier) {
        return Status::Invalid("Invalid read and signal file pair signal identifier: ",
                               signal_metadata.file_identifier,
                               ", reads identifier: ", reads_metadata.file_identifier);
    }

    ARROW_ASSIGN_OR_RAISE(auto const reads_table_size, read_table_file->GetSize());
    ARROW_ASSIGN_OR_RAISE(auto const signal_table_size, signal_table_file->GetSize());
    return std::make_unique<FileReaderImpl>(
            FileLocation(reads_path, 0, reads_table_size), std::move(read_table_reader),
            FileLocation(signal_path, 0, signal_table_size), std::move(signal_table_reader));
}

class SubFile : public arrow::io::RandomAccessFile {
public:
    SubFile(std::shared_ptr<arrow::io::RandomAccessFile>&& main_file,
            std::int64_t sub_file_offset,
            std::int64_t sub_file_length)
            : m_file(std::move(main_file)),
              m_sub_file_offset(sub_file_offset),
              m_sub_file_length(sub_file_length) {}

    arrow::Status Close() override { return m_file->Close(); }

    bool closed() const override { return m_file->closed(); }

    arrow::Result<std::int64_t> Tell() const override {
        ARROW_ASSIGN_OR_RAISE(auto t, m_file->Tell());
        return t - m_sub_file_offset;
    }

    arrow::Status Seek(int64_t offset) override {
        offset += m_sub_file_offset;
        return m_file->Seek(offset);
    }

    arrow::Result<std::int64_t> Read(int64_t length, void* data) override {
        return m_file->Read(length, data);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t length) override {
        return m_file->Read(length);
    }

    arrow::Result<std::int64_t> GetSize() override { return m_sub_file_length; }

private:
    std::shared_ptr<arrow::io::RandomAccessFile> m_file;
    std::int64_t m_sub_file_offset;
    std::int64_t m_sub_file_length;
};

pod5::Result<std::unique_ptr<FileReader>> open_combined_file_reader(
        boost::filesystem::path const& path,
        FileReaderOptions const& options) {
    auto pool = options.memory_pool();
    if (!pool) {
        return Status::Invalid("Invalid memory pool specified for file writer");
    }

    ARROW_ASSIGN_OR_RAISE(
            auto file, arrow::io::MemoryMappedFile::Open(path.string(), arrow::io::FileMode::READ));

    ARROW_ASSIGN_OR_RAISE(auto parsed_footer_metadata, combined_file_utils::read_footer(file));

    // Files are written standalone, and so needs to be treated with a file offset - it wants to seek around as if the reads file is standalone:

    // Seek to the start of the sub-file:
    ARROW_RETURN_NOT_OK(file->Seek(parsed_footer_metadata.reads_table.file_start_offset));
    // Restrict our open file to just the reads section:
    auto reads_sub_file =
            std::make_shared<SubFile>(file, parsed_footer_metadata.reads_table.file_start_offset,
                                      parsed_footer_metadata.reads_table.file_length);

    ARROW_ASSIGN_OR_RAISE(auto read_table_reader, make_read_table_reader(reads_sub_file, pool));

    // Seek to the start of the sub-file:
    ARROW_RETURN_NOT_OK(file->Seek(parsed_footer_metadata.signal_table.file_start_offset));
    // Signal file is generated _into_ the main file, and so the offset should start at 0, as all internal offsets in the arrow file will be relative to the main file header:
    auto signal_sub_file =
            std::make_shared<SubFile>(file, parsed_footer_metadata.signal_table.file_start_offset,
                                      parsed_footer_metadata.signal_table.file_length);
    ARROW_ASSIGN_OR_RAISE(auto signal_table_reader,
                          make_signal_table_reader(signal_sub_file, pool));

    auto signal_metadata = signal_table_reader.schema_metadata();
    auto reads_metadata = read_table_reader.schema_metadata();
    if (signal_metadata.file_identifier != reads_metadata.file_identifier) {
        return Status::Invalid("Invalid read and signal file pair signal identifier: ",
                               signal_metadata.file_identifier,
                               ", reads identifier: ", reads_metadata.file_identifier);
    }

    auto reads_table_location =
            FileLocation(path, parsed_footer_metadata.reads_table.file_start_offset,
                         parsed_footer_metadata.reads_table.file_length);
    auto signal_table_location =
            FileLocation(path, parsed_footer_metadata.signal_table.file_start_offset,
                         parsed_footer_metadata.signal_table.file_length);

    return std::make_unique<FileReaderImpl>(
            std::move(reads_table_location), std::move(read_table_reader),
            std::move(signal_table_location), std::move(signal_table_reader));
}

}  // namespace pod5
