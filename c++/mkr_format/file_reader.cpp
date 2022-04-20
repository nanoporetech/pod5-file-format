#include "mkr_format/file_reader.h"

#include "mkr_format/internal/combined_file_utils.h"
#include "mkr_format/read_table_reader.h"
#include "mkr_format/signal_table_reader.h"

#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <boost/uuid/uuid_io.hpp>

namespace mkr {

FileReaderOptions::FileReaderOptions() : m_memory_pool(arrow::system_memory_pool()) {}

class FileReaderBaseImpl : public FileReader {
public:
    FileReaderBaseImpl(ReadTableReader&& read_table_reader, SignalTableReader&& signal_table_reader)
            : m_read_table_reader(std::move(read_table_reader)),
              m_signal_table_reader(std::move(signal_table_reader)) {}

    Result<ReadTableRecordBatch> read_read_record_batch(std::size_t i) const override {
        return m_read_table_reader.read_record_batch(i);
    }

    std::size_t num_read_record_batches() const override {
        return m_read_table_reader.num_record_batches();
    }

    Result<SignalTableRecordBatch> read_signal_record_batch(std::size_t i) const override {
        return m_signal_table_reader.read_record_batch(i);
    }

    std::size_t num_signal_record_batches() const override {
        return m_signal_table_reader.num_record_batches();
    }

private:
    ReadTableReader m_read_table_reader;
    SignalTableReader m_signal_table_reader;
};

class SplitFileReader : public FileReaderBaseImpl {
public:
    SplitFileReader(ReadTableReader&& read_table_reader, SignalTableReader&& signal_table_reader)
            : FileReaderBaseImpl(std::move(read_table_reader), std::move(signal_table_reader)) {}
};

mkr::Result<std::unique_ptr<FileReader>> open_split_file_reader(
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

    return std::make_unique<SplitFileReader>(std::move(read_table_reader),
                                             std::move(signal_table_reader));
}

class SubFile : public arrow::io::RandomAccessFile {
public:
    SubFile(std::shared_ptr<arrow::io::RandomAccessFile>&& main_file,
            std::int64_t sub_file_offset,
            std::int64_t sub_file_length)
            : m_file(std::move(main_file)),
              m_sub_file_offset(sub_file_offset),
              m_sub_file_length(sub_file_length) {
        m_file->Seek(sub_file_offset);
    }

    arrow::Status Close() override { return m_file->Close(); }

    bool closed() const { return m_file->closed(); }

    arrow::Result<long int> Tell() const {
        ARROW_ASSIGN_OR_RAISE(auto t, m_file->Tell());
        return t - m_sub_file_offset;
    }

    arrow::Status Seek(int64_t offset) {
        offset += m_sub_file_offset;
        return m_file->Seek(offset);
    }

    arrow::Result<long int> Read(int64_t length, void* data) { return m_file->Read(length, data); }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t length) {
        return m_file->Read(length);
    }

    arrow::Result<long int> GetSize() { return m_sub_file_length; }

private:
    std::shared_ptr<arrow::io::RandomAccessFile> m_file;
    std::int64_t m_sub_file_offset;
    std::int64_t m_sub_file_length;
};

mkr::Result<std::unique_ptr<FileReader>> open_combined_file_reader(
        boost::filesystem::path const& path,
        FileReaderOptions const& options) {
    auto pool = options.memory_pool();
    if (!pool) {
        return Status::Invalid("Invalid memory pool specified for file writer");
    }

    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(path.string(), pool));

    ARROW_ASSIGN_OR_RAISE(auto parsed_footer_metadata, combined_file_utils::read_footer(file));

    // Restrict our open file to just the reads section:
    //
    // Reads file was written standalone, and so needs to be treated with a file offset - it wants to seek around as if the reads file is standalone:
    auto reads_sub_file =
            std::make_shared<SubFile>(file, parsed_footer_metadata.reads_table.file_start_offset,
                                      parsed_footer_metadata.reads_table.file_length);
    ARROW_ASSIGN_OR_RAISE(auto read_table_reader, make_read_table_reader(reads_sub_file, pool));

    // Open a second file and restrict it to just the signal section:
    ARROW_ASSIGN_OR_RAISE(auto signal_file, arrow::io::ReadableFile::Open(path.string(), pool));
    // Signal file is generated _into_ the main file, and so the offset should start at 0, as all internal offsets in the arrow file will be relative to the main file header:
    auto signal_sub_file = std::make_shared<SubFile>(
            signal_file, 0,
            parsed_footer_metadata.signal_table.file_length +
                    parsed_footer_metadata.signal_table.file_start_offset);
    ARROW_ASSIGN_OR_RAISE(auto signal_table_reader,
                          make_signal_table_reader(signal_sub_file, pool));

    auto signal_metadata = signal_table_reader.schema_metadata();
    auto reads_metadata = read_table_reader.schema_metadata();
    if (signal_metadata.file_identifier != reads_metadata.file_identifier) {
        return Status::Invalid("Invalid read and signal file pair signal identifier: ",
                               signal_metadata.file_identifier,
                               ", reads identifier: ", reads_metadata.file_identifier);
    }

    return std::make_unique<SplitFileReader>(std::move(read_table_reader),
                                             std::move(signal_table_reader));
}

}  // namespace mkr