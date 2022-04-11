#include "mkr_format/file_reader.h"

#include "mkr_format/read_table_reader.h"
#include "mkr_format/signal_table_reader.h"

#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <boost/uuid/uuid_io.hpp>

namespace mkr {

FileReaderOptions::FileReaderOptions() : m_memory_pool(arrow::system_memory_pool()) {}

class FileReaderBaseImpl : public FileReader {
public:
    FileReaderBaseImpl(ReadTableReader&& read_table_reader,
                       SignalTableReader&& signal_table_reader) :
            m_read_table_reader(std::move(read_table_reader)),
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
    SplitFileReader(ReadTableReader&& read_table_reader, SignalTableReader&& signal_table_reader) :
            FileReaderBaseImpl(std::move(read_table_reader), std::move(signal_table_reader)) {}
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

}  // namespace mkr