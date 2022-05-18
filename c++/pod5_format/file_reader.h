#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/read_table_utils.h"
#include "pod5_format/result.h"
#include "pod5_format/signal_table_utils.h"

#include <boost/filesystem/path.hpp>

#include <cstdint>
#include <memory>

namespace arrow {
class MemoryPool;
}

namespace pod5 {

class POD5_FORMAT_EXPORT FileReaderOptions {
public:
    FileReaderOptions();

    void memory_pool(arrow::MemoryPool* memory_pool) { m_memory_pool = memory_pool; }
    arrow::MemoryPool* memory_pool() const { return m_memory_pool; }

private:
    arrow::MemoryPool* m_memory_pool;
};

class POD5_FORMAT_EXPORT FileLocation {
public:
    FileLocation(boost::filesystem::path const& file_path_, std::size_t offset_, std::size_t size_)
            : file_path(file_path_), offset(offset_), size(size_) {}

    boost::filesystem::path file_path;
    std::size_t offset;
    std::size_t size;
};

class ReadTableRecordBatch;
class SignalTableRecordBatch;

class POD5_FORMAT_EXPORT FileReader {
public:
    virtual ~FileReader() = default;

    virtual Result<ReadTableRecordBatch> read_read_record_batch(std::size_t i) const = 0;
    virtual std::size_t num_read_record_batches() const = 0;

    virtual Result<std::vector<TraversalStep>> search_for_read_ids(
            ReadIdSearchInput const& search_input,
            ReadTableTraversalType sort_order,
            std::size_t* successful_find_count) = 0;

    virtual Result<SignalTableRecordBatch> read_signal_record_batch(std::size_t i) const = 0;
    virtual std::size_t num_signal_record_batches() const = 0;
    virtual Result<std::size_t> signal_batch_for_row_id(std::size_t row,
                                                        std::size_t* batch_start_row) const = 0;

    virtual Result<FileLocation> read_table_location() const = 0;
    virtual Result<FileLocation> signal_table_location() const = 0;
};

POD5_FORMAT_EXPORT pod5::Result<std::unique_ptr<FileReader>> open_split_file_reader(
        boost::filesystem::path const& signal_path,
        boost::filesystem::path const& reads_path,
        FileReaderOptions const& options);

POD5_FORMAT_EXPORT pod5::Result<std::unique_ptr<FileReader>> open_combined_file_reader(
        boost::filesystem::path const& path,
        FileReaderOptions const& options);

}  // namespace pod5