#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/read_table_utils.h"
#include "mkr_format/result.h"
#include "mkr_format/signal_table_utils.h"

#include <boost/filesystem/path.hpp>

#include <cstdint>
#include <memory>

namespace arrow {
class MemoryPool;
}

namespace mkr {

class MKR_FORMAT_EXPORT FileReaderOptions {
public:
    FileReaderOptions();

    void memory_pool(arrow::MemoryPool* memory_pool) { m_memory_pool = memory_pool; }
    arrow::MemoryPool* memory_pool() const { return m_memory_pool; }

private:
    arrow::MemoryPool* m_memory_pool;
};

class ReadTableRecordBatch;
class SignalTableRecordBatch;

class MKR_FORMAT_EXPORT FileReader {
public:
    virtual ~FileReader() = default;

    virtual Result<ReadTableRecordBatch> read_read_record_batch(std::size_t i) const = 0;
    virtual std::size_t num_read_record_batches() const = 0;

    virtual Result<SignalTableRecordBatch> read_signal_record_batch(std::size_t i) const = 0;
    virtual std::size_t num_signal_record_batches() const = 0;
};

MKR_FORMAT_EXPORT mkr::Result<std::unique_ptr<FileReader>> open_split_file_reader(
        boost::filesystem::path const& signal_path,
        boost::filesystem::path const& reads_path,
        FileReaderOptions const& options);

}  // namespace mkr