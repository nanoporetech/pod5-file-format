#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/read_table_utils.h"
#include "pod5_format/result.h"
#include "pod5_format/signal_table_utils.h"

#include <cstdint>
#include <memory>

namespace arrow {
class Array;
class Buffer;
class MemoryPool;
}  // namespace arrow

namespace pod5 {

class Version;
struct SchemaMetadataDescription;

class POD5_FORMAT_EXPORT FileReaderOptions {
public:
    static constexpr std::uint32_t DEFAULT_MAX_CACHED_SIGNAL_TABLE_BATCHES = 5;

    FileReaderOptions();

    void memory_pool(arrow::MemoryPool * memory_pool) { m_memory_pool = memory_pool; }

    arrow::MemoryPool * memory_pool() const { return m_memory_pool; }

    std::size_t max_cached_signal_table_batches() const
    {
        return m_max_cached_signal_table_batches;
    }

    // Set how many signal table batches can be cached in memory,
    // Note: 0 here implies no limit.
    void set_max_cached_signal_table_batches(std::size_t max_cached_signal_table_batches);

private:
    arrow::MemoryPool * m_memory_pool;
    std::size_t m_max_cached_signal_table_batches;
};

class POD5_FORMAT_EXPORT FileLocation {
public:
    FileLocation(std::string const & file_path_, std::size_t offset_, std::size_t size_)
    : file_path(file_path_)
    , offset(offset_)
    , size(size_)
    {
    }

    std::string file_path;
    std::size_t offset;
    std::size_t size;
};

class ReadTableRecordBatch;
class SignalTableRecordBatch;

class POD5_FORMAT_EXPORT FileReader {
public:
    virtual ~FileReader() = default;

    /// \brief Find the read schema metadata for this file.
    virtual SchemaMetadataDescription schema_metadata() const = 0;

    virtual Result<std::size_t> read_count() const = 0;

    virtual Result<ReadTableRecordBatch> read_read_record_batch(std::size_t i) const = 0;
    virtual std::size_t num_read_record_batches() const = 0;

    virtual Result<std::size_t> search_for_read_ids(
        ReadIdSearchInput const & search_input,
        gsl::span<uint32_t> const & batch_counts,
        gsl::span<uint32_t> const & batch_rows) = 0;

    virtual Result<SignalTableRecordBatch> read_signal_record_batch(std::size_t i) const = 0;
    virtual std::size_t num_signal_record_batches() const = 0;
    virtual Result<std::size_t> signal_batch_for_row_id(std::size_t row, std::size_t * batch_row)
        const = 0;
    /// \brief Find the number of samples in a given list of rows.
    /// \param row_indices      The rows to query for sample ount.
    /// \returns The sum of all sample counts on input rows.
    virtual Result<std::size_t> extract_sample_count(
        gsl::span<std::uint64_t const> const & row_indices) const = 0;

    /// \brief Extract the samples for a list of rows.
    /// \param row_indices      The rows to query for samples.
    /// \param output_samples   The output samples from the rows.
    virtual Status extract_samples(
        gsl::span<std::uint64_t const> const & row_indices,
        gsl::span<std::int16_t> const & output_samples) const = 0;

    /// \brief Extract the samples as written in the arrow table for a list of rows.
    /// \param row_indices      The rows to query for samples.
    /// \param output_samples   The output samples from the rows.
    virtual Result<std::vector<std::shared_ptr<arrow::Buffer>>> extract_samples_inplace(
        gsl::span<std::uint64_t const> const & row_indices,
        std::vector<std::uint32_t> & sample_count) const = 0;

    virtual FileLocation const & run_info_table_location() const = 0;
    virtual FileLocation const & read_table_location() const = 0;
    virtual FileLocation const & signal_table_location() const = 0;

    virtual Version file_version_pre_migration() const = 0;

    virtual SignalType signal_type() const = 0;

    virtual Result<std::shared_ptr<RunInfoData const>> find_run_info(
        std::string const & acquisition_id) const = 0;

    virtual Result<std::shared_ptr<RunInfoData const>> get_run_info(std::size_t index) const = 0;
    virtual Result<std::size_t> get_run_info_count() const = 0;
};

POD5_FORMAT_EXPORT pod5::Result<std::shared_ptr<FileReader>> open_file_reader(
    std::string const & path,
    FileReaderOptions const & options = {});

}  // namespace pod5
