#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/read_table_utils.h"
#include "pod5_format/result.h"
#include "pod5_format/signal_table_utils.h"

#include <cstdint>
#include <memory>

namespace arrow {
class MemoryPool;
}

namespace pod5 {

class ThreadPool;

class POD5_FORMAT_EXPORT FileWriterOptions {
public:
    /// \brief Default chunk size for signal table entries
    static constexpr std::uint32_t DEFAULT_SIGNAL_CHUNK_SIZE = 102'400;
    static constexpr std::uint32_t DEFAULT_SIGNAL_TABLE_BATCH_SIZE = 100;
    static constexpr std::uint32_t DEFAULT_READ_TABLE_BATCH_SIZE = 1000;
    static constexpr std::uint32_t DEFAULT_RUN_INFO_TABLE_BATCH_SIZE = 1;
    static constexpr SignalType DEFAULT_SIGNAL_TYPE = SignalType::VbzSignal;

    FileWriterOptions();

    void set_max_signal_chunk_size(std::uint32_t chunk_size)
    {
        m_max_signal_chunk_size = chunk_size;
    }

    std::uint32_t max_signal_chunk_size() const { return m_max_signal_chunk_size; }

    void memory_pool(arrow::MemoryPool * memory_pool) { m_memory_pool = memory_pool; }

    arrow::MemoryPool * memory_pool() const { return m_memory_pool; }

    void set_signal_type(SignalType signal_type) { m_signal_type = signal_type; }

    SignalType signal_type() const { return m_signal_type; }

    void set_signal_table_batch_size(std::size_t batch_size)
    {
        m_signal_table_batch_size = batch_size;
    }

    std::size_t signal_table_batch_size() const { return m_signal_table_batch_size; }

    void set_read_table_batch_size(std::size_t batch_size) { m_read_table_batch_size = batch_size; }

    std::size_t read_table_batch_size() const { return m_read_table_batch_size; }

    void set_run_info_table_batch_size(std::size_t batch_size)
    {
        m_run_info_table_batch_size = batch_size;
    }

    std::size_t run_info_table_batch_size() const { return m_run_info_table_batch_size; }

    void set_thread_pool(std::shared_ptr<ThreadPool> const & writer_thread_pool)
    {
        m_writer_thread_pool = writer_thread_pool;
    }

    std::shared_ptr<ThreadPool> thread_pool() const { return m_writer_thread_pool; }

private:
    std::shared_ptr<ThreadPool> m_writer_thread_pool;
    std::uint32_t m_max_signal_chunk_size;
    arrow::MemoryPool * m_memory_pool;
    SignalType m_signal_type;
    std::size_t m_signal_table_batch_size;
    std::size_t m_read_table_batch_size;
    std::size_t m_run_info_table_batch_size;
};

class FileWriterImpl;

class POD5_FORMAT_EXPORT FileWriter {
public:
    FileWriter(std::unique_ptr<FileWriterImpl> && impl);
    ~FileWriter();

    pod5::Status close();

    pod5::Status add_complete_read(
        ReadData const & read_data,
        gsl::span<std::int16_t const> const & signal);

    /// \brief Add a complete with rows already pre appended.
    pod5::Status add_complete_read(
        ReadData const & read_data,
        gsl::span<std::uint64_t const> const & signal_rows,
        std::uint64_t signal_duration);

    pod5::Result<std::vector<SignalTableRowIndex>> add_signal(
        boost::uuids::uuid const & read_id,
        gsl::span<std::int16_t const> const & signal);

    pod5::Result<SignalTableRowIndex> add_pre_compressed_signal(
        boost::uuids::uuid const & read_id,
        gsl::span<std::uint8_t const> const & signal_bytes,
        std::uint32_t sample_count);

    // Find or create an end reason index representing this read end reason.
    pod5::Result<EndReasonDictionaryIndex> lookup_end_reason(ReadEndReason end_reason) const;
    pod5::Result<PoreDictionaryIndex> add_pore_type(std::string const & pore_type_data);
    pod5::Result<RunInfoDictionaryIndex> add_run_info(RunInfoData const & run_info_data);

    SignalType signal_type() const;

    FileWriterImpl * impl() const { return m_impl.get(); };

private:
    std::unique_ptr<FileWriterImpl> m_impl;
};

POD5_FORMAT_EXPORT pod5::Result<std::unique_ptr<FileWriter>> create_file_writer(
    std::string const & path,
    std::string const & writing_software_name,
    FileWriterOptions const & options = {});

POD5_FORMAT_EXPORT pod5::Result<std::unique_ptr<FileWriter>> recover_file_writer(
    std::string const & src_path,
    std::string const & dest_path,
    FileWriterOptions const & options = {});

}  // namespace pod5
