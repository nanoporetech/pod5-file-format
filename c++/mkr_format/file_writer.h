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

class MKR_FORMAT_EXPORT FileWriterOptions {
public:
    /// \brief Default chunk size for signal table entries
    static constexpr std::uint32_t DEFAULT_SIGNAL_CHUNK_SIZE = 20'480;
    static constexpr SignalType DEFAULT_SIGNAL_TYPE = SignalType::VbzSignal;

    FileWriterOptions();

    void set_max_signal_chunk_size(std::uint32_t chunk_size) {
        m_max_signal_chunk_size = chunk_size;
    }
    std::uint32_t max_signal_chunk_size() const { return m_max_signal_chunk_size; }

    void memory_pool(arrow::MemoryPool* memory_pool) { m_memory_pool = memory_pool; }
    arrow::MemoryPool* memory_pool() const { return m_memory_pool; }

    void set_signal_type(SignalType signal_type) { m_signal_type = signal_type; }
    SignalType signal_type() const { return m_signal_type; }

private:
    std::uint32_t m_max_signal_chunk_size;
    arrow::MemoryPool* m_memory_pool;
    SignalType m_signal_type;
};

class FileWriterImpl;
class MKR_FORMAT_EXPORT FileWriter {
public:
    FileWriter(std::unique_ptr<FileWriterImpl>&& impl);
    ~FileWriter();

    arrow::Status close();

    arrow::Status add_complete_read(ReadData const& read_data,
                                    gsl::span<std::int16_t const> const& signal);

    mkr::Result<PoreDictionaryIndex> add_pore(PoreData const& pore_data);
    mkr::Result<CalibrationDictionaryIndex> add_calibration(
            CalibrationData const& calibration_data);
    mkr::Result<EndReasonDictionaryIndex> add_end_reason(EndReasonData const& end_reason_data);
    mkr::Result<RunInfoDictionaryIndex> add_run_info(RunInfoData const& run_info_data);

private:
    std::unique_ptr<FileWriterImpl> m_impl;
};

MKR_FORMAT_EXPORT mkr::Result<std::unique_ptr<FileWriter>> create_split_file_writer(
        boost::filesystem::path const& signal_path,
        boost::filesystem::path const& reads_path,
        std::string const& writing_software_name,
        FileWriterOptions const& options);

MKR_FORMAT_EXPORT mkr::Result<std::unique_ptr<FileWriter>> create_combined_file_writer(
        boost::filesystem::path const& path,
        std::string const& writing_software_name,
        FileWriterOptions const& options);

}  // namespace mkr