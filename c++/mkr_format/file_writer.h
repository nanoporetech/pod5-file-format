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

class MKR_FORMAT_EXPORT FileWriter {
public:
    virtual ~FileWriter() = default;
    /*
    virtual arrow::Status add_complete_read(
        ReadData const& read_data,
        gsl::span<SignalTableRowIndex> const& signal
    ) = 0;
*/

    virtual arrow::Status add_complete_read(ReadData const& read_data,
                                            gsl::span<std::int16_t> const& signal) = 0;

    virtual mkr::Result<PoreDictionaryIndex> add_pore(PoreData const& pore_data) = 0;
    virtual mkr::Result<CalibrationDictionaryIndex> add_calibration(
            CalibrationData const& calibration_data) = 0;
    virtual mkr::Result<EndReasonDictionaryIndex> add_end_reason(
            EndReasonData const& end_reason_data) = 0;
    virtual mkr::Result<RunInfoDictionaryIndex> add_run_info(RunInfoData const& run_info_data) = 0;
};

MKR_FORMAT_EXPORT mkr::Result<std::unique_ptr<FileWriter>> create_split_file_writer(
        boost::filesystem::path const& signal_path,
        boost::filesystem::path const& reads_path,
        std::string const& writing_software_name,
        FileWriterOptions const& options);

}  // namespace mkr