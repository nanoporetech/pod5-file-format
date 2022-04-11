#include "mkr_format/file_writer.h"

#include "mkr_format/read_table_writer.h"
#include "mkr_format/read_table_writer_utils.h"
#include "mkr_format/schema_metadata.h"
#include "mkr_format/signal_table_writer.h"
#include "mkr_format/version.h"

#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <boost/uuid/random_generator.hpp>

namespace mkr {

FileWriterOptions::FileWriterOptions() :
        m_max_signal_chunk_size(DEFAULT_SIGNAL_CHUNK_SIZE),
        m_memory_pool(arrow::system_memory_pool()),
        m_signal_type(DEFAULT_SIGNAL_TYPE) {}

class FileWriterBaseImpl : public FileWriter {
public:
    struct DictionaryWriters {
        std::shared_ptr<PoreWriter> pore_writer;
        std::shared_ptr<EndReasonWriter> end_reason_writer;
        std::shared_ptr<CalibrationWriter> calibration_writer;
        std::shared_ptr<RunInfoWriter> run_info_writer;
    };

    FileWriterBaseImpl(DictionaryWriters&& dict_writers) :
            m_dict_writers(std::move(dict_writers)) {}
    ~FileWriterBaseImpl() = default;

    mkr::Result<PoreDictionaryIndex> add_pore(PoreData const& pore_data) override {
        return m_dict_writers.pore_writer->add(pore_data);
    }

    mkr::Result<CalibrationDictionaryIndex> add_calibration(
            CalibrationData const& calibration_data) override {
        return m_dict_writers.calibration_writer->add(calibration_data);
    }

    mkr::Result<EndReasonDictionaryIndex> add_end_reason(
            EndReasonData const& end_reason_data) override {
        return m_dict_writers.end_reason_writer->add(end_reason_data);
    }

    mkr::Result<RunInfoDictionaryIndex> add_run_info(RunInfoData const& run_info_data) override {
        return m_dict_writers.run_info_writer->add(run_info_data);
    }

private:
    DictionaryWriters m_dict_writers;
};

class SplitFileWriter : public FileWriterBaseImpl {
public:
    SplitFileWriter(DictionaryWriters&& dict_writers,
                    ReadTableWriter&& read_table_writer,
                    SignalTableWriter&& signal_table_writer,
                    std::uint32_t signal_chunk_size,
                    arrow::MemoryPool* pool) :
            FileWriterBaseImpl(std::move(dict_writers)),
            m_read_table_writer(std::move(read_table_writer)),
            m_signal_table_writer(std::move(signal_table_writer)),
            m_signal_chunk_size(signal_chunk_size),
            m_pool(pool) {}

    arrow::Status add_complete_read(ReadData const& read_data,
                                    gsl::span<std::int16_t> const& signal) {
        arrow::TypedBufferBuilder<std::uint64_t> signal_row_builder(m_pool);

        // Chunk and write each piece of signal to the file:
        for (std::size_t chunk_start = 0; chunk_start < signal.size();
             chunk_start += m_signal_chunk_size) {
            std::size_t chunk_size =
                    std::min<std::size_t>(signal.size() - chunk_start, m_signal_chunk_size);

            auto const chunk_span = signal.subspan(chunk_start, chunk_size);

            ARROW_ASSIGN_OR_RAISE(auto row_index,
                                  m_signal_table_writer.add_signal(read_data.read_id, chunk_span));
            ARROW_RETURN_NOT_OK(signal_row_builder.Append(row_index));
        }

        // Write read data and signal row entries:
        auto read_table_row = m_read_table_writer.add_read(
                read_data, gsl::make_span(signal_row_builder.data(), signal_row_builder.length()));
        return read_table_row.status();
    }

private:
    ReadTableWriter m_read_table_writer;
    SignalTableWriter m_signal_table_writer;
    std::uint32_t m_signal_chunk_size;
    arrow::MemoryPool* m_pool;
};

mkr::Result<FileWriterBaseImpl::DictionaryWriters> make_dictionary_writers(
        arrow::MemoryPool* pool) {
    FileWriterBaseImpl::DictionaryWriters writers;
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

    ARROW_ASSIGN_OR_RAISE(auto dict_writers, make_dictionary_writers(pool));

    auto file_identifier = boost::uuids::random_generator_mt19937()();

    ARROW_ASSIGN_OR_RAISE(
            auto file_schema_metadata,
            make_schema_key_value_metadata({file_identifier, writing_software_name, MkrVersion}));

    ARROW_ASSIGN_OR_RAISE(auto read_table_file,
                          arrow::io::FileOutputStream::Open(reads_path.string(), pool));
    ARROW_ASSIGN_OR_RAISE(
            auto read_table_writer,
            make_read_table_writer(read_table_file, file_schema_metadata, dict_writers.pore_writer,
                                   dict_writers.calibration_writer, dict_writers.end_reason_writer,
                                   dict_writers.run_info_writer, pool));

    ARROW_ASSIGN_OR_RAISE(auto signal_table_file,
                          arrow::io::FileOutputStream::Open(signal_path.string(), pool));
    ARROW_ASSIGN_OR_RAISE(auto signal_table_writer,
                          make_signal_table_writer(signal_table_file, file_schema_metadata,
                                                   options.signal_type(), pool));

    return std::make_unique<SplitFileWriter>(std::move(dict_writers), std::move(read_table_writer),
                                             std::move(signal_table_writer),
                                             options.max_signal_chunk_size(), pool);
}

}  // namespace mkr