#include "mkr_format/read_table_writer.h"

#include "mkr_format/errors.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/extension_type.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace mkr {

ReadTableWriter::ReadTableWriter(std::shared_ptr<arrow::ipc::RecordBatchWriter>&& writer,
                                 std::shared_ptr<arrow::Schema>&& schema,
                                 ReadTableSchemaDescription const& field_locations,
                                 std::shared_ptr<PoreWriter> const& pore_writer,
                                 std::shared_ptr<CalibrationWriter> const& calibration_writer,
                                 std::shared_ptr<EndReasonWriter> const& end_reason_writer,
                                 std::shared_ptr<RunInfoWriter> const& run_info_writer,
                                 arrow::MemoryPool* pool) :
        m_pool(pool),
        m_schema(schema),
        m_field_locations(field_locations),
        m_writer(std::move(writer)),
        m_pore_writer(pore_writer),
        m_calibration_writer(calibration_writer),
        m_end_reason_writer(end_reason_writer),
        m_run_info_writer(run_info_writer) {
    auto uuid_type = m_schema->field(m_field_locations.read_id)->type();
    assert(uuid_type->id() == arrow::Type::EXTENSION);
    auto uuid_extension = static_cast<arrow::ExtensionType*>(uuid_type.get());
    m_read_id_builder =
            std::make_unique<arrow::FixedSizeBinaryBuilder>(uuid_extension->storage_type(), m_pool);

    m_signal_array_builder = std::make_shared<arrow::UInt64Builder>(pool);
    m_signal_builder = std::make_unique<arrow::ListBuilder>(pool, m_signal_array_builder),

    m_read_number_builder = std::make_unique<arrow::UInt32Builder>(pool);
    m_start_sample_builder = std::make_unique<arrow::UInt64Builder>(pool);
    m_median_before_builder = std::make_unique<arrow::FloatBuilder>(pool);

    m_pore_builder = std::make_unique<arrow::Int16Builder>(pool);
    m_calibration_builder = std::make_unique<arrow::Int16Builder>(pool);
    m_end_reason_builder = std::make_unique<arrow::Int16Builder>(pool);
    m_run_info_builder = std::make_unique<arrow::Int16Builder>(pool);

    assert(m_read_id_builder->byte_width() == 16);
}

ReadTableWriter::ReadTableWriter(ReadTableWriter&& other) = default;
ReadTableWriter& ReadTableWriter::operator=(ReadTableWriter&&) = default;
ReadTableWriter::~ReadTableWriter() {
    flush();
    close();
}

Result<std::size_t> ReadTableWriter::add_read(ReadData const& read_data,
                                              gsl::span<SignalTableRowIndex const> const& signal) {
    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    auto row_id = m_flushed_row_count + m_current_batch_row_count;
    ARROW_RETURN_NOT_OK(m_read_id_builder->Append(read_data.read_id.begin()));

    ARROW_RETURN_NOT_OK(m_signal_builder->Append());  // start new slot
    ARROW_RETURN_NOT_OK(m_signal_array_builder->AppendValues(signal.data(), signal.size()));

    ARROW_RETURN_NOT_OK(m_read_number_builder->Append(read_data.read_number));
    ARROW_RETURN_NOT_OK(m_start_sample_builder->Append(read_data.start_sample));
    ARROW_RETURN_NOT_OK(m_median_before_builder->Append(read_data.median_before));

    assert(read_data.pore <= m_pore_writer->item_count());
    ARROW_RETURN_NOT_OK(m_pore_builder->Append(read_data.pore));
    assert(read_data.end_reason <= m_end_reason_writer->item_count());
    ARROW_RETURN_NOT_OK(m_end_reason_builder->Append(read_data.end_reason));
    assert(read_data.calibration <= m_calibration_writer->item_count());
    ARROW_RETURN_NOT_OK(m_calibration_builder->Append(read_data.calibration));
    assert(read_data.run_info <= m_run_info_writer->item_count());
    ARROW_RETURN_NOT_OK(m_run_info_builder->Append(read_data.run_info));

    ++m_current_batch_row_count;
    return row_id;
}

Status ReadTableWriter::flush() {
    if (m_current_batch_row_count == 0) {
        return Status::OK();
    }

    if (!m_writer) {
        return Status::IOError("Writer terminated");
    }

    std::vector<std::shared_ptr<arrow::Array>> columns{nullptr, nullptr, nullptr, nullptr, nullptr,
                                                       nullptr, nullptr, nullptr, nullptr};
    ARROW_RETURN_NOT_OK(m_read_id_builder->Finish(&columns[m_field_locations.read_id]));
    ARROW_RETURN_NOT_OK(m_signal_builder->Finish(&columns[m_field_locations.signal]));
    ARROW_RETURN_NOT_OK(m_read_number_builder->Finish(&columns[m_field_locations.read_number]));
    ARROW_RETURN_NOT_OK(m_start_sample_builder->Finish(&columns[m_field_locations.start_sample]));
    ARROW_RETURN_NOT_OK(m_median_before_builder->Finish(&columns[m_field_locations.median_before]));

    auto finish_dictionary =
            [](std::shared_ptr<arrow::Array>* dest,
               std::unique_ptr<arrow::Int16Builder>& index_builder,
               std::shared_ptr<DictionaryWriter> const& dict_values) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(auto indices, index_builder->Finish());
        ARROW_ASSIGN_OR_RAISE(*dest, dict_values->build_dictionary_array(indices));
        return Status::OK();
    };

    ARROW_RETURN_NOT_OK(
            finish_dictionary(&columns[m_field_locations.pore], m_pore_builder, m_pore_writer));
    ARROW_RETURN_NOT_OK(finish_dictionary(&columns[m_field_locations.calibration],
                                          m_calibration_builder, m_calibration_writer));
    ARROW_RETURN_NOT_OK(finish_dictionary(&columns[m_field_locations.end_reason],
                                          m_end_reason_builder, m_end_reason_writer));
    ARROW_RETURN_NOT_OK(finish_dictionary(&columns[m_field_locations.run_info], m_run_info_builder,
                                          m_run_info_writer));

    auto const record_batch =
            arrow::RecordBatch::Make(m_schema, m_current_batch_row_count, std::move(columns));

    m_flushed_row_count += m_current_batch_row_count;
    m_current_batch_row_count = 0;
    ARROW_RETURN_NOT_OK(m_writer->WriteRecordBatch(*record_batch));
    return Status();
}

Status ReadTableWriter::close() {
    // Check for already closed
    if (!m_writer) {
        return Status::OK();
    }

    ARROW_RETURN_NOT_OK(m_writer->Close());
    m_writer = nullptr;
    return Status::OK();
}

Result<ReadTableWriter> make_read_table_writer(
        std::shared_ptr<arrow::io::OutputStream> const& sink,
        std::shared_ptr<const arrow::KeyValueMetadata> const& metadata,
        std::shared_ptr<PoreWriter> const& pore_writer,
        std::shared_ptr<CalibrationWriter> const& calibration_writer,
        std::shared_ptr<EndReasonWriter> const& end_reason_writer,
        std::shared_ptr<RunInfoWriter> const& run_info_writer,
        arrow::MemoryPool* pool) {
    ReadTableSchemaDescription field_locations;
    auto schema = make_read_table_schema(metadata, &field_locations);

    arrow::ipc::IpcWriteOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(sink, schema, options, metadata));

    return ReadTableWriter(std::move(writer), std::move(schema), field_locations, pore_writer,
                           calibration_writer, end_reason_writer, run_info_writer, pool);
}

}  // namespace mkr