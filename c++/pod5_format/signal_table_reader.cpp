#include "pod5_format/signal_table_reader.h"

#include "pod5_format/schema_metadata.h"
#include "pod5_format/signal_compression.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

#include <iostream>

namespace pod5 {

SignalTableRecordBatch::SignalTableRecordBatch(std::shared_ptr<arrow::RecordBatch>&& batch,
                                               SignalTableSchemaDescription field_locations,
                                               arrow::MemoryPool* pool)
        : TableRecordBatch(std::move(batch)), m_field_locations(field_locations), m_pool(pool) {}

std::shared_ptr<UuidArray> SignalTableRecordBatch::read_id_column() const {
    return std::static_pointer_cast<UuidArray>(batch()->column(m_field_locations.read_id));
}

std::shared_ptr<arrow::LargeListArray> SignalTableRecordBatch::uncompressed_signal_column() const {
    return std::static_pointer_cast<arrow::LargeListArray>(
            batch()->column(m_field_locations.signal));
}

std::shared_ptr<VbzSignalArray> SignalTableRecordBatch::vbz_signal_column() const {
    return std::static_pointer_cast<VbzSignalArray>(batch()->column(m_field_locations.signal));
}

std::shared_ptr<arrow::UInt32Array> SignalTableRecordBatch::samples_column() const {
    return std::static_pointer_cast<arrow::UInt32Array>(batch()->column(m_field_locations.samples));
}

Result<std::size_t> SignalTableRecordBatch::samples_byte_count(std::size_t row_index) const {
    switch (m_field_locations.signal_type) {
    case SignalType::UncompressedSignal: {
        auto signal_column = uncompressed_signal_column();
        auto signal = signal_column->value_slice(row_index);
        return signal->length() * sizeof(std::int16_t);
    }
    case SignalType::VbzSignal: {
        auto signal_column = vbz_signal_column();
        auto signal_compressed = signal_column->Value(row_index);
        return signal_compressed.size();
    }
    }

    return pod5::Status::Invalid("Unknown signal type");
}

Status SignalTableRecordBatch::extract_signal_row(std::size_t row_index,
                                                  gsl::span<std::int16_t> samples) const {
    if (row_index >= num_rows()) {
        return pod5::Status::Invalid("Queried signal row ", row_index,
                                     " is outside the available rows (", num_rows(), " in batch)");
    }

    auto sample_count = samples_column();
    auto samples_in_row = sample_count->Value(row_index);
    if (samples_in_row != samples.size()) {
        return pod5::Status::Invalid("Unexpected size for sample array ", samples.size(),
                                     " expected ", samples_in_row);
    }

    switch (m_field_locations.signal_type) {
    case SignalType::UncompressedSignal: {
        auto signal_column = uncompressed_signal_column();
        auto signal =
                std::static_pointer_cast<arrow::Int16Array>(signal_column->value_slice(row_index));
        std::copy(signal->raw_values(), signal->raw_values() + signal->length(), samples.begin());
        return Status::OK();
    }
    case SignalType::VbzSignal: {
        auto signal_column = vbz_signal_column();
        auto signal_compressed = signal_column->Value(row_index);
        return pod5::decompress_signal(signal_compressed, m_pool, samples);
    }
    }

    return pod5::Status::Invalid("Unknown signal type");
}

Result<gsl::span<std::uint8_t const>> SignalTableRecordBatch::extract_signal_row_inplace(
        std::size_t row_index) const {
    if (row_index >= num_rows()) {
        return pod5::Status::Invalid("Queried signal row ", row_index,
                                     " is outside the available rows (", num_rows(), " in batch)");
    }

    switch (m_field_locations.signal_type) {
    case SignalType::UncompressedSignal: {
        auto signal_column = uncompressed_signal_column();
        auto signal =
                std::static_pointer_cast<arrow::Int16Array>(signal_column->value_slice(row_index));
        return gsl::make_span(signal->raw_values(), signal->raw_values() + signal->length())
                .as_span<std::uint8_t const>();
    }
    case SignalType::VbzSignal: {
        auto signal_column = vbz_signal_column();
        return signal_column->Value(row_index);
    }
    }

    return pod5::Status::Invalid("Unknown signal type");
}

//---------------------------------------------------------------------------------------------------------------------

SignalTableReader::SignalTableReader(std::shared_ptr<void>&& input_source,
                                     std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                                     SignalTableSchemaDescription field_locations,
                                     SchemaMetadataDescription&& schema_metadata,
                                     arrow::MemoryPool* pool)
        : TableReader(std::move(input_source), std::move(reader), std::move(schema_metadata), pool),
          m_field_locations(field_locations),
          m_pool(pool),
          m_table_batches(num_record_batches()),
          m_batch_size(num_record_batches() > 0 ? read_record_batch(0)->num_rows() : 0) {}

SignalTableReader::SignalTableReader(SignalTableReader&& other)
        : TableReader(std::move(other)),
          m_field_locations(std::move(other.m_field_locations)),
          m_pool(other.m_pool),
          m_table_batches(std::move(other.m_table_batches)),
          m_batch_size(other.m_batch_size) {}

SignalTableReader& SignalTableReader::operator=(SignalTableReader&& other) {
    m_field_locations = std::move(other.m_field_locations);
    m_pool = other.m_pool;
    m_batch_size = other.m_batch_size;
    m_table_batches = std::move(other.m_table_batches);
    static_cast<TableReader&>(*this) = std::move(static_cast<TableReader&>(other));
    return *this;
}

Result<SignalTableRecordBatch> SignalTableReader::read_record_batch(std::size_t i) const {
    std::lock_guard<std::mutex> l(m_batch_get_mutex);

    if (m_table_batches[i]) {
        return *m_table_batches[i];
    }

    ARROW_ASSIGN_OR_RAISE(auto record_batch, reader()->ReadRecordBatch(i))
    m_table_batches[i].emplace(std::move(record_batch), m_field_locations, m_pool);
    return *m_table_batches[i];
}

Result<std::size_t> SignalTableReader::signal_batch_for_row_id(std::uint64_t row,
                                                               std::size_t* batch_row) const {
    auto batch = row / m_batch_size;

    if (batch_row) {
        *batch_row = row - (batch * m_batch_size);
    }

    if (batch >= num_record_batches()) {
        return Status::Invalid("Row outside batch bounds");
    }

    return batch;
}

Result<std::size_t> SignalTableReader::extract_sample_count(
        gsl::span<std::uint64_t const> const& row_indices) const {
    std::size_t sample_count = 0;
    for (auto const& signal_row : row_indices) {
        std::size_t batch_row = 0;
        ARROW_ASSIGN_OR_RAISE(auto const signal_batch_index,
                              signal_batch_for_row_id(signal_row, &batch_row));

        ARROW_ASSIGN_OR_RAISE(auto const& signal_batch, read_record_batch(signal_batch_index));
        auto const& samples_column = signal_batch.samples_column();
        sample_count += samples_column->Value(batch_row);
    }
    return sample_count;
}

Status SignalTableReader::extract_samples(gsl::span<std::uint64_t const> const& row_indices,
                                          gsl::span<std::int16_t> const& output_samples) const {
    std::size_t sample_count = 0;

    for (auto const& signal_row : row_indices) {
        std::size_t batch_row = 0;
        ARROW_ASSIGN_OR_RAISE(auto const signal_batch_index,
                              signal_batch_for_row_id(signal_row, &batch_row));

        ARROW_ASSIGN_OR_RAISE(auto const& signal_batch, read_record_batch(signal_batch_index));
        auto const& samples_column = signal_batch.samples_column();
        auto const row_samples_count = samples_column->Value(batch_row);
        std::size_t const sample_start = sample_count;
        sample_count += row_samples_count;
        if (sample_count > output_samples.size()) {
            return Status::Invalid("Too few samples in input samples array");
        }

        ARROW_RETURN_NOT_OK(signal_batch.extract_signal_row(
                batch_row, output_samples.subspan(sample_start, row_samples_count)));
    }
    return Status::OK();
}

Result<std::vector<gsl::span<std::uint8_t const>>> SignalTableReader::extract_samples_inplace(
        gsl::span<std::uint64_t const> const& row_indices,
        std::vector<std::uint32_t>& sample_count) const {
    std::vector<gsl::span<std::uint8_t const>> sample_buffers;

    for (auto const& signal_row : row_indices) {
        std::size_t batch_row = 0;
        ARROW_ASSIGN_OR_RAISE(auto const signal_batch_index,
                              signal_batch_for_row_id(signal_row, &batch_row));

        ARROW_ASSIGN_OR_RAISE(auto const& signal_batch, read_record_batch(signal_batch_index));

        ARROW_ASSIGN_OR_RAISE(auto signal_data, signal_batch.extract_signal_row_inplace(batch_row));
        sample_buffers.emplace_back(std::move(signal_data));

        auto const& samples_column = signal_batch.samples_column();
        sample_count.push_back(samples_column->Value(batch_row));
    }
    return sample_buffers;
}

SignalType SignalTableReader::signal_type() const { return m_field_locations.signal_type; }

//---------------------------------------------------------------------------------------------------------------------
Result<SignalTableReader> make_signal_table_reader(
        std::shared_ptr<arrow::io::RandomAccessFile> const& input,
        arrow::MemoryPool* pool) {
    arrow::ipc::IpcReadOptions options;
    options.memory_pool = pool;

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::ipc::RecordBatchFileReader::Open(input.get(), options));

    auto read_metadata_key_values = reader->schema()->metadata();
    if (!read_metadata_key_values) {
        return Status::IOError("Missing metadata on signal table schema");
    }
    ARROW_ASSIGN_OR_RAISE(auto read_metadata,
                          read_schema_key_value_metadata(read_metadata_key_values));
    ARROW_ASSIGN_OR_RAISE(auto field_locations, read_signal_table_schema(reader->schema()));

    return SignalTableReader({input}, std::move(reader), field_locations, std::move(read_metadata),
                             pool);
}

}  // namespace pod5