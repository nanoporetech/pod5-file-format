#include "mkr_format/signal_table_reader.h"

#include "mkr_format/schema_metadata.h"
#include "mkr_format/signal_compression.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/ipc/reader.h>

namespace mkr {

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

    return mkr::Status::Invalid("Unknown signal type");
}

Status SignalTableRecordBatch::extract_signal_row(std::size_t row_index,
                                                  gsl::span<std::int16_t> samples) const {
    if (row_index >= num_rows()) {
        return mkr::Status::Invalid("Queried signal row ", row_index,
                                    " is outside the available rows (", num_rows(), "in batch)");
    }

    auto sample_count = samples_column();
    auto samples_in_row = sample_count->Value(row_index);
    if (samples_in_row != samples.size()) {
        return mkr::Status::Invalid("Unexpected size for sample array ", samples.size(),
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
        return mkr::decompress_signal(signal_compressed, m_pool, samples);
    }
    }

    return mkr::Status::Invalid("Unknown signal type");
}

//---------------------------------------------------------------------------------------------------------------------

SignalTableReader::SignalTableReader(std::shared_ptr<void>&& input_source,
                                     std::shared_ptr<arrow::ipc::RecordBatchFileReader>&& reader,
                                     SignalTableSchemaDescription field_locations,
                                     SchemaMetadataDescription&& schema_metadata,
                                     arrow::MemoryPool* pool)
        : TableReader(std::move(input_source), std::move(reader), std::move(schema_metadata), pool),
          m_field_locations(field_locations),
          m_pool(pool) {}

Result<SignalTableRecordBatch> SignalTableReader::read_record_batch(std::size_t i) const {
    auto record_batch = reader()->ReadRecordBatch(i);
    if (!record_batch.ok()) {
        return record_batch.status();
    }
    return SignalTableRecordBatch{std::move(*record_batch), m_field_locations, m_pool};
}

Result<std::size_t> SignalTableReader::signal_batch_for_row_id(std::size_t row,
                                                               std::size_t* batch_start_row) const {
    if (m_cumulative_batch_sizes.empty()) {
        auto const batch_count = num_record_batches();
        m_cumulative_batch_sizes.resize(batch_count);

        std::size_t cumulative_row_count = 0;
        for (std::size_t i = 0; i < batch_count; ++i) {
            ARROW_ASSIGN_OR_RAISE(auto batch, read_record_batch(i));
            cumulative_row_count += batch.num_rows();

            // Assign each element the cumulative size of the batches.
            m_cumulative_batch_sizes[i] = cumulative_row_count;
        }
    }

    // Find cumulative row count >= to [row]
    auto it =
            std::lower_bound(m_cumulative_batch_sizes.begin(), m_cumulative_batch_sizes.end(), row);
    if (it == m_cumulative_batch_sizes.end()) {
        return mkr::Status::Invalid("Unable to find row ", row, " in batches (max row ",
                                    m_cumulative_batch_sizes.back(), ")");
    }

    if (batch_start_row) {
        *batch_start_row = 0;
        // If we aren't in the first batch, the abs batch start is the entry before the one we found.
        if (it != m_cumulative_batch_sizes.begin()) {
            *batch_start_row = *(it - 1);
        }
    }
    return std::distance(m_cumulative_batch_sizes.begin(), it);
}
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

}  // namespace mkr