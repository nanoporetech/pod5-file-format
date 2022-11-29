#include "pod5_format/migration/migration.h"
#include "pod5_format/migration/migration_utils.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/io_util.h>

#include <iostream>

namespace pod5 {

arrow::Result<std::size_t> get_num_samples(
    std::shared_ptr<arrow::ListArray> const & signal_col,
    std::size_t row_idx,
    std::vector<std::shared_ptr<arrow::RecordBatch>> const & signal_batches)
{
    if (signal_batches.empty()) {
        return 0;
    }

    std::size_t signal_batch_size = signal_batches[0]->num_rows();
    std::size_t num_samples = 0;

    auto values = std::static_pointer_cast<arrow::UInt64Array>(signal_col->values());

    auto offset = signal_col->value_offset(row_idx);
    for (std::int64_t index = 0; index < signal_col->value_length(row_idx); ++index) {
        auto const abs_index = offset + index;
        auto const abs_row = values->Value(abs_index);

        auto const batch_idx = abs_row / signal_batch_size;
        auto const batch_row = abs_row - (batch_idx * signal_batch_size);

        if (batch_idx >= signal_batches.size()) {
            return arrow::Status::Invalid(
                "Invalid signal row ", abs_row, ", cannot find signal batch ", batch_idx);
        }

        auto batch = signal_batches[batch_idx];

        auto samples_column =
            std::static_pointer_cast<arrow::UInt32Array>(batch->GetColumnByName("samples"));
        if (batch_row >= (std::size_t)samples_column->length()) {
            return arrow::Status::Invalid(
                "Invalid signal batch row ", batch_row, ", length is ", samples_column->length());
        }
        num_samples += samples_column->Value(batch_row);
    }

    return num_samples;
}

arrow::Result<MigrationResult> migrate_v1_to_v2(
    MigrationResult && v1_input,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(auto temp_dir, MakeTmpDir("pod5_v1_v2_migration"));
    ARROW_ASSIGN_OR_RAISE(auto v2_reads_table_path, temp_dir->path().Join("reads_table.arrow"));

    {
        ARROW_ASSIGN_OR_RAISE(
            auto v1_reader, open_record_batch_reader(pool, v1_input.footer().reads_table));
        ARROW_ASSIGN_OR_RAISE(
            auto v1_signal_reader, open_record_batch_reader(pool, v1_input.footer().signal_table));
        std::vector<std::shared_ptr<arrow::RecordBatch>> signal_batches(
            v1_signal_reader.reader->num_record_batches());
        for (std::size_t batch_idx = 0;
             batch_idx < (std::size_t)v1_signal_reader.reader->num_record_batches();
             ++batch_idx)
        {
            ARROW_ASSIGN_OR_RAISE(
                signal_batches[batch_idx], v1_signal_reader.reader->ReadRecordBatch(batch_idx));
        }

        auto v2_new_schama = arrow::schema({arrow::field("num_samples", arrow::uint64())});
        ARROW_ASSIGN_OR_RAISE(
            auto new_metadata, update_metadata(v1_reader.metadata, Version(0, 0, 32)));
        ARROW_ASSIGN_OR_RAISE(
            auto v2_schema, arrow::UnifySchemas({v1_reader.schema, v2_new_schama}));
        ARROW_ASSIGN_OR_RAISE(
            auto v2_writer,
            make_record_batch_writer(
                pool, v2_reads_table_path.ToString(), v2_schema, new_metadata));

        for (std::int64_t batch_idx = 0; batch_idx < v1_reader.reader->num_record_batches();
             ++batch_idx) {
            // Read V1 data:
            ARROW_ASSIGN_OR_RAISE(auto v1_batch, v1_reader.reader->ReadRecordBatch(batch_idx));
            auto const num_rows = v1_batch->num_rows();

            // Extend with V2 data:
            std::vector<std::shared_ptr<arrow::Array>> columns = v1_batch->columns();

            auto signal_column =
                std::static_pointer_cast<arrow::ListArray>(v1_batch->GetColumnByName("signal"));
            arrow::UInt64Builder num_samples_builder;
            for (std::int64_t row = 0; row < num_rows; ++row) {
                ARROW_ASSIGN_OR_RAISE(
                    auto num_samples, get_num_samples(signal_column, row, signal_batches));
                ARROW_RETURN_NOT_OK(num_samples_builder.Append(num_samples));
            }
            ARROW_RETURN_NOT_OK(
                set_column(v2_schema, columns, "num_samples", num_samples_builder.Finish()));
            ARROW_RETURN_NOT_OK(v2_writer.write_batch(num_rows, std::move(columns)));
        }

        ARROW_RETURN_NOT_OK(v2_writer.writer->Close());
    }

    // Set up migrated data to point at our new table:
    MigrationResult result = std::move(v1_input);
    ARROW_RETURN_NOT_OK(result.footer().reads_table.from_full_file(v2_reads_table_path.ToString()));
    result.add_temp_dir(std::move(temp_dir));

    return result;
}

}  // namespace pod5
