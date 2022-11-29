#include "pod5_format/migration/migration.h"
#include "pod5_format/migration/migration_utils.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/util/io_util.h>

#include <iostream>

namespace pod5 {

arrow::Result<MigrationResult> migrate_v0_to_v1(
    MigrationResult && v0_input,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(auto temp_dir, MakeTmpDir("pod5_v0_v1_migration"));
    ARROW_ASSIGN_OR_RAISE(auto v1_reads_table_path, temp_dir->path().Join("reads_table.arrow"));

    {
        ARROW_ASSIGN_OR_RAISE(
            auto v0_reader, open_record_batch_reader(pool, v0_input.footer().reads_table));

        auto v1_new_schama = arrow::schema(
            {arrow::field("num_minknow_events", arrow::uint64()),
             arrow::field("tracked_scaling_scale", arrow::float32()),
             arrow::field("tracked_scaling_shift", arrow::float32()),
             arrow::field("predicted_scaling_scale", arrow::float32()),
             arrow::field("predicted_scaling_shift", arrow::float32()),
             arrow::field("num_reads_since_mux_change", arrow::uint32()),
             arrow::field("time_since_mux_change", arrow::float32())});

        ARROW_ASSIGN_OR_RAISE(
            auto v1_schema, arrow::UnifySchemas({v0_reader.schema, v1_new_schama}));

        ARROW_ASSIGN_OR_RAISE(
            auto new_metadata, update_metadata(v0_reader.metadata, Version(0, 0, 24)));
        ARROW_ASSIGN_OR_RAISE(
            auto v1_writer,
            make_record_batch_writer(
                pool, v1_reads_table_path.ToString(), v1_schema, new_metadata));

        for (std::int64_t batch_idx = 0; batch_idx < v0_reader.reader->num_record_batches();
             ++batch_idx) {
            // Read V0 data:
            ARROW_ASSIGN_OR_RAISE(auto v0_batch, v0_reader.reader->ReadRecordBatch(batch_idx));
            auto const num_rows = v0_batch->num_rows();

            // Extend with V1 data:
            std::vector<std::shared_ptr<arrow::Array>> columns = v0_batch->columns();
            ARROW_RETURN_NOT_OK(set_column(
                v1_schema,
                columns,
                "num_minknow_events",
                make_filled_array<arrow::UInt64Builder>(pool, num_rows, 0)));
            ARROW_RETURN_NOT_OK(set_column(
                v1_schema,
                columns,
                "tracked_scaling_scale",
                make_filled_array<arrow::FloatBuilder>(
                    pool, num_rows, std::numeric_limits<float>::quiet_NaN())));
            ARROW_RETURN_NOT_OK(set_column(
                v1_schema,
                columns,
                "tracked_scaling_shift",
                make_filled_array<arrow::FloatBuilder>(
                    pool, num_rows, std::numeric_limits<float>::quiet_NaN())));
            ARROW_RETURN_NOT_OK(set_column(
                v1_schema,
                columns,
                "predicted_scaling_scale",
                make_filled_array<arrow::FloatBuilder>(
                    pool, num_rows, std::numeric_limits<float>::quiet_NaN())));
            ARROW_RETURN_NOT_OK(set_column(
                v1_schema,
                columns,
                "predicted_scaling_shift",
                make_filled_array<arrow::FloatBuilder>(
                    pool, num_rows, std::numeric_limits<float>::quiet_NaN())));
            ARROW_RETURN_NOT_OK(set_column(
                v1_schema,
                columns,
                "num_reads_since_mux_change",
                make_filled_array<arrow::UInt32Builder>(pool, num_rows, 0)));
            ARROW_RETURN_NOT_OK(set_column(
                v1_schema,
                columns,
                "time_since_mux_change",
                make_filled_array<arrow::FloatBuilder>(pool, num_rows, 0.0f)));
            ARROW_RETURN_NOT_OK(v1_writer.write_batch(num_rows, std::move(columns)));
        }

        ARROW_RETURN_NOT_OK(v1_writer.writer->Close());
    }

    // Set up migrated data to point at our new table:
    MigrationResult result = std::move(v0_input);
    ARROW_RETURN_NOT_OK(result.footer().reads_table.from_full_file(v1_reads_table_path.ToString()));
    result.add_temp_dir(std::move(temp_dir));

    return result;
}

}  // namespace pod5
