#include "pod5_format/migration/migration.h"
#include "pod5_format/migration/migration_utils.h"
#include "pod5_format/table_reader.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/status.h>
#include <arrow/util/io_util.h>

#include <unordered_map>

namespace pod5 {

arrow::Result<MigrationResult> migrate_v3_to_v4(
    MigrationResult && v3_input,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(auto temp_dir, MakeTmpDir("pod5_v3_v4_migration"));
    ARROW_ASSIGN_OR_RAISE(auto v4_reads_table_path, temp_dir->path().Join("reads_table.arrow"));

    {
        ARROW_ASSIGN_OR_RAISE(
            auto v3_reader, open_record_batch_reader(pool, v3_input.footer().reads_table));

        auto v4_new_schama = arrow::schema({arrow::field("open_pore_level", arrow::float32())});

        ARROW_ASSIGN_OR_RAISE(
            auto v4_schema, arrow::UnifySchemas({v3_reader.schema, v4_new_schama}));

        ARROW_ASSIGN_OR_RAISE(
            auto new_metadata, update_metadata(v3_reader.metadata, Version(0, 3, 30)));
        ARROW_ASSIGN_OR_RAISE(
            auto v4_writer,
            make_record_batch_writer(
                pool, v4_reads_table_path.ToString(), v4_schema, new_metadata));

        for (std::int64_t batch_idx = 0; batch_idx < v3_reader.reader->num_record_batches();
             ++batch_idx)
        {
            // Read V0 data:
            ARROW_ASSIGN_OR_RAISE(
                auto v3_batch, ReadRecordBatchAndValidate(*v3_reader.reader, batch_idx));
            ARROW_RETURN_NOT_OK(v3_batch->ValidateFull());
            auto const num_rows = v3_batch->num_rows();

            if (num_rows < 0) {
                return arrow::Status::Invalid("Invalid number of rows");
            } else if (POD5_ENABLE_FUZZERS && num_rows > 1'000'000) {
                return arrow::Status::Invalid("Skipping huge sizes when fuzzing");
            }

            // Extend with V4 data:
            std::vector<std::shared_ptr<arrow::Array>> columns = v3_batch->columns();
            ARROW_RETURN_NOT_OK(check_columns(v3_reader.schema, columns));
            ARROW_RETURN_NOT_OK(set_column(
                v4_schema,
                columns,
                "open_pore_level",
                make_filled_array<arrow::FloatBuilder>(
                    pool, num_rows, std::numeric_limits<float>::quiet_NaN())));
            ARROW_RETURN_NOT_OK(v4_writer.write_batch(num_rows, std::move(columns)));
        }

        ARROW_RETURN_NOT_OK(v4_writer.writer->Close());
    }

    // Set up migrated data to point at our new table:
    MigrationResult result = std::move(v3_input);
    ARROW_RETURN_NOT_OK(result.footer().reads_table.from_full_file(v4_reads_table_path.ToString()));
    result.add_temp_dir(std::move(temp_dir));

    return result;
}

}  // namespace pod5
