#include "pod5_format/migration/migration.h"
#include "pod5_format/migration/migration_utils.h"
#include "pod5_format/table_reader.h"
#include "pod5_format/version_constants.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/status.h>
#include <arrow/util/io_util.h>

#include <limits>

namespace pod5 {

arrow::Result<MigrationResult> migrate_v4_to_v5(
    MigrationResult && v4_input,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(
        auto v4_reader, open_record_batch_reader(pool, v4_input.footer().reads_table));

    auto const has_expected_open_pore_level =
        v4_reader.schema->GetFieldIndex("expected_open_pore_level") != -1;
    auto const has_selected_read_level =
        v4_reader.schema->GetFieldIndex("selected_read_level") != -1;
    if (has_expected_open_pore_level != has_selected_read_level) {
        return arrow::Status::Invalid(
            "Invalid reads table schema: expected_open_pore_level and selected_read_level must "
            "either both be present or both be absent");
    }
    if (has_expected_open_pore_level && has_selected_read_level) {
        return std::move(v4_input);
    }

    ARROW_ASSIGN_OR_RAISE(auto temp_dir, MakeTmpDir("pod5_v4_v5_migration"));
    ARROW_ASSIGN_OR_RAISE(auto v5_reads_table_path, temp_dir->path().Join("reads_table.arrow"));

    {
        auto v5_new_schema = arrow::schema(
            {arrow::field("expected_open_pore_level", arrow::float32()),
             arrow::field("selected_read_level", arrow::float32())});

        ARROW_ASSIGN_OR_RAISE(
            auto v5_schema, arrow::UnifySchemas({v4_reader.schema, v5_new_schema}));

        ARROW_ASSIGN_OR_RAISE(
            auto new_metadata, update_metadata(v4_reader.metadata, kPod5VersionReadTableV5));
        ARROW_ASSIGN_OR_RAISE(
            auto v5_writer,
            make_record_batch_writer(
                pool, v5_reads_table_path.ToString(), v5_schema, new_metadata));

        for (std::int64_t batch_idx = 0; batch_idx < v4_reader.reader->num_record_batches();
             ++batch_idx)
        {
            ARROW_ASSIGN_OR_RAISE(
                auto v4_batch, ReadRecordBatchAndValidate(*v4_reader.reader, batch_idx));
            ARROW_RETURN_NOT_OK(v4_batch->ValidateFull());
            auto const num_rows = v4_batch->num_rows();

            if (num_rows < 0) {
                return arrow::Status::Invalid("Invalid number of rows");
            } else if (POD5_ENABLE_FUZZERS && num_rows > 1'000'000) {
                return arrow::Status::Invalid("Skipping huge sizes when fuzzing");
            }

            // Extend with V5 data:
            std::vector<std::shared_ptr<arrow::Array>> columns = v4_batch->columns();
            ARROW_RETURN_NOT_OK(check_columns(v4_reader.schema, columns));
            if (!has_expected_open_pore_level) {
                ARROW_RETURN_NOT_OK(set_column(
                    v5_schema,
                    columns,
                    "expected_open_pore_level",
                    make_filled_array<arrow::FloatBuilder>(
                        pool, num_rows, std::numeric_limits<float>::quiet_NaN())));
            }
            if (!has_selected_read_level) {
                ARROW_RETURN_NOT_OK(set_column(
                    v5_schema,
                    columns,
                    "selected_read_level",
                    make_filled_array<arrow::FloatBuilder>(
                        pool, num_rows, std::numeric_limits<float>::quiet_NaN())));
            }
            ARROW_RETURN_NOT_OK(v5_writer.write_batch(num_rows, std::move(columns)));
        }

        ARROW_RETURN_NOT_OK(v5_writer.writer->Close());
    }

    // Set up migrated data to point at our new table:
    MigrationResult result = std::move(v4_input);
    ARROW_RETURN_NOT_OK(result.footer().reads_table.from_full_file(v5_reads_table_path.ToString()));
    result.add_temp_dir(std::move(temp_dir));

    return result;
}

}  // namespace pod5
