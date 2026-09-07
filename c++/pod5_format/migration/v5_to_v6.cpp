#include "pod5_format/migration/migration.h"
#include "pod5_format/migration/migration_utils.h"
#include "pod5_format/read_table_schema.h"
#include "pod5_format/table_reader.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/status.h>
#include <arrow/util/io_util.h>

#include <limits>

namespace pod5 {

namespace {

bool likely_v6(Pod5BatchRecordReader const & reader)
{
    // Check if the input file is already "V6"
    auto const channel_column_index = reader.schema->GetFieldIndex("channel");
    if (channel_column_index < 0) {
        return false;
    }

    auto const field = reader.schema->field(channel_column_index);
    auto const type = field->type();
    return type->Equals(arrow::UInt32Type());
}

}  // namespace

arrow::Result<MigrationResult> migrate_v5_to_v6(
    MigrationResult && v5_input,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(
        auto v5_reader, open_record_batch_reader(pool, v5_input.footer().reads_table));

    if (likely_v6(v5_reader)) {
        return std::move(v5_input);
    }

    auto const channel_column_index = v5_reader.schema->GetFieldIndex("channel");

    ARROW_ASSIGN_OR_RAISE(auto temp_dir, MakeTmpDir("pod5_v5_v6_migration"));
    ARROW_ASSIGN_OR_RAISE(auto v6_reads_table_path, temp_dir->path().Join("reads_table.arrow"));

    // Make a writer using a V6 schema. Copy the V5 schema, remove 16-bit
    // channel field and add a new 32-bit field to make the V6 schema.
    ARROW_ASSIGN_OR_RAISE(
        auto new_metadata, update_metadata(v5_reader.metadata, kPod5VersionReadTableV6));
    auto v6_schema = std::make_shared<arrow::Schema>(*v5_reader.schema);
    ARROW_ASSIGN_OR_RAISE(v6_schema, v6_schema->RemoveField(channel_column_index));
    auto const channel_32bit_field = arrow::field("channel", arrow::uint32());
    ARROW_ASSIGN_OR_RAISE(
        v6_schema, v6_schema->AddField(channel_column_index, channel_32bit_field));

    ARROW_ASSIGN_OR_RAISE(
        auto v6_writer,
        make_record_batch_writer(pool, v6_reads_table_path.ToString(), v6_schema, new_metadata));

    for (std::int64_t batch_idx = 0; batch_idx < v5_reader.reader->num_record_batches();
         ++batch_idx)
    {
        ARROW_ASSIGN_OR_RAISE(
            auto v5_batch, ReadRecordBatchAndValidate(*v5_reader.reader, batch_idx));
        ARROW_RETURN_NOT_OK(v5_batch->ValidateFull());
        auto const num_rows = v5_batch->num_rows();

        if (num_rows < 0) {
            return arrow::Status::Invalid("Invalid number of rows");
        } else if (POD5_ENABLE_FUZZERS && num_rows > 1'000'000) {
            return arrow::Status::Invalid("Skipping huge sizes when fuzzing");
        }

        // Get the v5 columns, make a 32-bit copy of the 16-bit "channel"
        // column, swap-out the 16-bit version for the 32-bit, and then
        // write the batch to the v6 version of the file.
        std::vector<std::shared_ptr<arrow::Array>> columns = v5_batch->columns();
        ARROW_RETURN_NOT_OK(check_columns(v5_reader.schema, columns));
        auto const channels_16bit =
            std::dynamic_pointer_cast<arrow::UInt16Array>(v5_batch->GetColumnByName("channel"));

        arrow::UInt32Builder builder(pool);
        for (std::int64_t row = 0; row < num_rows; ++row) {
            auto const channel = channels_16bit->Value(row);
            ARROW_RETURN_NOT_OK(builder.Append(channel));
        }
        ARROW_ASSIGN_OR_RAISE(auto channels_32bit, builder.Finish());
        columns[channel_column_index] = channels_32bit;

        ARROW_RETURN_NOT_OK(v6_writer.write_batch(num_rows, std::move(columns)));
    }
    ARROW_RETURN_NOT_OK(v6_writer.writer->Close());

    // Set up migrated data to point at the newly written table.
    MigrationResult result = std::move(v5_input);
    ARROW_RETURN_NOT_OK(result.footer().reads_table.from_full_file(v6_reads_table_path.ToString()));
    result.add_temp_dir(std::move(temp_dir));

    return result;
}

}  // namespace pod5
