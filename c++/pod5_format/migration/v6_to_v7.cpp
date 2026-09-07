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

bool file_is_likely_v7(Pod5BatchRecordReader const & reader)
{
    // Check if the input file is already "V7"
    auto const has_field_with_type = [&reader](
                                         char const * field_name, auto const field_type) -> bool {
        auto const channel_column_index = reader.schema->GetFieldIndex(field_name);
        if (channel_column_index < 0) {
            return false;
        }

        auto const field = reader.schema->field(channel_column_index);
        auto const type = field->type();

        return type->Equals(field_type);
    };

    // V7 characterised by a 16-bitchannel field and a 32-bit channel_32bit field.
    return has_field_with_type("channel", arrow::UInt16Type())
           && has_field_with_type("channel_32bit", arrow::UInt32Type());
}

}  // namespace

// Migration should extract the 32-bit "channel", move it to the channel_32bit field then adapt the value
// for the 16-bit "channel"
arrow::Result<MigrationResult> migrate_v6_to_v7(
    MigrationResult && v6_input,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(
        auto v6_reader, open_record_batch_reader(pool, v6_input.footer().reads_table));

    if (file_is_likely_v7(v6_reader)) {
        return std::move(v6_input);
    }

    auto const channel_column_index = v6_reader.schema->GetFieldIndex("channel");

    ARROW_ASSIGN_OR_RAISE(auto temp_dir, MakeTmpDir("pod5_v6_v7_migration"));
    ARROW_ASSIGN_OR_RAISE(auto v7_reads_table_path, temp_dir->path().Join("reads_table.arrow"));

    // Make a writer using the V7 schema. This is a copy of the V6 schema
    // with the 32-bit channel removed, then add
    // 1. 16-bit channel at the location of the old 32-bit channel
    // 2. 32-bit channel_32bit at the end
    ARROW_ASSIGN_OR_RAISE(
        auto new_metadata, update_metadata(v6_reader.metadata, kPod5VersionReadTableV6));
    auto v7_schema = std::make_shared<arrow::Schema>(*v6_reader.schema);
    ARROW_ASSIGN_OR_RAISE(v7_schema, v7_schema->RemoveField(channel_column_index));
    auto const channel_16bit_field = arrow::field("channel", arrow::uint16());
    ARROW_ASSIGN_OR_RAISE(
        v7_schema, v7_schema->AddField(channel_column_index, channel_16bit_field));
    auto const channel_32bit_field = arrow::field("channel_32bit", arrow::uint32());
    auto const channel_32bit_index = 24;
    ARROW_ASSIGN_OR_RAISE(v7_schema, v7_schema->AddField(channel_32bit_index, channel_32bit_field));

    ARROW_ASSIGN_OR_RAISE(
        auto v7_writer,
        make_record_batch_writer(pool, v7_reads_table_path.ToString(), v7_schema, new_metadata));

    for (std::int64_t batch_idx = 0; batch_idx < v6_reader.reader->num_record_batches();
         ++batch_idx)
    {
        ARROW_ASSIGN_OR_RAISE(
            auto v6_batch, ReadRecordBatchAndValidate(*v6_reader.reader, batch_idx));
        ARROW_RETURN_NOT_OK(v6_batch->ValidateFull());
        auto const num_rows = v6_batch->num_rows();

        if (num_rows < 0) {
            return arrow::Status::Invalid("Invalid number of rows");
        } else if (POD5_ENABLE_FUZZERS && num_rows > 1'000'000) {
            return arrow::Status::Invalid("Skipping huge sizes when fuzzing");
        }

        // Get the v6 columns, copy the 32-bit data into the channel_32bit field
        // column and possibly the 16-bit channel field. If the channel is too
        // big for the 16-bit field write zero. Write the batch to the v7
        // version of the file.
        std::vector<std::shared_ptr<arrow::Array>> columns = v6_batch->columns();
        ARROW_RETURN_NOT_OK(check_columns(v6_reader.schema, columns));
        auto const channels_v6 =  // returns null
            std::dynamic_pointer_cast<arrow::UInt32Array>(v6_batch->column(channel_column_index));
        if (!channels_v6) {
            return arrow::Status::Invalid("Unable to find channel field when migrating file");
        }

        arrow::UInt16Builder builder_16bit(pool);
        arrow::UInt32Builder builder_32bit(pool);
        for (std::int64_t row = 0; row < num_rows; ++row) {
            auto const channel = channels_v6->Value(row);
            auto const channel_16bit = channel < 65536 ? channel : 0;
            ARROW_RETURN_NOT_OK(builder_16bit.Append(channel_16bit));
            ARROW_RETURN_NOT_OK(builder_32bit.Append(channel));
        }
        ARROW_ASSIGN_OR_RAISE(auto channels_16bit, builder_16bit.Finish());
        ARROW_ASSIGN_OR_RAISE(auto channels_32bit, builder_32bit.Finish());
        columns[channel_column_index] = channels_16bit;

        ARROW_RETURN_NOT_OK(set_column(v7_schema, columns, "channel_32bit", channels_32bit));

        ARROW_RETURN_NOT_OK(v7_writer.write_batch(num_rows, std::move(columns)));
    }
    ARROW_RETURN_NOT_OK(v7_writer.writer->Close());

    // Set up migrated data to point at the newly written table.
    MigrationResult result = std::move(v6_input);
    ARROW_RETURN_NOT_OK(result.footer().reads_table.from_full_file(v7_reads_table_path.ToString()));
    result.add_temp_dir(std::move(temp_dir));

    return result;
}

}  // namespace pod5
