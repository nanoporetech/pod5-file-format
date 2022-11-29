#include "pod5_format/migration/migration.h"
#include "pod5_format/migration/migration_utils.h"
#include "pod5_format/types.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/io_util.h>

#include <unordered_map>

namespace pod5 {

struct StructRow {
    std::int64_t dict_item_index;
    std::shared_ptr<arrow::StructArray> data;
};

struct StringDictBuilder {
    arrow::Int16Builder indices;
    arrow::StringBuilder items;

    arrow::Result<std::shared_ptr<arrow::Array>> finish()
    {
        ARROW_ASSIGN_OR_RAISE(auto finished_indices, indices.Finish());
        ARROW_ASSIGN_OR_RAISE(auto finished_items, items.Finish());

        auto finished_items_val = std::static_pointer_cast<arrow::StringArray>(finished_items);

        // Re append the finished items to the now blank list
        for (std::int64_t i = 0; i < finished_items_val->length(); ++i) {
            ARROW_RETURN_NOT_OK(items.Append(finished_items_val->GetString(i)));
        }

        return arrow::DictionaryArray::FromArrays(finished_indices, finished_items);
    }

    std::unordered_map<std::string, std::int16_t> lookup;
};

arrow::Result<StructRow> get_dict_struct(
    std::shared_ptr<arrow::RecordBatch> const & batch,
    std::size_t row,
    char const * field_name)
{
    auto column = batch->GetColumnByName(field_name);
    if (!column) {
        return Status::Invalid("Failed to find column ", field_name);
    }

    auto dict_column = std::dynamic_pointer_cast<arrow::DictionaryArray>(column);
    if (!dict_column) {
        return Status::Invalid("Found column ", field_name, " is not a dictionary as expected");
    }

    auto dict_items = std::dynamic_pointer_cast<arrow::StructArray>(dict_column->dictionary());
    if (!dict_items) {
        return Status::Invalid("Dictionary column is not a struct as expected");
    }

    return StructRow{dict_column->GetValueIndex(row), dict_items};
}

template <typename ArrayType, typename Builder>
arrow::Status
append_struct_row(StructRow const & struct_row, char const * field_name, Builder & builder)
{
    auto field_array = struct_row.data->GetFieldByName(field_name);
    if (!field_array) {
        return Status::Invalid("Struct is missing ", field_name, " field");
    }

    auto typed_field_array = std::dynamic_pointer_cast<ArrayType>(field_array);
    if (!typed_field_array) {
        return Status::Invalid(field_name, " field is the wrong type");
    }

    if (struct_row.dict_item_index >= field_array->length()) {
        return Status::Invalid("Dictionary index is out of range");
    }
    return builder.Append(typed_field_array->Value(struct_row.dict_item_index));
}

arrow::Status append_struct_row_to_dict(
    StructRow const & struct_row,
    char const * field_name,
    StringDictBuilder & builder)
{
    auto field_array = struct_row.data->GetFieldByName(field_name);
    if (!field_array) {
        return Status::Invalid("Struct is missing ", field_name, " field");
    }

    auto typed_field_array = std::dynamic_pointer_cast<arrow::StringArray>(field_array);
    if (!typed_field_array) {
        return Status::Invalid(field_name, " field is the wrong type");
    }

    if (struct_row.dict_item_index >= field_array->length()) {
        return Status::Invalid("Dictionary index is out of range");
    }

    auto str_value = typed_field_array->Value(struct_row.dict_item_index).to_string();
    auto it = builder.lookup.find(str_value);
    if (it != builder.lookup.end()) {
        return builder.indices.Append(it->second);
    }

    auto index = builder.items.length();
    ARROW_RETURN_NOT_OK(builder.items.Append(str_value));
    builder.lookup[str_value] = index;
    return builder.indices.Append(index);
}

arrow::Result<MigrationResult> migrate_v2_to_v3(
    MigrationResult && v2_input,
    arrow::MemoryPool * pool)
{
    ARROW_ASSIGN_OR_RAISE(auto temp_dir, MakeTmpDir("pod5_v2_v3_migration"));
    ARROW_ASSIGN_OR_RAISE(auto v3_reads_table_path, temp_dir->path().Join("reads_table.arrow"));
    ARROW_ASSIGN_OR_RAISE(
        auto v3_run_info_table_path, temp_dir->path().Join("run_info_table.arrow"));

    {
        ARROW_ASSIGN_OR_RAISE(
            auto v2_reader, open_record_batch_reader(pool, v2_input.footer().reads_table));
        ARROW_ASSIGN_OR_RAISE(
            auto new_metadata, update_metadata(v2_reader.metadata, Version(0, 0, 35)));

        {
            auto v3_reads_schema = arrow::schema(
                {arrow::field("read_id", uuid()),
                 arrow::field("signal", arrow::list(arrow::uint64())),
                 arrow::field("read_number", arrow::uint32()),
                 arrow::field("start", arrow::uint64()),
                 arrow::field("median_before", arrow::float32()),
                 arrow::field("num_minknow_events", arrow::uint64()),
                 arrow::field("tracked_scaling_scale", arrow::float32()),
                 arrow::field("tracked_scaling_shift", arrow::float32()),
                 arrow::field("predicted_scaling_scale", arrow::float32()),
                 arrow::field("predicted_scaling_shift", arrow::float32()),
                 arrow::field("num_reads_since_mux_change", arrow::uint32()),
                 arrow::field("time_since_mux_change", arrow::float32()),
                 arrow::field("num_samples", arrow::uint64()),
                 arrow::field("channel", arrow::uint16()),
                 arrow::field("well", arrow::uint8()),
                 arrow::field("pore_type", arrow::dictionary(arrow::int16(), arrow::utf8())),
                 arrow::field("calibration_offset", arrow::float32()),
                 arrow::field("calibration_scale", arrow::float32()),
                 arrow::field("end_reason", arrow::dictionary(arrow::int16(), arrow::utf8())),
                 arrow::field("end_reason_forced", arrow::boolean()),
                 arrow::field("run_info", arrow::dictionary(arrow::int16(), arrow::utf8()))},
                new_metadata);
            ARROW_ASSIGN_OR_RAISE(
                auto v3_reads_writer,
                make_record_batch_writer(
                    pool, v3_reads_table_path.ToString(), v3_reads_schema, new_metadata));

            std::vector<std::string> const columns_to_copy{
                "read_id",
                "signal",
                "read_number",
                "start",
                "median_before",
                "num_minknow_events",
                "tracked_scaling_scale",
                "tracked_scaling_shift",
                "predicted_scaling_scale",
                "predicted_scaling_shift",
                "num_reads_since_mux_change",
                "time_since_mux_change",
                "num_samples"};

            // Builders for dict columns
            StringDictBuilder pore_type;
            StringDictBuilder end_reason;
            StringDictBuilder run_info;
            for (std::int64_t batch_idx = 0; batch_idx < v2_reader.reader->num_record_batches();
                 ++batch_idx) {
                // Read V2 data:
                ARROW_ASSIGN_OR_RAISE(auto v2_batch, v2_reader.reader->ReadRecordBatch(batch_idx));
                auto const num_rows = v2_batch->num_rows();

                std::vector<std::shared_ptr<arrow::Array>> v3_columns;

                // Write V3 data:
                std::vector<std::shared_ptr<arrow::Array>> v2_columns = v2_batch->columns();
                for (auto const & col_name : columns_to_copy) {
                    ARROW_RETURN_NOT_OK(copy_column(
                        v2_reader.schema,
                        v2_columns,
                        col_name.data(),
                        v3_reads_schema,
                        v3_columns));
                }

                arrow::UInt16Builder channel;
                arrow::UInt8Builder well;
                arrow::FloatBuilder calibration_offset;
                arrow::FloatBuilder calibration_scale;
                arrow::BooleanBuilder end_reason_forced;
                for (std::int64_t row = 0; row < num_rows; ++row) {
                    ARROW_ASSIGN_OR_RAISE(
                        auto calibration_data, get_dict_struct(v2_batch, row, "calibration"));
                    ARROW_RETURN_NOT_OK(append_struct_row<arrow::FloatArray>(
                        calibration_data, "offset", calibration_offset));
                    ARROW_RETURN_NOT_OK(append_struct_row<arrow::FloatArray>(
                        calibration_data, "scale", calibration_scale));

                    ARROW_ASSIGN_OR_RAISE(auto pore_data, get_dict_struct(v2_batch, row, "pore"));
                    ARROW_RETURN_NOT_OK(
                        append_struct_row<arrow::UInt16Array>(pore_data, "channel", channel));
                    ARROW_RETURN_NOT_OK(
                        append_struct_row<arrow::UInt8Array>(pore_data, "well", well));
                    ARROW_RETURN_NOT_OK(
                        append_struct_row_to_dict(pore_data, "pore_type", pore_type));

                    ARROW_ASSIGN_OR_RAISE(
                        auto end_reason_data, get_dict_struct(v2_batch, row, "end_reason"));
                    ARROW_RETURN_NOT_OK(
                        append_struct_row_to_dict(end_reason_data, "name", end_reason));
                    ARROW_RETURN_NOT_OK(append_struct_row<arrow::BooleanArray>(
                        end_reason_data, "forced", end_reason_forced));

                    ARROW_ASSIGN_OR_RAISE(
                        auto run_info_data, get_dict_struct(v2_batch, row, "run_info"));
                    ARROW_RETURN_NOT_OK(
                        append_struct_row_to_dict(run_info_data, "acquisition_id", run_info));
                }
                ARROW_RETURN_NOT_OK(set_column(
                    v3_reads_schema,
                    v3_columns,
                    "calibration_offset",
                    calibration_offset.Finish()));
                ARROW_RETURN_NOT_OK(set_column(
                    v3_reads_schema, v3_columns, "calibration_scale", calibration_scale.Finish()));
                ARROW_RETURN_NOT_OK(
                    set_column(v3_reads_schema, v3_columns, "channel", channel.Finish()));
                ARROW_RETURN_NOT_OK(set_column(v3_reads_schema, v3_columns, "well", well.Finish()));
                ARROW_RETURN_NOT_OK(
                    set_column(v3_reads_schema, v3_columns, "pore_type", pore_type.finish()));
                ARROW_RETURN_NOT_OK(
                    set_column(v3_reads_schema, v3_columns, "end_reason", end_reason.finish()));
                ARROW_RETURN_NOT_OK(set_column(
                    v3_reads_schema, v3_columns, "end_reason_forced", end_reason_forced.Finish()));
                ARROW_RETURN_NOT_OK(
                    set_column(v3_reads_schema, v3_columns, "run_info", run_info.finish()));

                ARROW_RETURN_NOT_OK(v3_reads_writer.write_batch(num_rows, std::move(v3_columns)));
            }
            ARROW_RETURN_NOT_OK(v3_reads_writer.writer->Close());
        }
        {
            ARROW_ASSIGN_OR_RAISE(
                auto v2_last_batch,
                v2_reader.reader->ReadRecordBatch(v2_reader.reader->num_record_batches() - 1));
            auto run_info_column = std::dynamic_pointer_cast<arrow::DictionaryArray>(
                v2_last_batch->GetColumnByName("run_info"));
            if (!run_info_column) {
                return arrow::Status::Invalid("Failed to find the run info column");
            }
            auto run_info_dict_type =
                std::dynamic_pointer_cast<arrow::DictionaryType>(run_info_column->type());
            if (!run_info_dict_type) {
                return arrow::Status::Invalid("Failed to find a run info of the right type");
            }
            auto run_info_items =
                std::dynamic_pointer_cast<arrow::StructArray>(run_info_column->dictionary());
            if (!run_info_items) {
                return arrow::Status::Invalid("Failed to find a run info items array");
            }
            auto run_info_items_type =
                std::dynamic_pointer_cast<arrow::StructType>(run_info_items->type());
            if (!run_info_items_type) {
                return arrow::Status::Invalid(
                    "Failed to find a run info items array of the right type");
            }

            // Append all the run info dict-struct data to the new table:
            auto v3_run_info_schema = arrow::schema(run_info_items_type->fields(), new_metadata);
            ARROW_ASSIGN_OR_RAISE(
                auto v3_run_info_writer,
                make_record_batch_writer(
                    pool, v3_run_info_table_path.ToString(), v3_run_info_schema, new_metadata));

            auto const & fields = run_info_items->fields();
            std::vector<std::shared_ptr<arrow::Array>> v3_columns(
                v3_run_info_schema->fields().size());
            for (std::size_t col = 0; col < v3_columns.size(); ++col) {
                v3_columns[col] = fields[col];
            }

            ARROW_RETURN_NOT_OK(
                v3_run_info_writer.write_batch(run_info_items->length(), std::move(v3_columns)));
            ARROW_RETURN_NOT_OK(v3_run_info_writer.writer->Close());
        }
    }

    // Set up migrated data to point at our new table:
    MigrationResult result = std::move(v2_input);
    ARROW_RETURN_NOT_OK(result.footer().reads_table.from_full_file(v3_reads_table_path.ToString()));
    ARROW_RETURN_NOT_OK(
        result.footer().run_info_table.from_full_file(v3_run_info_table_path.ToString()));
    result.add_temp_dir(std::move(temp_dir));

    return result;
}

}  // namespace pod5
