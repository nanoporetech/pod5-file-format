#pragma once

#include <arrow/array.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/key_value_metadata.h>

namespace pod5 {

template <typename T, typename U>
arrow::Result<std::shared_ptr<arrow::Array>>
make_filled_array(arrow::MemoryPool * pool, std::size_t row_count, U default_value)
{
    T builder(pool);
    for (std::size_t i = 0; i < row_count; ++i) {
        ARROW_RETURN_NOT_OK(builder.Append(default_value));
    }

    return builder.Finish();
}

inline arrow::Status set_column(
    std::shared_ptr<arrow::Schema> const & schema,
    std::vector<std::shared_ptr<arrow::Array>> & columns,
    char const * field_name,
    arrow::Result<std::shared_ptr<arrow::Array>> const & array)
{
    auto field_index = schema->GetFieldIndex(field_name);
    if (field_index == -1) {
        return arrow::Status::Invalid("Failed to find field '", field_name, "' during migration.");
    }

    if (field_index >= (std::int64_t)columns.size()) {
        columns.resize(field_index + 1);
    }

    ARROW_ASSIGN_OR_RAISE(columns[field_index], array);

    return arrow::Status::OK();
}

inline arrow::Status copy_column(
    std::shared_ptr<arrow::Schema> const & schema_a,
    std::vector<std::shared_ptr<arrow::Array>> & columns_a,
    char const * field_name,
    std::shared_ptr<arrow::Schema> const & schema_b,
    std::vector<std::shared_ptr<arrow::Array>> & columns_b)
{
    auto field_index_a = schema_a->GetFieldIndex(field_name);
    if (field_index_a == -1 || field_index_a >= (std::int64_t)columns_a.size()) {
        return arrow::Status::Invalid("Failed to find field '", field_name, "' during migration.");
    }

    auto source_column = columns_a[field_index_a];

    auto field_index_b = schema_b->GetFieldIndex(field_name);
    if (field_index_b >= (std::int64_t)columns_b.size()) {
        columns_b.resize(field_index_b + 1);
    }

    columns_b[field_index_b] = source_column;

    return arrow::Status::OK();
}

struct Pod5BatchRecordReader {
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader;
    std::shared_ptr<arrow::Schema> schema;
    std::shared_ptr<arrow::KeyValueMetadata const> metadata;
};

struct Pod5BatchRecordWriter {
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    std::shared_ptr<arrow::Schema> schema;

    arrow::Status write_batch(
        std::size_t num_rows,
        std::vector<std::shared_ptr<arrow::Array>> const & columns)
    {
        auto const record_batch = arrow::RecordBatch::Make(schema, num_rows, std::move(columns));
        return writer->WriteRecordBatch(*record_batch);
    }
};

inline pod5::Result<Pod5BatchRecordReader> open_record_batch_reader(
    arrow::MemoryPool * pool,
    combined_file_utils::ParsedFileInfo file_info)
{
    Pod5BatchRecordReader result;
    ARROW_ASSIGN_OR_RAISE(auto file, open_sub_file(file_info));

    arrow::ipc::IpcReadOptions read_options;
    read_options.memory_pool = pool;
    ARROW_ASSIGN_OR_RAISE(
        result.reader, arrow::ipc::RecordBatchFileReader::Open(file, read_options));

    result.schema = result.reader->schema();
    result.metadata = result.schema->metadata();
    if (!result.metadata) {
        return Status::IOError("Missing metadata on read table schema");
    }

    return result;
}

inline pod5::Result<std::shared_ptr<arrow::KeyValueMetadata const>> update_metadata(
    std::shared_ptr<arrow::KeyValueMetadata const> original_metadata,
    Version version_to_write)
{
    auto result = original_metadata->Copy();
    // Update the reader for the new version:
    ARROW_RETURN_NOT_OK(result->Set("MINKNOW:pod5_version", version_to_write.to_string()));
    return result;
}

inline pod5::Result<Pod5BatchRecordWriter> make_record_batch_writer(
    arrow::MemoryPool * pool,
    std::string path,
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<arrow::KeyValueMetadata const> metadata)
{
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::FileOutputStream::Open(path, false));
    arrow::ipc::IpcWriteOptions write_options;
    write_options.memory_pool = pool;
    write_options.emit_dictionary_deltas = true;

    Pod5BatchRecordWriter result;
    ARROW_ASSIGN_OR_RAISE(
        result.writer, arrow::ipc::MakeFileWriter(file, schema, write_options, metadata));
    result.schema = schema;

    return result;
}

inline pod5::Status check_columns(
    std::shared_ptr<arrow::Schema> const & schema,
    std::vector<std::shared_ptr<arrow::Array>> const & columns)
{
    for (std::size_t i = 0; i < columns.size(); ++i) {
        auto const & column = columns[i];
        auto const & schema_field = schema->field(i);

        if (auto list = std::dynamic_pointer_cast<arrow::ListArray>(column)) {
            auto last_value = list->value_offset(0);
            for (int i = 1; i <= list->length(); ++i) {
                if (list->value_offset(i) < last_value) {
                    return arrow::Status::Invalid(
                        "Field content for field `",
                        schema_field->name(),
                        "`, list offsets are invalid"
                        " at row index ",
                        i,
                        " (",
                        list->value_offset(i),
                        " < ",
                        last_value,
                        ")");
                }
                last_value = list->value_offset(i);
            }
        } else if (auto dict = std::dynamic_pointer_cast<arrow::DictionaryArray>(column)) {
            auto dict_values = dict->dictionary();
            auto string_dictionary_values =
                std::dynamic_pointer_cast<arrow::StringArray>(dict_values);
            if (string_dictionary_values) {
                auto const value_offsets = string_dictionary_values->value_offsets();
                std::int64_t const value_offsets_length =
                    value_offsets->size() / sizeof(arrow::StringArray::offset_type);
                if (value_offsets_length != (1 + dict_values->length()))
                {  // We expect N+1 offsets for the final element length
                    return arrow::Status::Invalid(
                        "Dictionary length for field `",
                        schema_field->name(),
                        "`, dictionary length is ",
                        dict_values->length(),
                        " but value offsets is length ",
                        value_offsets_length);
                }
            }

            auto indices = std::dynamic_pointer_cast<arrow::Int16Array>(dict->indices());
            if (!indices) {
                return arrow::Status::Invalid(
                    "Field content for field `",
                    schema_field->name(),
                    "`, dictionary indexes are missing");
            }
            for (int i = 0; i < indices->length(); ++i) {
                if (indices->Value(i) >= dict_values->length()) {
                    return arrow::Status::Invalid(
                        "Field content for field `",
                        schema_field->name(),
                        "`, dictionary indexes are invalid"
                        " at row index ",
                        i,
                        " (",
                        indices->Value(i),
                        " >= ",
                        dict_values->length(),
                        ")");
                }
            }
        }
    }

    return {};
}

}  // namespace pod5
